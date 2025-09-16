import time
import os
import sqlite3
import hashlib
import threading
import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from rich.console import Console
from datetime import datetime

# console output helper
console = Console()

# your Jazler network path (folder, not file)
NETWORK_PATH = r"\\Ifodo\c\Jazler SOHO\Logs\Main"

# SQLite database file
DB_FILE = "jazler_logs.db"

# remote ingest endpoint
REMOTE_URL = "https://rbs.elektranbroadcast.com/ingest"
REMOTE_BULK_URL = REMOTE_URL + "/bulk"

# ensure database & tables exist
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT,
            play_time TEXT,
            event_type TEXT,
            artist TEXT,
            title TEXT,
            raw_line TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            event_id TEXT,
            sent INTEGER DEFAULT 0,
            sent_at DATETIME
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS file_offsets (
            filename TEXT PRIMARY KEY,
            offset INTEGER
        )
    """)

    conn.commit()
    conn.close()


def _compute_event_id(filename, play_time, event_type, artist, title, raw_line):
    base = f"{filename}|{play_time}|{event_type}|{artist or ''}|{title or ''}|{raw_line or ''}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

# get last read position for a file
def get_last_offset(filename):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT offset FROM file_offsets WHERE filename=?", (filename,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else 0

# save new offset after reading
def save_offset(filename, offset):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("REPLACE INTO file_offsets (filename, offset) VALUES (?, ?)", (filename, offset))
    conn.commit()
    conn.close()

# parse Jazler log line
def parse_line(line):
    try:
        parts = line.strip().split(" ", 2)
        play_time = parts[0]  # HH:MM:SS
        event_type = parts[1]  # SONG, SPOT, JING, SWEE, MISC
        rest = parts[2] if len(parts) > 2 else ""

        artist, title = None, None
        if event_type == "SONG" and " - " in rest:
            artist, title = rest.split(" - ", 1)
        else:
            title = rest

        return play_time, event_type, artist, title
    except Exception:
        return None, None, None, line.strip()

# save line into database
def save_to_db(filename, line):
    play_time, event_type, artist, title = parse_line(line)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    event_id = _compute_event_id(filename, play_time, event_type, artist, title, line.strip())
    cursor.execute("""
        INSERT INTO logs (filename, play_time, event_type, artist, title, raw_line, event_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (filename, play_time, event_type, artist, title, line.strip(), event_id))
    conn.commit()
    conn.close()
    return play_time, event_type, artist, title, event_id

# push to remote server
def push_to_remote(filename, play_time, event_type, artist, title, raw_line, event_id, is_backfill=False, local_timestamp=None):
    data = {
        "filename": filename,
        "play_time": play_time,
        "event_type": event_type,
        "artist": artist,
        "title": title,
        "raw_line": raw_line,
        "local_timestamp": (local_timestamp or datetime.now().isoformat()),
        "event_id": event_id,
        "is_backfill": bool(is_backfill),
    }
    try:
        resp = requests.post(REMOTE_URL, json=data, timeout=5)
        if resp.status_code == 200:
            console.log(f"[blue]✅ Sent to VPS[/blue] {data}")
            # mark as sent in local DB
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute("UPDATE logs SET sent=1, sent_at=? WHERE event_id=?", (datetime.now().isoformat(), event_id))
            conn.commit()
            conn.close()
        else:
            console.log(f"[red]⚠️ VPS error {resp.status_code}[/red]: {resp.text}")
    except Exception as e:
        console.log(f"[red]⚠️ Could not send to VPS: {e}[/red]")


def backfill_unsent(batch_size=100, sleep_seconds=3):
    while True:
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, filename, play_time, event_type, artist, title, raw_line, timestamp, event_id FROM logs WHERE sent=0 ORDER BY timestamp ASC LIMIT ?",
                (batch_size,)
            )
            rows = cursor.fetchall()
            conn.close()

            if not rows:
                time.sleep(sleep_seconds)
                continue

            items = []
            ids = []
            for r in rows:
                (_id, filename, play_time, event_type, artist, title, raw_line, ts, event_id) = r
                ids.append((_id, event_id))
                items.append({
                    "filename": filename,
                    "play_time": play_time,
                    "event_type": event_type,
                    "artist": artist,
                    "title": title,
                    "raw_line": raw_line,
                    "local_timestamp": ts,
                    "event_id": event_id,
                    "is_backfill": True,
                })

            try:
                resp = requests.post(REMOTE_BULK_URL, json={"items": items, "is_backfill": True}, timeout=20)
                if resp.status_code == 200:
                    # mark all as sent
                    conn = sqlite3.connect(DB_FILE)
                    cursor = conn.cursor()
                    now_iso = datetime.now().isoformat()
                    cursor.executemany("UPDATE logs SET sent=1, sent_at=? WHERE id=?", [(now_iso, _id) for (_id, _eid) in ids])
                    conn.commit()
                    conn.close()
                    console.log(f"[blue]✅ Backfilled {len(ids)} events[/blue]")
                else:
                    console.log(f"[red]⚠️ Backfill error {resp.status_code}[/red]: {resp.text}")
                    time.sleep(sleep_seconds)
            except Exception as e:
                console.log(f"[red]⚠️ Backfill request failed: {e}[/red]")
                time.sleep(sleep_seconds)
        except Exception as e:
            console.log(f"[red]Backfill loop error: {e}[/red]")
            time.sleep(sleep_seconds)

class JazlerHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith(".txt"):
            filename = os.path.basename(event.src_path)
            try:
                last_offset = get_last_offset(filename)
                with open(event.src_path, "r", encoding="utf-8") as f:
                    f.seek(last_offset)
                    for line in f:
                        if line.strip():
                            console.log(f"[green]{line.strip()}[/green]")
                            play_time, event_type, artist, title, event_id = save_to_db(filename, line)
                            push_to_remote(filename, play_time, event_type, artist, title, line.strip(), event_id, is_backfill=False)
                    new_offset = f.tell()
                save_offset(filename, new_offset)
            except Exception as e:
                console.log(f"[red]Error reading file {filename}: {e}[/red]")

def get_today_logfile():
    today = datetime.now().strftime("%Y-%m-%d") + ".txt"
    return os.path.join(NETWORK_PATH, today)

if __name__ == "__main__":
    init_db()
    console.log("✅ Database initialized")

    # start backfill worker thread
    backfill_thread = threading.Thread(target=backfill_unsent, name="backfill", daemon=True)
    backfill_thread.start()

    observer = Observer()
    event_handler = JazlerHandler()
    observer.schedule(event_handler, NETWORK_PATH, recursive=False)
    observer.start()
    console.log(f"👀 Monitoring Jazler logs at: {NETWORK_PATH}")

    try:
        while True:
            today_file = get_today_logfile()
            if not os.path.exists(today_file):
                console.log(f"[yellow]Waiting for today's log file: {today_file}[/yellow]")
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
