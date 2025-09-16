import time
import os
import sqlite3
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
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
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
    cursor.execute("""
        INSERT INTO logs (filename, play_time, event_type, artist, title, raw_line)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (filename, play_time, event_type, artist, title, line.strip()))
    conn.commit()
    conn.close()
    return play_time, event_type, artist, title

# push to remote server
def push_to_remote(filename, play_time, event_type, artist, title, raw_line):
    data = {
        "filename": filename,
        "play_time": play_time,
        "event_type": event_type,
        "artist": artist,
        "title": title,
        "raw_line": raw_line,
        "local_timestamp": datetime.now().isoformat()
    }
    try:
        resp = requests.post(REMOTE_URL, json=data, timeout=5)
        if resp.status_code == 200:
            console.log(f"[blue]✅ Sent to VPS[/blue] {data}")
        else:
            console.log(f"[red]⚠️ VPS error {resp.status_code}[/red]: {resp.text}")
    except Exception as e:
        console.log(f"[red]⚠️ Could not send to VPS: {e}[/red]")

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
                            play_time, event_type, artist, title = save_to_db(filename, line)
                            push_to_remote(filename, play_time, event_type, artist, title, line.strip())
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
