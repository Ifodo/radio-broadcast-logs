import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

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
import atexit
import re
from dotenv import load_dotenv
load_dotenv()
AUDIO_UPLOAD_URL = os.getenv("AUDIO_UPLOAD_URL")  # e.g., http://yourserver:8000/audio_logs

import shutil

# console output helper
console = Console()

# your Jazler network path (folder, not file)
NETWORK_PATH = r"\\Ifodo\c\Jazler SOHO\Logs\Main"

# SQLite database file
DB_FILE = "jazler_logs.db"

# local daily logs directory
LOGS_DIR = "logs"

# remote ingest endpoint
REMOTE_URL = "https://rbs.elektranbroadcast.com/ingest"
REMOTE_BULK_URL = REMOTE_URL + "/bulk"

# --- AUDIO LOGS WATCHER CONFIG ---
AUDIO_LOGS_PATH = r"\\Ifodo\c\rl_audio_logs"
AUDIO_LOG_TXT = "audio_log.txt"
AUDIO_LOG_SENT = "audio_log_sent.txt"
AUDIO_LOG_SIZE_KB = 14063
AUDIO_LOG_SIZE_BYTES = AUDIO_LOG_SIZE_KB * 1024
AUDIO_LOG_PATTERN = re.compile(r"^\d{8}-\d{6}\.mp3$")
AUDIO_LOG_MIN_SIZE_BYTES = 1  # Any file > 0 KB

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


def migrate_db():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(logs)")
        cols = [row[1] for row in cursor.fetchall()]
        if "event_id" not in cols:
            cursor.execute("ALTER TABLE logs ADD COLUMN event_id TEXT")
        if "sent" not in cols:
            cursor.execute("ALTER TABLE logs ADD COLUMN sent INTEGER DEFAULT 0")
        if "sent_at" not in cols:
            cursor.execute("ALTER TABLE logs ADD COLUMN sent_at DATETIME")
        conn.commit()
        conn.close()
    except Exception as e:
        console.log(f"[yellow]DB migration notice: {e}[/yellow]")


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
            migrate_db()
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

def load_sent_audio_files():
    if not os.path.exists(AUDIO_LOG_SENT):
        return set()
    with open(AUDIO_LOG_SENT, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def mark_audio_file_sent(filename):
    with open(AUDIO_LOG_SENT, "a", encoding="utf-8") as f:
        f.write(filename + "\n")

def append_to_audio_log_txt(filename):
    with open(AUDIO_LOG_TXT, "a", encoding="utf-8") as f:
        f.write(filename + "\n")

def upload_audio_file(filepath):
    if not AUDIO_UPLOAD_URL:
        console.log("[red]AUDIO_UPLOAD_URL not set in .env![/red]")
        return False
    fname = os.path.basename(filepath)
    try:
        with open(filepath, "rb") as f:
            files = {"file": (fname, f, "audio/mpeg")}
            resp = requests.post(AUDIO_UPLOAD_URL, files=files, timeout=30)
        if resp.status_code == 200:
            console.log(f"[green]Uploaded audio file:[/green] {fname}")
            return True
        else:
            console.log(f"[red]Failed to upload {fname}: {resp.status_code} {resp.text}")
            return False
    except Exception as e:
        console.log(f"[red]Exception uploading {fname}: {e}[/red]")
        return False

def process_audio_log_txt():
    if not os.path.exists(AUDIO_LOG_TXT):
        return
    with open(AUDIO_LOG_TXT, "r", encoding="utf-8") as f:
        files = [line.strip() for line in f if line.strip()]
    if not files:
        return
    remaining = []
    for filepath in files:
        if not os.path.exists(filepath):
            console.log(f"[yellow]File not found, skipping: {filepath}[/yellow]")
            continue
        if upload_audio_file(filepath):
            mark_audio_file_sent(os.path.basename(filepath))
        else:
            remaining.append(filepath)
    # Rewrite audio_log.txt with only files that failed to upload
    with open(AUDIO_LOG_TXT, "w", encoding="utf-8") as f:
        for path in remaining:
            f.write(path + "\n")

class DailyFileLogger:
    def __init__(self, directory):
        self.directory = directory
        os.makedirs(self.directory, exist_ok=True)
        self.current_date = None
        self.file_handle = None
        self.lock = threading.Lock()
        # Ensure today's file exists immediately
        self._rollover_if_needed()

    def _rollover_if_needed(self):
        date_str = datetime.now().strftime("%Y-%m-%d")
        if date_str != self.current_date or self.file_handle is None:
            if self.file_handle:
                try:
                    self.file_handle.flush()
                    self.file_handle.close()
                except Exception:
                    pass
            self.current_date = date_str
            filepath = os.path.join(self.directory, f"{date_str}.txt")
            self.file_handle = open(filepath, "a", encoding="utf-8")

    def write_line(self, text):
        try:
            with self.lock:
                self._rollover_if_needed()
                self.file_handle.write(text + "\n")
                self.file_handle.flush()
        except Exception as e:
            console.log(f"[yellow]File logging issue: {e}[/yellow]")

    def close(self):
        with self.lock:
            if self.file_handle:
                try:
                    self.file_handle.flush()
                    self.file_handle.close()
                finally:
                    self.file_handle = None

class JazlerHandler(FileSystemEventHandler):
    def __init__(self, daily_logger):
        super().__init__()
        self.daily_logger = daily_logger
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
                            self.daily_logger.write_line(line.strip())
                            play_time, event_type, artist, title, event_id = save_to_db(filename, line)
                            push_to_remote(filename, play_time, event_type, artist, title, line.strip(), event_id, is_backfill=False)
                    new_offset = f.tell()
                save_offset(filename, new_offset)
            except Exception as e:
                console.log(f"[red]Error reading file {filename}: {e}[/red]")

class AudioLogHandler(FileSystemEventHandler):
    def __init__(self):
        super().__init__()
        self.sent_files = load_sent_audio_files()

    def _check_and_log(self, path):
        if not os.path.isfile(path):
            return
        fname = os.path.basename(path)
        if fname in self.sent_files:
            return
        if not AUDIO_LOG_PATTERN.match(fname):
            return
        try:
            size = os.path.getsize(path)
            if size >= AUDIO_LOG_MIN_SIZE_BYTES:
                append_to_audio_log_txt(path)
                mark_audio_file_sent(fname)
                self.sent_files.add(fname)
                console.log(f"[cyan]Audio log ready: {fname} ({size} bytes)[/cyan]")
        except Exception as e:
            console.log(f"[yellow]Audio log check error for {fname}: {e}[/yellow]")

    def on_created(self, event):
        if not event.is_directory:
            self._check_and_log(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            self._check_and_log(event.src_path)

def get_today_logfile():
    today = datetime.now().strftime("%Y-%m-%d") + ".txt"
    return os.path.join(NETWORK_PATH, today)

if __name__ == "__main__":
    init_db()
    migrate_db()
    console.log("✅ Database initialized")

    # start backfill worker thread
    backfill_thread = threading.Thread(target=backfill_unsent, name="backfill", daemon=True)
    backfill_thread.start()

    # initialize daily file logger
    daily_logger = DailyFileLogger(LOGS_DIR)
    atexit.register(daily_logger.close)

    observer = Observer()
    event_handler = JazlerHandler(daily_logger)
    observer.schedule(event_handler, NETWORK_PATH, recursive=False)
    observer.start()
    console.log(f"👀 Monitoring Jazler logs at: {NETWORK_PATH}")

    # Start audio logs watcher
    audio_handler = AudioLogHandler()
    audio_observer = Observer()
    audio_observer.schedule(audio_handler, AUDIO_LOGS_PATH, recursive=False)
    audio_observer.start()
    console.log(f"👀 Monitoring audio logs at: {AUDIO_LOGS_PATH}")

    # Periodically process audio_log.txt for uploads
    def audio_upload_loop():
        while True:
            process_audio_log_txt()
            time.sleep(30)  # Check every 30 seconds
    upload_thread = threading.Thread(target=audio_upload_loop, name="audio_upload", daemon=True)
    upload_thread.start()

    try:
        while True:
            today_file = get_today_logfile()
            if not os.path.exists(today_file):
                console.log(f"[yellow]Waiting for today's log file: {today_file}[/yellow]")
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
        audio_observer.stop()
    observer.join()
    audio_observer.join()
