import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import time
import os
import sqlite3
import requests
import hashlib
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from rich.console import Console
from datetime import datetime
import re
import atexit
import json
from bs4 import BeautifulSoup
# Anchor all local files to this script's directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

VPS_BASE = "https://rbs.elektranbroadcast.com"

# =========================
# CONFIG
# =========================
# Console logging to system stdout only
console = Console()

# --- Local paths ---
NETWORK_PATH = r"\\Ifodo\c\Jazler SOHO\Logs\Main"   # Jazler daily text logs
AUDIO_LOGS_PATH = r"\\Ifodo\c\rl_audio_logs"        # Jazler audio mp3 logs
HTML_REPORT_PATH = r"\\Ifodo\c\jazlerReport\Report.html"  # Jazler HTML report
HTML_JSON_LOG = os.path.join(BASE_DIR, "html_reports.json")  # JSON log for HTML reports
HTML_COUNTER_FILE = os.path.join(BASE_DIR, "html_counter.txt")  # Counter for processed records
AUDIO_LOG_SENT = os.path.join(BASE_DIR, "sent_files.txt")
AUDIO_LOG_TXT = os.path.join(BASE_DIR, "audio_log.txt")
AUDIO_UPLOAD_LOG = os.path.join(BASE_DIR, "audio_uploads.txt")
ENABLE_UPLOADS = True  # Enable audio file uploads to server

# --- Database ---
DB_FILE = os.path.join(BASE_DIR, "jazler_logs.db")

# --- Remote VPS ---
INGEST_URL = f"{VPS_BASE}/ingest"
AUDIO_UPLOAD_URL = f"{VPS_BASE}/audio_logs"
SPOTS_REPORT_URL = f"{VPS_BASE}/jazler_spots_report"

# Retry configuration for server communication
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# --- Audio settings ---
AUDIO_LOG_PATTERN = re.compile(r"^\d{8}-\d{6}\.mp3$")
AUDIO_LOG_MIN_SIZE_BYTES = 1  # Only skip files with 0 bytes


# =========================
# DB SETUP
# =========================
def init_db():
    """Initialize database with all required tables"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Drop existing html_records table to ensure clean schema
    cursor.execute("DROP TABLE IF EXISTS html_records")

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

    # Recreate html_records table to ensure latest schema
    cursor.execute("DROP TABLE IF EXISTS html_records")
    cursor.execute("""
        CREATE TABLE html_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT UNIQUE NOT NULL,
            station_address TEXT,
            print_date TEXT,
            running_between TEXT,
            ad_company TEXT,
            client TEXT,
            total_spots INTEGER DEFAULT 0,
            days INTEGER DEFAULT 0,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active INTEGER DEFAULT 1
        )
    """)
    
    console.log("[green]✅ Database initialized with updated schema[/green]")

    conn.commit()
    conn.close()


def get_last_offset(filename):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT offset FROM file_offsets WHERE filename=?", (filename,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else 0


def save_offset(filename, offset):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("REPLACE INTO file_offsets (filename, offset) VALUES (?, ?)", (filename, offset))
    conn.commit()
    conn.close()


def send_records_to_server(records):
    """Send HTML records to the server with retry logic"""
    if not records:
        return
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                SPOTS_REPORT_URL,
                json=records,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                console.log(f"[green]✅ Server sync successful:[/green]")
                console.log(f"[green]   New records: {result.get('inserted', 0)}[/green]")
                console.log(f"[green]   Updated records: {result.get('updated', 0)}[/green]")
                if result.get('errors'):
                    console.log("[yellow]⚠️ Some records had errors:[/yellow]")
                    for error in result['errors']:
                        console.log(f"[yellow]   {error}[/yellow]")
                return True
            else:
                console.log(f"[red]❌ Server returned error {response.status_code}: {response.text}[/red]")
                
        except Exception as e:
            console.log(f"[red]❌ Error sending to server (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)}[/red]")
        
        if attempt < MAX_RETRIES - 1:
            console.log(f"[yellow]⏳ Retrying in {RETRY_DELAY} seconds...[/yellow]")
            time.sleep(RETRY_DELAY)
    
    return False


def save_html_records_to_db(records):
    """Save/update HTML records in database. Updates existing records, inserts new ones."""
    if not records:
        console.log("[yellow]No records to save to database[/yellow]")
        return

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        console.log("\n[blue]DATABASE OPERATIONS:[/blue]")
        console.log("[blue]===================[/blue]")
        
        # First, mark all existing records as inactive
        cursor.execute("UPDATE html_records SET is_active = 0")
        
        # Track changes for logging
        updated_count = 0
        new_count = 0
        
        for record in records:
            # Check if record exists
            cursor.execute("SELECT id, total_spots, days FROM html_records WHERE title = ?", 
                          (record['title'],))
            existing = cursor.fetchone()
            
            if existing:
                # Update existing record
                record_id, old_spots, old_days = existing
                cursor.execute("""
                    UPDATE html_records 
                    SET ad_company = ?, client = ?, total_spots = ?, days = ?, 
                        station_address = ?, print_date = ?, running_between = ?,
                        last_updated = CURRENT_TIMESTAMP, is_active = 1
                    WHERE id = ?
                """, (record['ad_company'], record['client'], record['total_spots'], 
                     record['days'], record['station_address'], record['print_date'],
                     record['running_between'], record_id))
                
                console.log(f"[cyan]📝 Updated DB Record:[/cyan]")
                console.log(f"[cyan]   Title: {record['title'][:40]}...[/cyan]")
                console.log(f"[cyan]   Station: {record['station_address']}[/cyan]")
                console.log(f"[cyan]   Print Date: {record['print_date']}[/cyan]")
                console.log(f"[cyan]   Running Between: {record['running_between']}[/cyan]")
                console.log(f"[cyan]   Ad Company: {record['ad_company']}[/cyan]")
                console.log(f"[cyan]   Client: {record['client']}[/cyan]")
                console.log(f"[cyan]   Spots: {old_spots}→{record['total_spots']}[/cyan]")
                console.log(f"[cyan]   Days: {old_days}→{record['days']}[/cyan]")
                console.log("")
                updated_count += 1
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO html_records (
                        title, ad_company, client, total_spots, days,
                        station_address, print_date, running_between
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (record['title'], record['ad_company'], record['client'], 
                     record['total_spots'], record['days'], record['station_address'],
                     record['print_date'], record['running_between']))
                
                console.log(f"[green]🆕 New DB Record:[/green]")
                console.log(f"[green]   Title: {record['title'][:40]}...[/green]")
                console.log(f"[green]   Station: {record['station_address']}[/green]")
                console.log(f"[green]   Print Date: {record['print_date']}[/green]")
                console.log(f"[green]   Running Between: {record['running_between']}[/green]")
                console.log(f"[green]   Ad Company: {record['ad_company']}[/green]")
                console.log(f"[green]   Client: {record['client']}[/green]")
                console.log(f"[green]   Spots: {record['total_spots']}[/green]")
                console.log(f"[green]   Days: {record['days']}[/green]")
                console.log("")
                new_count += 1
        
        conn.commit()
        
        # Verify saved records
        cursor.execute("""
            SELECT title, station_address, print_date, total_spots, days 
            FROM html_records 
            WHERE is_active = 1
        """)
        saved_records = cursor.fetchall()
        
        console.log(f"[blue]DATABASE SUMMARY:[/blue]")
        console.log(f"[blue]=================[/blue]")
        console.log(f"[blue]✓ {new_count} new records saved[/blue]")
        console.log(f"[blue]✓ {updated_count} records updated[/blue]")
        console.log(f"[blue]✓ {len(saved_records)} total active records in database[/blue]")
        
        # Show inactive records (removed from HTML)
        cursor.execute("SELECT title FROM html_records WHERE is_active = 0")
        inactive_records = cursor.fetchall()
        if inactive_records:
            console.log(f"[yellow]⚠️ {len(inactive_records)} records marked inactive[/yellow]")
            
    except Exception as e:
        console.log(f"[red]❌ Database error: {e}[/red]")
        conn.rollback()
    finally:
        conn.close()


def get_html_records_summary():
    """Get summary of HTML records from database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        # Get counts
        cursor.execute("SELECT COUNT(*) FROM html_records WHERE is_active = 1")
        active_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM html_records WHERE is_active = 0")
        inactive_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT SUM(total_spots) FROM html_records WHERE is_active = 1")
        total_spots = cursor.fetchone()[0] or 0
        
        return {
            'active_records': active_count,
            'inactive_records': inactive_count,
            'total_spots': total_spots
        }
        
    except Exception as e:
        console.log(f"[red]❌ Error getting DB summary: {e}[/red]")
        return None
    finally:
        conn.close()


# =========================
# TEXT LOG PARSER
# =========================
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

    # Forward to VPS
    payload = {
        "filename": filename,
        "play_time": play_time,
        "event_type": event_type,
        "artist": artist,
        "title": title,
        "raw_line": line.strip()
    }
    try:
        resp = requests.post(INGEST_URL, json=payload, timeout=10)
        if resp.status_code == 200:
            console.log(f"[cyan]📡 Sent log to VPS: {payload}[/cyan]")
        else:
            console.log(f"[red]⚠️ VPS ingest failed[/red]: {resp.text}")
    except Exception as e:
        console.log(f"[red]❌ VPS connection error[/red]: {e}")


# =========================
# WATCHERS
# =========================
class JazlerHandler(FileSystemEventHandler):
    """Handles Jazler text logs"""
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
                            save_to_db(filename, line)
                    new_offset = f.tell()
                save_offset(filename, new_offset)
            except Exception as e:
                console.log(f"[red]Error reading file {filename}: {e}[/red]")


class AudioFileHandler(FileSystemEventHandler):
    """Handles audio mp3 logs (no upload here, just for future use)"""
    pass


class HTMLReportHandler(FileSystemEventHandler):
    """Handles HTML report monitoring and content extraction"""
    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith(".html"):
            try:
                process_html_report(event.src_path)
            except Exception as e:
                console.log(f"[red]Error processing HTML {event.src_path}: {e}[/red]")




def get_file_hash(filepath):
    """Calculate SHA-256 hash of a file"""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def parse_date(date_str):
    """Parse date string in format MM/DD/YYYY or M/D/YYYY"""
    try:
        return datetime.strptime(date_str.strip(), '%m/%d/%Y')
    except ValueError:
        return None

def extract_field(content, field_name):
    """Extract field value from content"""
    try:
        start_idx = content.index(f"{field_name}:")
        line_end = content.index('\n', start_idx)
        value = content[start_idx + len(field_name) + 1:line_end].strip()
        return value
    except (ValueError, IndexError):
        return None




def to_iso_date(value):
    """Return YYYY-MM-DD string or None."""
    if not value:
        return None
    try:
        return value.strftime('%Y-%m-%d')
    except Exception:
        return str(value)


def to_iso_datetime(value):
    """Return ISO8601 datetime string or None."""
    if not value:
        return None
    try:
        return value.isoformat()
    except Exception:
        return str(value)


def get_html_counter():
    """Get current counter value"""
    try:
        if os.path.exists(HTML_COUNTER_FILE):
            with open(HTML_COUNTER_FILE, 'r') as f:
                return int(f.read().strip())
        return 0
    except:
        return 0


def increment_html_counter(count):
    """Increment counter by count"""
    current = get_html_counter()
    new_count = current + count
    try:
        with open(HTML_COUNTER_FILE, 'w') as f:
            f.write(str(new_count))
        return new_count
    except:
        return current


def parse_html_report(filepath):
    """Parse HTML report and extract all records using CSS class-based parsing"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        records = []
        seen_titles = set()  # Track titles to avoid duplicates
        
        # Extract header information first
        station_address = None
        print_date = None
        running_between = None
        
        # Find station address (usually in first few rows)
        for td in soup.find_all('td'):
            if td.get('rowspan') == '3' and td.get('colspan') == '8':
                station_address = td.get_text(strip=True)
                console.log(f"[cyan]STATION ADDRESS: {station_address}[/cyan]")
                break
        
        # Find print date
        for td in soup.find_all('td'):
            text = td.get_text(strip=True)
            if text.startswith('Print Date:'):
                print_date = text.replace('Print Date:', '').strip()
                console.log(f"[cyan]PRINT DATE: {print_date}[/cyan]")
                break
        
        # Find running_between date range
        for td in soup.find_all('td'):
            text = td.get_text(strip=True)
            if text.startswith('Running between:'):
                running_between = text.replace('Running between:', '').strip()
                console.log(f"[cyan]RUNNING BETWEEN: {running_between}[/cyan]")
                break
                
        # Store header info to be used in each record
        header_info = {
            'station_address': station_address,
            'print_date': print_date,
            'running_between': running_between
        }
        
        # Find all title elements (they start each record)
        title_elements = soup.find_all('td', class_='sa8eef915')
        
        console.log(f"[cyan]CSS CLASS SEARCH: Found {len(title_elements)} potential title elements[/cyan]")
        
        # If no elements found with that class, try broader search
        if not title_elements:
            console.log("[yellow]SEARCH STATUS: No CSS class matches, trying broader search...[/yellow]")
            # Look for any td containing "Title"
            all_tds = soup.find_all('td')
            for td in all_tds:
                if td.get_text(strip=True) == 'Title':
                    title_elements.append(td)
            console.log(f"[cyan]TITLE ELEMENTS: Found {len(title_elements)} elements containing 'Title'[/cyan]")
        
        for title_elem in title_elements:
            if title_elem.get_text(strip=True) == 'Title':
                # Extract the record data from this title element
                record = extract_record_from_css_structure(soup, title_elem, header_info)
                if record:
                    # Check for duplicates by title
                    if record['title'] not in seen_titles:
                        records.append(record)
                        seen_titles.add(record['title'])
                        console.log(f"[green]✅ Extracted: {record['title'][:40]}...[/green]")
                    else:
                        console.log(f"[yellow]⚠️ Skipped duplicate: {record['title'][:40]}...[/yellow]")
        
        console.log(f"[cyan]Total unique records extracted: {len(records)}[/cyan]")
        return records
        
    except Exception as e:
        console.log(f"[red]Error parsing HTML: {e}[/red]")
        return []


def extract_record_from_css_structure(soup, title_elem, header_info):
    """Extract record from the CSS-based HTML structure around a title element"""
    try:
        # Get title value from the adjacent cell
        title_value_elem = title_elem.find_next_sibling('td', class_='sf0169225')
        
        # If specific class not found, try next sibling regardless of class
        if not title_value_elem:
            title_value_elem = title_elem.find_next_sibling('td')
        
        if not title_value_elem:
            console.log("[red]  No title value element found[/red]")
            return None
            
        title = title_value_elem.get_text(strip=True)
        console.log(f"[cyan]  Title value: {title}[/cyan]")
        
        record = {
            'title': title,
            'ad_company': '',
            'client': '',
            'total_spots': 0,
            'days': 0,
            'station_address': header_info['station_address'],
            'print_date': header_info['print_date'],
            'running_between': header_info['running_between']
        }
        
        # Debug: Print title being processed
        console.log(f"[yellow]Processing title: {title}[/yellow]")
        
        # Find the current row and look in nearby rows for the associated data
        current_row = title_elem.find_parent('tr')
        if not current_row:
            return None
        
        # Look in the next several rows for associated data
        # But stop when we hit another "Title" to avoid cross-contamination
        next_rows = []
        current = current_row
        for _ in range(8):  # Look ahead up to 8 rows to find all data
            current = current.find_next_sibling('tr')
            if current:
                # Stop if we find another title (start of next record)
                title_check = current.find('td', class_='sa8eef915')
                if title_check and title_check.get_text(strip=True) == 'Title':
                    break
                next_rows.append(current)
            else:
                break
        
        # Search in current row and next rows for our data
        all_rows = [current_row] + next_rows
        
        for row in all_rows:
            cells = row.find_all('td')
            
            for i, cell in enumerate(cells):
                cell_text = cell.get_text(strip=True)
                
                # Debug: Show what we're checking
                if cell_text in ['Ad Company:', 'Client:', 'Total Spots:', 'Days:']:
                    console.log(f"[yellow]  Found label: {cell_text}[/yellow]")
                
                # Ad Company (look for exact pattern in same row)
                if cell_text == 'Ad Company:':
                    # Find the next cell with content (try specific class first, then any cell)
                    for j in range(i+1, len(cells)):
                        next_cell = cells[j]
                        cell_classes = next_cell.get('class', [])
                        cell_content = next_cell.get_text(strip=True)
                        
                        # Try specific class first, then any non-empty cell
                        if ('sd8c11fa4' in cell_classes or cell_content) and cell_content:
                            if cell_content != 'Media Shop:' and not record['ad_company']:
                                record['ad_company'] = cell_content
                                console.log(f"[yellow]  Found ad_company: {cell_content}[/yellow]")
                                break
                
                # Client (look for exact pattern in same row)
                elif cell_text == 'Client:':
                    # Find the next cell with content (try specific class first, then any cell)
                    for j in range(i+1, len(cells)):
                        next_cell = cells[j]
                        cell_classes = next_cell.get('class', [])
                        cell_content = next_cell.get_text(strip=True)
                        
                        # Try specific class first, then any non-empty cell
                        if ('sd8c11fa4' in cell_classes or cell_content) and cell_content:
                            if cell_content != 'Media Shop:' and not record['client']:
                                record['client'] = cell_content
                                console.log(f"[yellow]  Found client: {cell_content}[/yellow]")
                                break
                
                # Total Spots
                elif cell_text == 'Total Spots:':
                    console.log(f"[yellow]  Looking for spots after 'Total Spots:' label[/yellow]")
                    # Find the next cell with a number (try specific class first, then any cell)
                    for j in range(i+1, len(cells)):
                        next_cell = cells[j]
                        cell_classes = next_cell.get('class', [])
                        cell_content = next_cell.get_text(strip=True)
                        console.log(f"[yellow]    Cell {j}: classes={cell_classes}, content='{cell_content}'[/yellow]")
                        
                        # Look for numbers in cell content
                        import re
                        spots_match = re.search(r'\d+', cell_content)
                        if spots_match:
                            record['total_spots'] = int(spots_match.group())
                            console.log(f"[green]  ✅ Found spots: {record['total_spots']}[/green]")
                            break
                
                # Days
                elif cell_text == 'Days:':
                    console.log(f"[yellow]  Looking for days after 'Days:' label[/yellow]")
                    # Find the next cell with a number (try specific class first, then any cell)
                    for j in range(i+1, len(cells)):
                        next_cell = cells[j]
                        cell_classes = next_cell.get('class', [])
                        cell_content = next_cell.get_text(strip=True)
                        
                        # Look for numbers in cell content
                        import re
                        days_match = re.search(r'\d+', cell_content)
                        if days_match:
                            record['days'] = int(days_match.group())
                            console.log(f"[green]  ✅ Found days: {record['days']}[/green]")
                            break
        
        # Only return if we have meaningful data
        if record['title'] and (record['ad_company'] or record['client']):
            return record
        
    except Exception as e:
        console.log(f"[red]Error extracting from CSS structure: {e}[/red]")
    
    return None


def extract_record_from_cells(cells):
    """Extract record from table cells (legacy method)"""
    try:
        if len(cells) >= 5:
            # Clean and extract data
            title = cells[0].strip()
            ad_company = cells[1].strip()
            client = cells[2].strip()
            
            # Extract numbers from spots and days
            spots_text = cells[3].strip()
            days_text = cells[4].strip()
            
            import re
            spots_match = re.search(r'\d+', spots_text)
            days_match = re.search(r'\d+', days_text)
            
            total_spots = int(spots_match.group()) if spots_match else 0
            days = int(days_match.group()) if days_match else 0
            
            # Only return if we have meaningful data
            if title and (ad_company or client):
                return {
                    'title': title,
                    'ad_company': ad_company, 
                    'client': client,
                    'total_spots': total_spots,
                    'days': days
                }
    except Exception as e:
        console.log(f"[red]Error extracting from cells: {e}[/red]")
    return None


def parse_html_text_blocks(soup):
    """Parse HTML when not in table format"""
    records = []
    text = soup.get_text()
    
    # Look for patterns like "Title:", "Ad Company:", etc.
    import re
    
    # Find all Title occurrences and extract following data
    title_pattern = r'Title:\s*([^\n]+)'
    titles = re.findall(title_pattern, text, re.IGNORECASE)
    
    for title in titles:
        # Try to find the associated data around this title
        title_pos = text.find(f"Title: {title}")
        if title_pos != -1:
            # Extract text around this position
            section = text[title_pos:title_pos+500]
            record = extract_record_from_section(section)
            if record:
                records.append(record)
    
    return records


def extract_record_from_section(section):
    """Extract record data from a text section"""
    import re
    
    try:
        title_match = re.search(r'Title:\s*([^\n]+)', section, re.IGNORECASE)
        ad_company_match = re.search(r'Ad Company:\s*([^\n]+)', section, re.IGNORECASE)
        client_match = re.search(r'Client:\s*([^\n]+)', section, re.IGNORECASE)
        spots_match = re.search(r'Total Spots:\s*(\d+)', section, re.IGNORECASE)
        days_match = re.search(r'Days:\s*(\d+)', section, re.IGNORECASE)
        
        if title_match:
            return {
                'title': title_match.group(1).strip(),
                'ad_company': ad_company_match.group(1).strip() if ad_company_match else '',
                'client': client_match.group(1).strip() if client_match else '',
                'total_spots': int(spots_match.group(1)) if spots_match else 0,
                'days': int(days_match.group(1)) if days_match else 0
            }
    except:
        pass
    
    return None


def save_html_to_json(records, running_between_constant="2025-09-27 & 2025-09-30"):
    """Save HTML records to JSON file"""
    try:
        timestamp = datetime.now().isoformat()
        
        # Load existing data
        existing_data = []
        if os.path.exists(HTML_JSON_LOG):
            try:
                with open(HTML_JSON_LOG, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            except:
                existing_data = []
        
        # Add each record
        for record in records:
            log_entry = {
                "timestamp": timestamp,
                "running_between": running_between_constant,
                "title": record['title'],
                "ad_company": record['ad_company'],
                "client": record['client'],
                "total_spots": record['total_spots'],
                "days": record['days']
            }
            existing_data.append(log_entry)
        
        # Keep only last 500 entries
        if len(existing_data) > 500:
            existing_data = existing_data[-500:]
        
        # Write back to file
        with open(HTML_JSON_LOG, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=2, ensure_ascii=False)
            
        console.log(f"[green]✅ Saved {len(records)} records to JSON: {HTML_JSON_LOG}[/green]")
        
    except Exception as e:
        console.log(f"[red]❌ Error saving HTML to JSON: {e}[/red]")


def process_html_report(filepath):
    """Process HTML file and extract all records"""
    if not os.path.exists(filepath):
        console.log(f"[yellow]HTML file not found: {filepath}[/yellow]")
        return

    console.log("[blue]STATUS: Processing HTML report...[/blue]")
    
    # Parse HTML and extract records (already deduplicated in parsing)
    records = parse_html_report(filepath)
    
    if not records:
        console.log("[yellow]STATUS: No valid records found in HTML file[/yellow]")
        return
    
    # Records are already validated in the parser, just display them
    console.log(f"[cyan]📊 Processing {len(records)} unique records[/cyan]")
    
    for i, record in enumerate(records, 1):
        log_entry = {
            "record_number": i,
            "station_address": record.get('station_address', ''),
            "print_date": record.get('print_date', ''),
            "running_between": record.get('running_between', ''),
            "title": record['title'],
            "ad_company": record['ad_company'],
            "client": record['client'],
            "total_spots": record['total_spots'],
            "days": record['days']
        }
        console.log(f"[white]📋 Record {i}:[/white]")
        console.log(f"[white]   STATION ADDRESS: {log_entry['station_address']}[/white]")
        console.log(f"[white]   PRINT DATE: {log_entry['print_date']}[/white]")
        console.log(f"[white]   RUNNING BETWEEN: {log_entry['running_between']}[/white]")
        console.log(f"[white]   TITLE: {log_entry['title']}[/white]")
        console.log(f"[white]   AD COMPANY: {log_entry['ad_company']}[/white]")
        console.log(f"[white]   CLIENT: {log_entry['client']}[/white]")
        console.log(f"[white]   TOTAL SPOTS: {log_entry['total_spots']}[/white]")
        console.log(f"[white]   DAYS: {log_entry['days']}[/white]")
        console.log("")  # Empty line between records
    
    # Save records to JSON file
    save_html_to_json(records)
    
    # Save records to database (updates existing, inserts new)
    save_html_records_to_db(records)
    
    # Get database summary for display
    db_summary = get_html_records_summary()
    if db_summary:
        console.log(f"[blue]📊 Database: {db_summary['active_records']} active records, {db_summary['total_spots']} total spots[/blue]")
    
    # Send records to server
    console.log("[blue]📡 Syncing records with server...[/blue]")
    send_records_to_server(records)
    
    console.log(f"[green]✅ Processed {len(records)} unique records from HTML file[/green]")


# Helper to load sent files
def load_sent_files():
    if not os.path.exists(AUDIO_LOG_SENT):
        return set()
    with open(AUDIO_LOG_SENT, "r") as f:
        return set(line.strip() for line in f if line.strip())

def save_sent_file(filename):
    with open(AUDIO_LOG_SENT, "a") as f:
        f.write(filename + "\n")

def load_audio_log_txt():
    if not os.path.exists(AUDIO_LOG_TXT):
        return set()
    with open(AUDIO_LOG_TXT, "r") as f:
        return set(line.strip() for line in f if line.strip())

def save_audio_log_txt(files):
    with open(AUDIO_LOG_TXT, "w") as f:
        for file in sorted(files):
            f.write(file + "\n")

def scan_audio_files():
    sent = load_sent_files()
    to_upload = set()
    for fname in os.listdir(AUDIO_LOGS_PATH):
        if not fname.lower().endswith(".mp3"):
            continue
        if not AUDIO_LOG_PATTERN.match(fname):
            continue
        if fname in sent:
            continue
        fpath = os.path.join(AUDIO_LOGS_PATH, fname)
        if not os.path.isfile(fpath):
            continue
        if os.path.getsize(fpath) == 0:
            continue
        to_upload.add(fpath)
    save_audio_log_txt(to_upload)
    return to_upload

def is_file_ready(fpath, wait_time=2, retries=3):
    """Check if file size is stable (not being written)."""
    last_size = -1
    for _ in range(retries):
        if not os.path.exists(fpath):
            return False
        size = os.path.getsize(fpath)
        if size == 0:
            return False
        if size == last_size:
            return True
        last_size = size
        time.sleep(wait_time)
    return False


def upload_audio_files():
    sent = load_sent_files()
    to_upload = load_audio_log_txt()
    still_to_upload = set()
    for fpath in to_upload:
        fname = os.path.basename(fpath)
        if fname in sent:
            continue
        if not os.path.exists(fpath):
            # Remove missing files from queue
            continue
        if not is_file_ready(fpath):
            console.log(f"[yellow]File {fname} not ready (size changing or zero), will retry later.[/yellow]")
            still_to_upload.add(fpath)
            continue
        
        # Upload audio if enabled; otherwise mark as sent to avoid reprocessing
        if ENABLE_UPLOADS:
            try:
                with open(fpath, "rb") as f:
                    resp = requests.post(AUDIO_UPLOAD_URL, files={"file": f}, timeout=60)
                if resp.status_code == 200:
                    size_now = os.path.getsize(fpath)
                    console.log(f"[green]✅ Uploaded audio {fname} | {size_now} bytes[/green]")
                    log_audio_upload(fname, 0, size_now)  # duration set to 0 since we don't check it
                    save_sent_file(fname)
                else:
                    console.log(f"[red]⚠️ Upload failed {fname}[/red]: {resp.text}")
                    still_to_upload.add(fpath)
            except Exception as e:
                console.log(f"[red]❌ Error uploading {fname}: {e}[/red]")
                still_to_upload.add(fpath)
        else:
            # Do not upload; consider analysis complete, prevent reprocessing
            save_sent_file(fname)
            console.log(f"[blue]⬇️ Upload disabled; marked {fname} as processed[/blue]")
    save_audio_log_txt(still_to_upload)


# Helper to record successful uploads with duration

def log_audio_upload(filename, duration_seconds, file_size_bytes):
    try:
        with open(AUDIO_UPLOAD_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().isoformat()} | {filename} | {duration_seconds/60:.2f} min | {file_size_bytes} bytes\n")
    except Exception:
        pass


# =========================
# RUN
# =========================
def get_today_logfile():
    today = datetime.now().strftime("%Y-%m-%d") + ".txt"
    return os.path.join(NETWORK_PATH, today)


if __name__ == "__main__":
    init_db()
    console.log("✅ Database initialized")


    # --- Text logs observer
    observer_txt = Observer()
    observer_txt.schedule(JazlerHandler(), NETWORK_PATH, recursive=False)
    observer_txt.start()
    console.log(f"👀 Monitoring Jazler TEXT logs at: {NETWORK_PATH}")

    # --- Audio logs observer (no upload on event)
    observer_audio = Observer()
    observer_audio.schedule(AudioFileHandler(), AUDIO_LOGS_PATH, recursive=False)
    observer_audio.start()
    console.log(f"🎵 Monitoring Jazler AUDIO logs at: {AUDIO_LOGS_PATH}")

    # --- HTML report observer
    observer_html = Observer()
    observer_html.schedule(HTMLReportHandler(), os.path.dirname(HTML_REPORT_PATH), recursive=False)
    observer_html.start()
    console.log(f"📄 Monitoring HTML report at: {HTML_REPORT_PATH}")

    # Initial HTML processing
    if os.path.exists(HTML_REPORT_PATH):
        try:
            process_html_report(HTML_REPORT_PATH)
        except Exception as e:
            console.log(f"[red]Error processing initial HTML: {e}[/red]")

    # HTML processing now only triggered by file watcher (no periodic checking)


    # --- Periodic audio scan/upload loop
    def audio_upload_loop():
        while True:
            scan_audio_files()
            upload_audio_files()
            time.sleep(30)
    import threading
    upload_thread = threading.Thread(target=audio_upload_loop, name="audio_upload", daemon=True)
    upload_thread.start()

    # Register cleanup
    atexit.register(lambda: observer_txt.stop())
    atexit.register(lambda: observer_txt.join())
    atexit.register(lambda: observer_audio.stop())
    atexit.register(lambda: observer_audio.join())
    atexit.register(lambda: observer_html.stop())
    atexit.register(lambda: observer_html.join())

    try:
        while True:
            today_file = get_today_logfile()
            if not os.path.exists(today_file):
                console.log(f"[yellow]Waiting for today's log file: {today_file}[/yellow]")
            time.sleep(5)
    except KeyboardInterrupt:
        observer_txt.stop()
        observer_audio.stop()
        observer_html.stop()
        observer_txt.join()
        observer_audio.join()
        observer_html.join()
