import os
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
from datetime import datetime, date, time
from typing import Any, List, Optional

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL is not set in .env file")

# Database setup
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

try:
    conn = engine.connect()
    print("✅ Connected to PostgreSQL successfully")
    conn.close()
except Exception as e:
    print(f"❌ Failed to connect to PostgreSQL: {e}")
    raise

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Define Logs table
class Log(Base):
    __tablename__ = "logs"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String(255))
    play_time = Column(String(50))
    event_type = Column(String(50))
    artist = Column(String(255), nullable=True)
    title = Column(String(255), nullable=True)
    raw_line = Column(Text)
    timestamp = Column(DateTime, default=datetime.utcnow)

# Create tables if they don’t exist
Base.metadata.create_all(bind=engine)

# FastAPI app
app = FastAPI(title="Jazler Monitoring API")

# Pydantic model for request
class LogEntry(BaseModel):
    filename: str
    play_time: str
    event_type: str
    artist: str | None = None
    title: str | None = None
    raw_line: str


# ---------- Utilities ----------
def _parse_air_timestamp(filename: str, play_time_str: str) -> Optional[datetime]:
    """Combine date from filename (YYYY-MM-DD.txt) with HH:MM:SS into a datetime.
    Returns None if parsing fails."""
    try:
        date_part = filename.split(".")[0]
        file_date = datetime.strptime(date_part, "%Y-%m-%d").date()
        play_time_val = datetime.strptime(play_time_str.strip(), "%H:%M:%S").time()
        return datetime.combine(file_date, play_time_val)
    except Exception:
        return None


def _compute_latency_seconds(air_ts: Optional[datetime], local_ts: Optional[datetime]) -> Optional[float]:
    if not air_ts or not local_ts:
        return None
    try:
        delta = (local_ts - air_ts).total_seconds()
        return round(delta, 3)
    except Exception:
        return None

@app.post("/ingest")
def ingest_log(entry: LogEntry):
    db = SessionLocal()
    try:
        log = Log(
            filename=entry.filename,
            play_time=entry.play_time,
            event_type=entry.event_type,
            artist=entry.artist,
            title=entry.title,
            raw_line=entry.raw_line,
        )
        db.add(log)
        db.commit()
        db.refresh(log)
        return {"status": "success", "id": log.id}
    except Exception as e:
        db.rollback()
        return {"status": "error", "detail": str(e)}
    finally:
        db.close()

@app.get("/")
def health_check():
    return {"status": "running", "db": "connected"}


# ---------- Read Endpoints ----------
class EventItem(BaseModel):
    id: int
    station: Optional[str] = None  # placeholder for future multi-station support
    event_type: str
    title: Optional[str]
    artist: Optional[str]
    play_time: Optional[str]
    air_timestamp: Optional[str]
    local_timestamp: Optional[str]
    ingest_latency_seconds: Optional[float]
    raw_line: str
    source_file: Optional[str]


def _serialize_log(log: Log) -> EventItem:
    air_ts = _parse_air_timestamp(log.filename or "", log.play_time or "") if (log.filename and log.play_time) else None
    local_ts = log.timestamp
    return EventItem(
        id=log.id,
        station=None,
        event_type=log.event_type,
        title=log.title,
        artist=log.artist,
        play_time=log.play_time,
        air_timestamp=air_ts.isoformat() if air_ts else None,
        local_timestamp=local_ts.isoformat() if local_ts else None,
        ingest_latency_seconds=_compute_latency_seconds(air_ts, local_ts),
        raw_line=log.raw_line,
        source_file=log.filename,
    )


@app.get("/now-on-air")
def get_now_on_air():
    """Return the most recent on-air event based on latest ingested log."""
    db = SessionLocal()
    try:
        log: Optional[Log] = db.query(Log).order_by(Log.id.desc()).first()
        if not log:
            raise HTTPException(status_code=404, detail="No events available")
        return _serialize_log(log)
    finally:
        db.close()


@app.get("/events/by-type")
def get_events_by_type(
    type: str = Query(..., description="Event type to filter by, e.g., SONG, SPOT, JINGLE"),
    limit: int = Query(50, ge=1, le=500, description="Max number of items to return"),
    order: str = Query("desc", pattern="^(?i)(asc|desc)$", description="Sort by newest or oldest"),
):
    db = SessionLocal()
    try:
        q = db.query(Log).filter(Log.event_type.ilike(type))
        if order.lower() == "asc":
            q = q.order_by(Log.id.asc())
        else:
            q = q.order_by(Log.id.desc())
        logs: List[Log] = q.limit(limit).all()
        items = [_serialize_log(l) for l in logs]
        return {"items": [item.dict() for item in items], "count": len(items)}
    finally:
        db.close()

