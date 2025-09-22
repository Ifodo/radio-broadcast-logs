import os
import hashlib
from fastapi import FastAPI, HTTPException, Query, UploadFile, File, Request, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, UniqueConstraint
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import text as sql_text
from dotenv import load_dotenv
from datetime import datetime, date, time, timedelta
from typing import Any, List, Optional, Literal
from mutagen.mp3 import MP3
import smtplib
from email.message import EmailMessage
import logging

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
    # New fields for idempotency and accurate ordering
    event_id = Column(String(128), nullable=True, index=True)
    air_timestamp = Column(DateTime, nullable=True, index=True)
    producer_timestamp = Column(DateTime, nullable=True)

# Define AudioFiles table
class AudioFile(Base):
    __tablename__ = "audio_files"
    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String(255), unique=True, index=True, nullable=False)
    upload_time = Column(DateTime, default=datetime.utcnow)
    file_size = Column(Integer, nullable=False)
    uploader_ip = Column(String(64), nullable=True)
    __table_args__ = (UniqueConstraint('filename', name='uq_audio_filename'),)

# Create tables if they don’t exist
Base.metadata.create_all(bind=engine)

# FastAPI app
app = FastAPI(title="Jazler Monitoring API")
app.state.syncing_until = datetime.utcnow() - timedelta(seconds=1)

# SMTP configuration (set these as environment variables or hardcode for testing)
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.example.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "user@example.com")
SMTP_PASS = os.getenv("SMTP_PASS", "password")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER)

# Pydantic model for request
class LogEntry(BaseModel):
    filename: str
    play_time: str
    event_type: str
    artist: str | None = None
    title: str | None = None
    raw_line: str
    local_timestamp: Optional[str] = None
    event_id: Optional[str] = None
    is_backfill: Optional[bool] = None


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


def _ensure_columns_exist():
    """Attempt to add new columns if they don't exist (PostgreSQL)."""
    try:
        with engine.begin() as conn:
            conn.execute(sql_text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name='logs' AND column_name='event_id'
                    ) THEN
                        ALTER TABLE logs ADD COLUMN event_id varchar(128);
                    END IF;
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name='logs' AND column_name='air_timestamp'
                    ) THEN
                        ALTER TABLE logs ADD COLUMN air_timestamp timestamp NULL;
                    END IF;
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name='logs' AND column_name='producer_timestamp'
                    ) THEN
                        ALTER TABLE logs ADD COLUMN producer_timestamp timestamp NULL;
                    END IF;
                END$$;
            """))
    except Exception as e:
        # Non-fatal; table might already be correct or running on an engine without permissions
        print(f"[migrate] Skipped/failed ensuring new columns: {e}")


_ensure_columns_exist()

def _compute_event_id(entry: LogEntry) -> str:
    base = f"{entry.filename}|{entry.play_time}|{entry.event_type}|{entry.artist or ''}|{entry.title or ''}|{entry.raw_line}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


@app.post("/ingest")
def ingest_log(entry: LogEntry):
    db = SessionLocal()
    try:
        # compute helper fields
        event_id = entry.event_id or _compute_event_id(entry)
        air_ts = _parse_air_timestamp(entry.filename, entry.play_time)
        producer_ts = None
        if entry.local_timestamp:
            try:
                producer_ts = datetime.fromisoformat(entry.local_timestamp)
            except Exception:
                producer_ts = None

        # idempotency check
        existing = db.query(Log).filter(Log.event_id == event_id).first()
        if existing:
            return {"status": "success", "id": existing.id, "duplicated": True}

        log = Log(
            filename=entry.filename,
            play_time=entry.play_time,
            event_type=entry.event_type,
            artist=entry.artist,
            title=entry.title,
            raw_line=entry.raw_line,
            event_id=event_id,
            air_timestamp=air_ts,
            producer_timestamp=producer_ts,
        )
        db.add(log)
        db.commit()
        db.refresh(log)
        # mark syncing window if backfill
        if entry.is_backfill:
            app.state.syncing_until = datetime.utcnow() + timedelta(seconds=20)
        return {"status": "success", "id": log.id, "duplicated": False}
    except Exception as e:
        db.rollback()
        return {"status": "error", "detail": str(e)}
    finally:
        db.close()


class IngestBulkRequest(BaseModel):
    items: List[LogEntry]
    is_backfill: Optional[bool] = None


@app.post("/ingest/bulk")
def ingest_bulk(req: IngestBulkRequest):
    db = SessionLocal()
    inserted = 0
    duplicates = 0
    try:
        for item in req.items:
            try:
                event_id = item.event_id or _compute_event_id(item)
                exists = db.query(Log).filter(Log.event_id == event_id).first()
                if exists:
                    duplicates += 1
                    continue
                air_ts = _parse_air_timestamp(item.filename, item.play_time)
                producer_ts = None
                if item.local_timestamp:
                    try:
                        producer_ts = datetime.fromisoformat(item.local_timestamp)
                    except Exception:
                        producer_ts = None
                row = Log(
                    filename=item.filename,
                    play_time=item.play_time,
                    event_type=item.event_type,
                    artist=item.artist,
                    title=item.title,
                    raw_line=item.raw_line,
                    event_id=event_id,
                    air_timestamp=air_ts,
                    producer_timestamp=producer_ts,
                )
                db.add(row)
                inserted += 1
            except Exception:
                db.rollback()
                db.begin()
                continue
        db.commit()
        # update syncing indicator
        if req.is_backfill or any(getattr(i, "is_backfill", False) for i in req.items):
            app.state.syncing_until = datetime.utcnow() + timedelta(seconds=20)
        return {"status": "success", "inserted": inserted, "duplicates": duplicates}
    except Exception as e:
        db.rollback()
        return {"status": "error", "detail": str(e)}
    finally:
        db.close()

@app.post("/audio_logs")
def upload_audio_log(request: Request, file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".mp3"):
        raise HTTPException(status_code=400, detail="Only .mp3 files are allowed.")
    db = SessionLocal()
    try:
        # Check for duplicate filename in DB
        existing = db.query(AudioFile).filter(AudioFile.filename == file.filename).first()
        if existing or os.path.exists(os.path.join("audio_files", file.filename)):
            raise HTTPException(status_code=409, detail="File with this name already exists.")
        # Save file to disk
        dest_path = os.path.join("audio_files", file.filename)
        with open(dest_path, "wb") as out_file:
            content = file.file.read()
            out_file.write(content)
        file_size = os.path.getsize(dest_path)
        uploader_ip = request.client.host if request.client else None
        # Save metadata to DB
        audio_entry = AudioFile(
            filename=file.filename,
            upload_time=datetime.utcnow(),
            file_size=file_size,
            uploader_ip=uploader_ip,
        )
        db.add(audio_entry)
        db.commit()
        db.refresh(audio_entry)
        return {
            "status": "success",
            "filename": audio_entry.filename,
            "upload_time": audio_entry.upload_time.isoformat(),
            "file_size": audio_entry.file_size,
            "uploader_ip": audio_entry.uploader_ip,
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
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
    air_ts = log.air_timestamp or (_parse_air_timestamp(log.filename or "", log.play_time or "") if (log.filename and log.play_time) else None)
    local_ts = log.producer_timestamp or log.timestamp
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
        # Prefer a LIVE candidate: recently produced (producer_timestamp within last 5 minutes)
        live_window_start = datetime.utcnow() - timedelta(minutes=5)
        log: Optional[Log] = (
            db.query(Log)
            .filter(Log.producer_timestamp.isnot(None))
            .filter(Log.producer_timestamp >= live_window_start)
            .order_by(Log.air_timestamp.desc().nullslast(), Log.id.desc())
            .first()
        )
        if not log:
            # Fallback: latest by air time overall
            log = (
                db.query(Log)
                .filter(Log.air_timestamp.isnot(None))
                .order_by(Log.air_timestamp.desc())
                .first()
            )
        if not log:
            log = db.query(Log).order_by(Log.id.desc()).first()
        if not log:
            raise HTTPException(status_code=404, detail="No events available")
        item = _serialize_log(log)
        syncing = datetime.utcnow() < getattr(app.state, "syncing_until", datetime.utcnow() - timedelta(seconds=1))
        data = item.dict()
        data["syncing"] = syncing
        return data
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
            q = q.order_by(Log.air_timestamp.asc().nullslast(), Log.id.asc())
        else:
            q = q.order_by(Log.air_timestamp.desc().nullslast(), Log.id.desc())
        logs: List[Log] = q.limit(limit).all()
        items = [_serialize_log(l) for l in logs]
        return {"items": [item.dict() for item in items], "count": len(items)}
    finally:
        db.close()


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


@app.get("/stats/spots/titles")
def stats_spot_titles(
    filename: str = Query(..., description="Daily log filename, e.g., 2025-09-16.txt"),
    limit: int = Query(50, ge=1, le=500),
    title: Optional[str] = Query(None, description="Optional case-insensitive substring filter on title"),
):
    db = SessionLocal()
    try:
        params = {
            "filename": filename,
            "q": (title.lower().strip() if title else None),
            "limit": limit,
        }

        # Main aggregation by normalized title within a single filename (day)
        sql = sql_text(
            """
            WITH base AS (
                SELECT
                    lower(trim(title)) AS group_title,
                    COALESCE(air_timestamp, timestamp) AS sort_ts,
                    play_time
                FROM logs
                WHERE event_type ILIKE 'SPOT'
                  AND title IS NOT NULL
                  AND filename = :filename
                  AND (:q IS NULL OR lower(title) LIKE '%' || :q || '%')
            )
            SELECT group_title AS title,
                   COUNT(*) AS count,
                   MIN(sort_ts) AS first_air_timestamp,
                   MAX(sort_ts) AS last_air_timestamp
            FROM base
            GROUP BY group_title
            ORDER BY COUNT(*) DESC, MAX(sort_ts) DESC
            LIMIT :limit
            """
        )

        rows = db.execute(sql, params).fetchall()

        results = []
        for r in rows:
            title_group = r[0]
            count = int(r[1])
            first_ts = r[2]
            last_ts = r[3]

            item = {
                "title": title_group,
                "count": count,
                "first_air_timestamp": first_ts.isoformat() if first_ts else None,
                "last_air_timestamp": last_ts.isoformat() if last_ts else None,
            }

            # Include all air times for this filename (bounded by file scope)
            times_sql = sql_text(
                """
                SELECT COALESCE(air_timestamp, timestamp) AS ts, play_time
                FROM logs
                WHERE event_type ILIKE 'SPOT'
                  AND title IS NOT NULL
                  AND filename = :filename
                  AND lower(trim(title)) = :group_title
                ORDER BY ts DESC
                """
            )
            times_rows = db.execute(
                times_sql,
                {
                    "filename": filename,
                    "group_title": title_group,
                },
            ).fetchall()
            item["air_times"] = [tr[0].isoformat() for tr in times_rows if tr[0] is not None]
            item["play_times"] = [tr[1] for tr in times_rows if tr[1] is not None]

            results.append(item)

        return {"items": results, "count": len(results)}
    finally:
        db.close()


@app.get("/stats/spots/all")
def stats_spots_all(
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    limit: int = Query(50, ge=1, le=500, description="Number of titles per page"),
):
    """Aggregate all SPOT titles across the database with pagination.
    Groups by normalized title and returns per-filename breakdown for each title.
    """
    db = SessionLocal()
    try:
        offset = (page - 1) * limit

        total_sql = sql_text(
            """
            SELECT COUNT(*) FROM (
                SELECT 1
                FROM logs
                WHERE event_type ILIKE 'SPOT' AND title IS NOT NULL
                GROUP BY lower(trim(title))
            ) t
            """
        )
        total_titles = db.execute(total_sql).scalar() or 0

        main_sql = sql_text(
            """
            WITH base AS (
                SELECT lower(trim(title)) AS group_title,
                       COALESCE(air_timestamp, timestamp) AS ts
                FROM logs
                WHERE event_type ILIKE 'SPOT' AND title IS NOT NULL
            ),
            title_agg AS (
                SELECT group_title,
                       COUNT(*) AS total_count,
                       MIN(ts) AS first_ts,
                       MAX(ts) AS last_ts
                FROM base
                GROUP BY group_title
            ),
            paged AS (
                SELECT * FROM title_agg
                ORDER BY total_count DESC, last_ts DESC
                LIMIT :limit OFFSET :offset
            )
            SELECT group_title, total_count, first_ts, last_ts
            FROM paged
            """
        )
        rows = db.execute(main_sql, {"limit": limit, "offset": offset}).fetchall()

        items = []
        for r in rows:
            group_title = r[0]
            total_count = int(r[1])
            first_ts = r[2]
            last_ts = r[3]

            files_sql = sql_text(
                """
                SELECT filename,
                       COUNT(*) AS count,
                       MIN(COALESCE(air_timestamp, timestamp)) AS first_ts,
                       MAX(COALESCE(air_timestamp, timestamp)) AS last_ts
                FROM logs
                WHERE event_type ILIKE 'SPOT'
                  AND title IS NOT NULL
                  AND lower(trim(title)) = :group_title
                GROUP BY filename
                ORDER BY last_ts DESC
                """
            )
            file_rows = db.execute(files_sql, {"group_title": group_title}).fetchall()
            files = [
                {
                    "filename": fr[0],
                    "count": int(fr[1]),
                    "first_air_timestamp": (fr[2].isoformat() if fr[2] else None),
                    "last_air_timestamp": (fr[3].isoformat() if fr[3] else None),
                }
                for fr in file_rows
            ]

            items.append(
                {
                    "title": group_title,
                    "total_count": total_count,
                    "first_air_timestamp": first_ts.isoformat() if first_ts else None,
                    "last_air_timestamp": last_ts.isoformat() if last_ts else None,
                    "files": files,
                }
            )

        has_next = (offset + len(items)) < total_titles
        return {
            "items": items,
            "page": {"page": page, "limit": limit, "total_titles": total_titles, "hasNext": has_next},
        }
    finally:
        db.close()


        # -------------------- file analysis --------------------

from sqlalchemy import JSON  # add JSON type for analysis result storage

# ---------- New Model for Analysis Jobs ----------
class AnalysisJob(Base):
    __tablename__ = "analysis_jobs"
    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String(255), nullable=False, index=True)
    spot_name = Column(String(255), nullable=False)
    status = Column(String(50), default="queued", nullable=False)  # queued, processing, completed, failed, cancelled
    progress = Column(Integer, nullable=True)
    result = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    match_type = Column(String(32), default="substring")
    cancelled = Column(Integer, default=0)  # 0 = not cancelled, 1 = cancelled


# Create the new table if not exists
Base.metadata.create_all(bind=engine)


# ---------- Helper for Analysis ----------
def _run_analysis(job_id: int):
    db = SessionLocal()
    try:
        job = db.query(AnalysisJob).filter(AnalysisJob.id == job_id).first()
        if not job or job.cancelled:
            logger.info(f"Job {job_id} cancelled or not found before start.")
            return
        try:
            logger.info(f"Job {job_id} started: {job.filename} / {job.spot_name}")
            job.status = "processing"
            job.progress = 10
            db.commit()

            # Audio content analysis (already present)
            audio_info = {}
            audio_file_entry = db.query(AudioFile).filter(AudioFile.filename == job.filename).first()
            if audio_file_entry:
                audio_path = os.path.join("audio_files", audio_file_entry.filename)
                try:
                    from mutagen.mp3 import MP3
                    audio = MP3(audio_path)
                    audio_info = {
                        "duration_seconds": round(audio.info.length, 2) if hasattr(audio.info, 'length') else None,
                        "bitrate": audio.info.bitrate if hasattr(audio.info, 'bitrate') else None,
                        "mode": getattr(audio.info, 'mode', None),
                        "sample_rate": getattr(audio.info, 'sample_rate', None),
                        "channels": getattr(audio.info, 'channels', None),
                    }
                except Exception as e:
                    audio_info = {"error": f"Audio analysis failed: {e}"}
                    logger.error(f"Audio analysis failed for job {job_id}: {e}")

            # Spot name matching
            match_type = getattr(job, 'match_type', 'substring')
            spot_name = job.spot_name
            query = db.query(Log).filter(Log.filename == job.filename).filter(Log.event_type.ilike("SPOT"))
            if match_type == 'exact':
                query = query.filter(Log.title == spot_name)
            elif match_type == 'regex':
                query = query.filter(Log.title.op('~*')(spot_name))
            else:
                query = query.filter(Log.title.ilike(f"%{spot_name}%"))
            rows = query.order_by(Log.air_timestamp.asc().nullslast()).all()

            # Check for cancellation during processing
            job = db.query(AnalysisJob).filter(AnalysisJob.id == job_id).first()
            if job and job.cancelled:
                job.status = "cancelled"
                job.progress = None
                db.commit()
                logger.info(f"Job {job_id} cancelled during processing.")
                return

            results = []
            for row in rows:
                results.append({
                    "id": row.id,
                    "title": row.title,
                    "artist": row.artist,
                    "play_time": row.play_time,
                    "air_timestamp": row.air_timestamp.isoformat() if row.air_timestamp else None,
                    "producer_timestamp": row.producer_timestamp.isoformat() if row.producer_timestamp else None,
                    "source_file": row.filename,
                })

            job.progress = 90
            job.result = {
                "spot_name": job.spot_name,
                "filename": job.filename,
                "audio_info": audio_info,
                "matches": results,
                "count": len(results),
                "match_type": match_type,
            }
            job.status = "completed"
            job.progress = 100
            db.commit()
            logger.info(f"Job {job_id} completed: {len(results)} matches.")
        except Exception as e:
            job.status = "failed"
            job.result = {"error": str(e)}
            db.commit()
            logger.error(f"Job {job_id} failed: {e}")
    finally:
        db.close()


# ---------- API Endpoints ----------
class AnalysisRequest(BaseModel):
    filename: str
    spot_name: str
    match_type: Optional[Literal['exact', 'substring', 'regex']] = 'substring'


@app.post("/analyze/request")
def request_analysis(req: AnalysisRequest, background_tasks: BackgroundTasks):
    db = SessionLocal()
    try:
        # Ensure the file exists in audio_files
        file_entry = db.query(AudioFile).filter(AudioFile.filename == req.filename).first()
        if not file_entry:
            logger.warning(f"Analysis request for missing file: {req.filename}")
            raise HTTPException(status_code=404, detail="Audio file not found")

        job = AnalysisJob(filename=req.filename, spot_name=req.spot_name, status="queued", progress=0)
        # Store match_type in job if present
        if hasattr(req, 'match_type') and req.match_type:
            setattr(job, 'match_type', req.match_type)
        db.add(job)
        db.commit()
        db.refresh(job)
        logger.info(f"Job {job.id} created: {req.filename} / {req.spot_name}")

        # Enqueue background analysis
        background_tasks.add_task(_run_analysis, job.id)

        return {"status": "queued", "job_id": job.id}
    finally:
        db.close()


@app.get("/analyze/status/{job_id}")
def get_analysis_status(job_id: int):
    db = SessionLocal()
    try:
        job = db.query(AnalysisJob).filter(AnalysisJob.id == job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return {
            "job_id": job.id,
            "status": job.status,
            "progress": job.progress,
            "updated_at": job.updated_at.isoformat() if job.updated_at else None,
        }
    finally:
        db.close()


@app.get("/analyze/report/{job_id}")
def get_analysis_report(job_id: int):
    db = SessionLocal()
    try:
        job = db.query(AnalysisJob).filter(AnalysisJob.id == job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        if job.status != "completed":
            return {"status": job.status, "message": "Report not ready"}
        return {"status": "success", "report": job.result}
    finally:
        db.close()


class SendReportRequest(BaseModel):
    email: str


@app.post("/analyze/send-report/{job_id}")
def send_analysis_report(job_id: int, req: SendReportRequest):
    db = SessionLocal()
    try:
        job = db.query(AnalysisJob).filter(AnalysisJob.id == job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        if job.status != "completed":
            raise HTTPException(status_code=400, detail="Report not ready")

        # Prepare email
        msg = EmailMessage()
        msg["Subject"] = f"Analysis Report for {job.spot_name} in {job.filename}"
        msg["From"] = SMTP_FROM
        msg["To"] = req.email
        report = job.result
        body = f"Spot Name: {report.get('spot_name')}\nFilename: {report.get('filename')}\nCount: {report.get('count')}\nMatch Type: {report.get('match_type')}\n\nAudio Info: {report.get('audio_info')}\n\nMatches:\n"
        for match in report.get("matches", []):
            body += f"- {match}\n"
        msg.set_content(body)

        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASS)
                server.send_message(msg)
            return {
                "status": "success",
                "message": f"Report for job {job.id} sent to {req.email}",
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to send email: {e}",
            }
    finally:
        db.close()


@app.post("/analyze/cancel/{job_id}")
def cancel_analysis_job(job_id: int):
    db = SessionLocal()
    try:
        job = db.query(AnalysisJob).filter(AnalysisJob.id == job_id).first()
        if not job:
            logger.warning(f"Cancel request for missing job: {job_id}")
            raise HTTPException(status_code=404, detail="Job not found")
        if job.status in ("completed", "failed", "cancelled"):
            logger.info(f"Cancel request for finished job: {job_id} (status: {job.status})")
            return {"status": job.status, "message": "Job already finished or cancelled"}
        job.cancelled = 1
        job.status = "cancelled"
        db.commit()
        logger.info(f"Job {job_id} cancelled by user.")
        return {"status": "cancelled", "job_id": job.id}
    finally:
        db.close()

@app.post("/analyze/retry/{job_id}")
def retry_analysis_job(job_id: int, background_tasks: BackgroundTasks):
    db = SessionLocal()
    try:
        job = db.query(AnalysisJob).filter(AnalysisJob.id == job_id).first()
        if not job:
            logger.warning(f"Retry request for missing job: {job_id}")
            raise HTTPException(status_code=404, detail="Job not found")
        if job.status not in ("failed", "cancelled"):
            logger.info(f"Retry request for job {job_id} not in failed/cancelled state (status: {job.status})")
            return {"status": job.status, "message": "Job is not failed or cancelled, cannot retry"}
        # Reset job state
        job.status = "queued"
        job.progress = 0
        job.result = None
        job.cancelled = 0
        db.commit()
        logger.info(f"Job {job_id} retried by user.")
        # Enqueue background analysis
        background_tasks.add_task(_run_analysis, job.id)
        return {"status": "queued", "job_id": job.id}
    finally:
        db.close()


@app.middleware("http")
async def log_requests(request, call_next):
    logger.info(f"Request: {request.method} {request.url}")
    response = await call_next(request)
    logger.info(f"Response: {request.method} {request.url} {response.status_code}")
    return response


# Setup logging (ensure this is before any function that uses logger)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    handlers=[
        logging.FileHandler("server.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("serverpg")

