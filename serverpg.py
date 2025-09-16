import os
from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
from datetime import datetime

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

