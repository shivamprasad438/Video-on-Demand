import os, io, time, uuid
from typing import List
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import boto3
from botocore.client import Config
from sqlalchemy import create_engine, text

app = FastAPI(title="VOD API")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin12345")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "vod")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://voduser:vodpass@db:5432/voddb")

s3 = boto3.client(
    "s3",
    endpoint_url=f"http://{MINIO_ENDPOINT}",
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

engine = create_engine(DATABASE_URL, future=True)

def init_db():
    with engine.begin() as con:
        con.execute(text("""
        CREATE TABLE IF NOT EXISTS videos(
          id UUID PRIMARY KEY,
          title TEXT,
          source_key TEXT,
          status TEXT,
          created_at TIMESTAMP DEFAULT NOW()
        );
        """))

init_db()

class VideoOut(BaseModel):
    id: str
    title: str
    status: str
    hls_url: str | None = None

@app.post("/upload", response_model=VideoOut)
async def upload_video(file: UploadFile = File(...)):
    if not file.filename.lower().endswith((".mp4", ".mov", ".mkv")):
        raise HTTPException(400, "Only video files allowed")

    vid = str(uuid.uuid4())
    src_key = f"source/{vid}/{file.filename}"

    # upload to MinIO
    data = await file.read()
    s3.put_object(Bucket=MINIO_BUCKET, Key=src_key, Body=data, ContentType=file.content_type)

    # db record
    with engine.begin() as con:
        con.execute(text("INSERT INTO videos(id,title,source_key,status) VALUES(:i,:t,:k,'queued')"),
                    {"i": vid, "t": file.filename, "k": src_key})

    # enqueue job as simple flag object in S3
    s3.put_object(Bucket=MINIO_BUCKET, Key=f"jobs/{vid}.json", Body=b"{}")

    return VideoOut(id=vid, title=file.filename, status="queued")

@app.get("/videos", response_model=List[VideoOut])
def list_videos():
    with engine.connect() as con:
        rows = con.execute(text("SELECT id,title,status FROM videos ORDER BY created_at DESC")).all()
    out = []
    for r in rows:
        hls_key = f"transcoded/{r.id}/index.m3u8"
        # check if transcoded
        hls_url = None
        try:
            s3.head_object(Bucket=MINIO_BUCKET, Key=hls_key)
            # cdn reverse proxies /hls to minio
            hls_url = f"http://localhost:{os.getenv('CDN_PORT','8081')}/hls/{r.id}/index.m3u8"
        except Exception:
            pass
        out.append(VideoOut(id=str(r.id), title=r.title, status=r.status, hls_url=hls_url))
    return out

@app.get("/health")
def health():
    return {"ok": True, "time": int(time.time())}
