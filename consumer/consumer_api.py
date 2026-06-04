"""
Spotify Stream Consumer — FastAPI Backend
------------------------------------------
Consumes events from Kafka topic and exposes REST API
for top artists, listening hours, and user activity.

Endpoints:
    GET /health
    GET /stats/top-artists
    GET /stats/listening-hours
    GET /stats/activity
    GET /events/recent
    GET /events/stream  (SSE)
"""
import json
import asyncio
import threading
from collections import defaultdict
from datetime import datetime, timezone
from typing import List, Dict

from fastapi import FastAPI, Response
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware   # ← FIX 1: import CORS
from kafka import KafkaConsumer
import uvicorn
import os

app = FastAPI(title="Spotify Real-Time Analytics API", version="1.0.0")

# ── FIX 1: CORS — allows your frontend HTML to call this API ──────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── In-memory aggregations ────────────────────────────────────────────────────
artist_play_count: Dict[str, int] = defaultdict(int)
artist_duration_ms: Dict[str, int] = defaultdict(int)
hourly_activity: Dict[str, int] = defaultdict(int)
recent_events: List[dict] = []
MAX_RECENT = 200
stats_lock = threading.Lock()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC",  "spotify-stream")

# ── Background consumer thread ────────────────────────────────────────────────
def consume_loop():
    """Runs in a background thread, consuming Kafka messages."""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='fastapi-consumer-group',
    )
    print(f"✅ Kafka consumer connected → {KAFKA_BROKER}/{KAFKA_TOPIC}")

    for message in consumer:
        event = message.value

        # ── FIX 2: map CSV column names → frontend field names ────────────────
        # CSV columns:  ts, master_metadata_track_name,
        #               master_metadata_album_artist_name, ms_played, platform
        # Frontend expects: artist_name, track_name, event_time, duration_ms
        artist    = (event.get("master_metadata_album_artist_name")
                     or event.get("artist_name", "Unknown"))
        track     = (event.get("master_metadata_track_name")
                     or event.get("track_name", "Unknown"))
        duration  = int(event.get("ms_played") or event.get("duration_ms") or 0)
        event_time = event.get("ts") or event.get("event_time") or event.get("timestamp")
        platform  = event.get("platform", "web")

        normalized = {
            "artist_name": artist,
            "track_name":  track,
            "duration_ms": duration,
            "event_time":  event_time,
            "platform":    platform,
        }

        with stats_lock:
            artist_play_count[artist] += 1
            artist_duration_ms[artist] += duration

            # hourly bucket
            try:
                dt = datetime.fromisoformat(event_time)
                hour_key = dt.strftime("%Y-%m-%d %H:00")
                hourly_activity[hour_key] += 1
            except Exception:
                pass

            recent_events.append(normalized)   # store normalized event
            if len(recent_events) > MAX_RECENT:
                recent_events.pop(0)


# start consumer thread on startup
@app.on_event("startup")
def startup_event():
    t = threading.Thread(target=consume_loop, daemon=True)
    t.start()


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/stats/top-artists")
def top_artists(limit: int = 10):
    with stats_lock:
        ranked = sorted(artist_play_count.items(), key=lambda x: x[1], reverse=True)
    return {
        "top_artists": [
            {
                "artist":        a,
                "plays":         c,
                "total_minutes": round(artist_duration_ms.get(a, 0) / 60000, 2),
            }
            for a, c in ranked[:limit]
        ]
    }


@app.get("/stats/listening-hours")
def listening_hours():
    with stats_lock:
        total_ms = sum(artist_duration_ms.values())
    return {
        "total_hours":   round(total_ms / 3_600_000, 2),
        "total_minutes": round(total_ms / 60_000, 2),
        "total_events":  sum(artist_play_count.values()),
    }


@app.get("/stats/activity")
def activity():
    with stats_lock:
        data = dict(sorted(hourly_activity.items())[-24:])
    return {"hourly_activity": data}


@app.get("/events/recent")
def recent():
    with stats_lock:
        return {"events": list(reversed(recent_events[-50:]))}


async def event_generator():
    """SSE generator for real-time dashboard."""
    last_count = 0
    while True:
        with stats_lock:
            total = sum(artist_play_count.values())
        if total != last_count:
            last_count = total
            ranked = sorted(
                artist_play_count.items(), key=lambda x: x[1], reverse=True
            )[:5]
            payload = json.dumps({
                "top5":  [{"artist": a, "plays": c} for a, c in ranked],
                "total": total,
            })
            yield f"data: {payload}\n\n"
        await asyncio.sleep(1)


@app.get("/events/stream")
async def stream():
    return StreamingResponse(event_generator(), media_type="text/event-stream")


if __name__ == "__main__":
    uvicorn.run("consumer_api:app", host="0.0.0.0", port=8080, reload=False)