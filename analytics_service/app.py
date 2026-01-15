import os
import asyncio
import time
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import redis
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from common.config import REDIS_URL
from common.metrics import EVENT_COUNT

app = FastAPI(title="Analytics Service (Cloud)", version="1.0")
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

STREAM = "telecom_events"
GROUP = "analytics_group"
CONSUMER = os.getenv("CONSUMER_ID", "c1")

STATE = {
    "events_seen": 0,
    "session_committed": 0,
    "session_aborted": 0,
    "cdr_committed": 0,
    "cdr_aborted": 0,
    "last_id": "0-0",
}

def ensure_group():
    try:
        r.xgroup_create(STREAM, GROUP, id="0-0", mkstream=True)
    except Exception:
        pass

async def consumer_loop():
    ensure_group()
    while True:
        try:
            msgs = r.xreadgroup(GROUP, CONSUMER, {STREAM: ">"}, count=50, block=1000)
            if not msgs:
                await asyncio.sleep(0.1)
                continue
            for _, entries in msgs:
                for msg_id, fields in entries:
                    payload = fields.get("payload", "")
                    STATE["events_seen"] += 1
                    STATE["last_id"] = msg_id
                    if "session_committed" in payload:
                        STATE["session_committed"] += 1
                    if "session_aborted" in payload:
                        STATE["session_aborted"] += 1
                    if "cdr_committed" in payload:
                        STATE["cdr_committed"] += 1
                    if "cdr_aborted" in payload:
                        STATE["cdr_aborted"] += 1
                    EVENT_COUNT.labels("analytics_service", STREAM, "consume").inc()
                    r.xack(STREAM, GROUP, msg_id)
        except Exception:
            await asyncio.sleep(0.5)

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(consumer_loop())

@app.get("/status")
async def status():
    return {"service": "analytics_service", **STATE}

@app.get("/metrics")
async def metrics():
    return HTMLResponse(generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST)
