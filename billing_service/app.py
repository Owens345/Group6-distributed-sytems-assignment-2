import os
import time
import random
from fastapi import FastAPI
from fastapi.responses import JSONResponse, HTMLResponse
import redis
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from common.config import REDIS_URL, HMAC_SECRET
from common.security import verify_payload
from common.models import SignedRequest, TxPrepareRequest
from common.db import ensure_db, connect, exec_one, fetch_one
from common.util import now
from common.metrics import REQ_LATENCY, REQ_COUNT, EVENT_COUNT

app = FastAPI(title="Billing Service (Participant)", version="1.0")
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "billing.sqlite")
ensure_db(DB_PATH)
conn = connect(DB_PATH)
conn.execute("""
    CREATE TABLE IF NOT EXISTS tx_state (
      tx_id TEXT PRIMARY KEY,
      state TEXT NOT NULL,
      updated_at REAL NOT NULL
    );
""")
conn.execute("""
    CREATE TABLE IF NOT EXISTS cdr (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tx_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      subscriber_id TEXT NOT NULL,
      created_at REAL NOT NULL
    );
""")
conn.commit()

FAULTS = {"slow_ms": 0, "fail_prepare_pct": 0}
STREAM = "telecom_events"

def verify_or_401(sr: SignedRequest):
    if not verify_payload(sr.payload, sr.signature, HMAC_SECRET):
        return JSONResponse({"error": "invalid signature"}, status_code=401)
    return None

def maybe_sleep():
    ms = int(FAULTS.get("slow_ms", 0))
    if ms > 0:
        time.sleep(ms / 1000.0)

def maybe_fail_prepare() -> bool:
    pct = float(FAULTS.get("fail_prepare_pct", 0))
    return random.random() < (pct / 100.0)

def publish_event(event_type: str, data: dict):
    payload = {"type": event_type, "ts": now(), "data": data}
    r.xadd(STREAM, {"session_id": data.get("session_id",""), "payload": str(payload)})
    EVENT_COUNT.labels("billing_service", STREAM, "produce").inc()

@app.get("/status")
async def status():
    return {"service": "billing_service", "faults": FAULTS}

@app.post("/admin/faults")
async def set_faults(sr: SignedRequest):
    bad = verify_or_401(sr)
    if bad: return bad
    FAULTS.update(sr.payload)
    return {"ok": True, "faults": FAULTS}

@app.post("/tx/prepare")
async def prepare(sr: SignedRequest):
    t0 = time.time()
    status_code = "200"
    try:
        bad = verify_or_401(sr)
        if bad:
            status_code = "401"
            return bad
        maybe_sleep()
        if maybe_fail_prepare():
            status_code = "409"
            return JSONResponse({"ok": False, "reason": "injected prepare failure"}, status_code=409)
        req = TxPrepareRequest(**sr.payload)
        exec_one(conn, "INSERT OR REPLACE INTO tx_state (tx_id, state, updated_at) VALUES (?,?,?)",
                 (req.tx_id, "PREPARED", now()))
        return {"ok": True}
    finally:
        dt = time.time() - t0
        REQ_LATENCY.labels("billing_service", "/tx/prepare", "POST", status_code).observe(dt)
        REQ_COUNT.labels("billing_service", "/tx/prepare", "POST", status_code).inc()

@app.post("/tx/precommit")
async def precommit(sr: SignedRequest):
    bad = verify_or_401(sr)
    if bad: return bad
    maybe_sleep()
    tx_id = sr.payload.get("tx_id", "")
    row = fetch_one(conn, "SELECT state FROM tx_state WHERE tx_id=?", (tx_id,))
    if row is None or row["state"] != "PREPARED":
        return JSONResponse({"ok": False, "reason": "not prepared"}, status_code=409)
    exec_one(conn, "UPDATE tx_state SET state=?, updated_at=? WHERE tx_id=?", ("PRECOMMITTED", now(), tx_id))
    return {"ok": True}

@app.post("/tx/commit")
async def commit(sr: SignedRequest):
    bad = verify_or_401(sr)
    if bad: return bad
    maybe_sleep()
    tx_id = sr.payload.get("tx_id", "")
    # For demo, create a CDR on commit
    row = fetch_one(conn, "SELECT state FROM tx_state WHERE tx_id=?", (tx_id,))
    exec_one(conn, "INSERT OR REPLACE INTO tx_state (tx_id, state, updated_at) VALUES (?,?,?)",
             (tx_id, "COMMITTED", now()))
    publish_event("cdr_committed", {"tx_id": tx_id})
    return {"ok": True}

@app.post("/tx/abort")
async def abort(sr: SignedRequest):
    bad = verify_or_401(sr)
    if bad: return bad
    maybe_sleep()
    tx_id = sr.payload.get("tx_id", "")
    exec_one(conn, "INSERT OR REPLACE INTO tx_state (tx_id, state, updated_at) VALUES (?,?,?)",
             (tx_id, "ABORTED", now()))
    publish_event("cdr_aborted", {"tx_id": tx_id})
    return {"ok": True}

@app.get("/metrics")
async def metrics():
    return HTMLResponse(generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST)
