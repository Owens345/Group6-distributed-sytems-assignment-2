import os
import asyncio
import time
from typing import Dict, Any, Optional, Tuple
from fastapi import FastAPI
from fastapi.responses import JSONResponse, HTMLResponse
import httpx
import redis
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from common.config import REDIS_URL, HMAC_SECRET, RESOURCE_MANAGER_URL, BILLING_SERVICE_URL, SERVICE_NAME
from common.security import verify_payload, sign_payload
from common.models import SignedRequest, SessionCreateRequest, SessionRecord
from common.dsm import DSM
from common.db import ensure_db, connect, exec_one, fetch_one
from common.util import gen_id, now
from common.metrics import REQ_LATENCY, REQ_COUNT, TX_COUNT, LEADER_GAUGE, EVENT_COUNT, QUEUE_DELAY, DEADLOCKS
from common.scheduler import PriorityScheduler

INSTANCE_ID = os.getenv("INSTANCE_ID", gen_id("sm"))

app = FastAPI(title="Session Manager (Core)", version="1.0")

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
dsm = DSM(REDIS_URL, namespace="core_sessions")

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "session_manager.sqlite")
ensure_db(DB_PATH)
conn = connect(DB_PATH)
conn.execute("""
    CREATE TABLE IF NOT EXISTS tx_log (
      tx_id TEXT PRIMARY KEY,
      protocol TEXT NOT NULL,
      state TEXT NOT NULL,
      decision TEXT,
      created_at REAL NOT NULL,
      updated_at REAL NOT NULL
    );
""")
conn.execute("""
    CREATE TABLE IF NOT EXISTS sessions (
      session_id TEXT PRIMARY KEY,
      subscriber_id TEXT NOT NULL,
      status TEXT NOT NULL,
      resources INTEGER NOT NULL,
      created_at REAL NOT NULL,
      version INTEGER NOT NULL
    );
""")
conn.commit()

# Scheduling knobs
SCHEDULING = {
    "enabled": True,
    "voice_priority": 0,
    "data_priority": 5
}
SCHEDULER = PriorityScheduler()

# Demo fault knobs
FAULTS = {"slow_ms": 0, "random_abort_pct": 0}

STREAM = "telecom_events"

def is_leader() -> bool:
    # Redis based leader election using SET NX with TTL.
    key = "leader:session_manager"
    ttl_ms = 4000
    try:
        ok = r.set(key, INSTANCE_ID, nx=True, px=ttl_ms)
        if ok:
            return True
        cur = r.get(key)
        return cur == INSTANCE_ID
    except Exception:
        return False

async def leader_heartbeat():
    key = "leader:session_manager"
    ttl_ms = 4000
    while True:
        lead = is_leader()
        LEADER_GAUGE.labels("session_manager", INSTANCE_ID).set(1 if lead else 0)
        if lead:
            try:
                r.pexpire(key, ttl_ms)
            except Exception:
                pass
        await asyncio.sleep(1.0)

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(leader_heartbeat())

    async def worker(payload: dict):
        return await _start_session_internal(payload)

    await SCHEDULER.start(worker)

def verify_or_401(sr: SignedRequest) -> Optional[JSONResponse]:
    if not verify_payload(sr.payload, sr.signature, HMAC_SECRET):
        return JSONResponse({"error": "invalid signature"}, status_code=401)
    return None

async def participant_call(url: str, endpoint: str, payload: Dict[str, Any], timeout_s: float = 3.0) -> Tuple[int, Dict[str, Any]]:
    signature = sign_payload(payload, HMAC_SECRET)
    body = {"payload": payload, "signature": signature}
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        r = await client.post(f"{url}{endpoint}", json=body)
        return r.status_code, r.json()

def maybe_sleep():
    ms = int(FAULTS.get("slow_ms", 0))
    if ms > 0:
        time.sleep(ms / 1000.0)

def maybe_random_abort() -> bool:
    import random
    pct = float(FAULTS.get("random_abort_pct", 0))
    return random.random() < (pct / 100.0)

def log_tx(tx_id: str, protocol: str, state: str, decision: Optional[str] = None):
    t = now()
    row = fetch_one(conn, "SELECT tx_id FROM tx_log WHERE tx_id=?", (tx_id,))
    if row is None:
        exec_one(conn, "INSERT INTO tx_log (tx_id, protocol, state, decision, created_at, updated_at) VALUES (?,?,?,?,?,?)",
                 (tx_id, protocol, state, decision, t, t))
    else:
        exec_one(conn, "UPDATE tx_log SET state=?, decision=?, updated_at=? WHERE tx_id=?",
                 (state, decision, t, tx_id))

def upsert_session(session: SessionRecord):
    row = fetch_one(conn, "SELECT session_id FROM sessions WHERE session_id=?", (session.session_id,))
    if row is None:
        exec_one(conn, "INSERT INTO sessions (session_id, subscriber_id, status, resources, created_at, version) VALUES (?,?,?,?,?,?)",
                 (session.session_id, session.subscriber_id, session.status, session.resources, session.created_at, session.version))
    else:
        exec_one(conn, "UPDATE sessions SET status=?, version=? WHERE session_id=?",
                 (session.status, session.version, session.session_id))

def publish_event(event_type: str, data: Dict[str, Any]):
    payload = {"type": event_type, "ts": now(), "data": data}
    # Ordered per session_id by using it as stream field and group key later.
    r.xadd(STREAM, {"session_id": data.get("session_id",""), "payload": str(payload)})
    EVENT_COUNT.labels("session_manager", STREAM, "produce").inc()

@app.get("/status")
async def status():
    return {"service": "session_manager", "instance_id": INSTANCE_ID, "leader": is_leader(), "faults": FAULTS}

@app.post("/admin/scheduler")
async def set_scheduler(sr: SignedRequest):
    bad = verify_or_401(sr)
    if bad: return bad
    SCHEDULING.update(sr.payload)
    return {"ok": True, "scheduling": SCHEDULING}

@app.post("/admin/faults")

async def set_faults(sr: SignedRequest):
    bad = verify_or_401(sr)
    if bad: return bad
    FAULTS.update(sr.payload)
    return {"ok": True, "faults": FAULTS}

@app.post("/sessions/start")
async def start_session(sr: SignedRequest):
    t0 = time.time()
    status_code = "200"
    try:
        bad = verify_or_401(sr)
        if bad:
            status_code = "401"
            return bad

        maybe_sleep()
        if not is_leader():
            status_code = "409"
            return JSONResponse({"error": "not leader", "hint": "retry", "instance_id": INSTANCE_ID}, status_code=409)

        req = SessionCreateRequest(**sr.payload)
        if maybe_random_abort():
            status_code = "503"
            return JSONResponse({"error": "random abort injected"}, status_code=503)

        session_id = gen_id("sess")
        tx_id = gen_id("tx")
        protocol = req.protocol

        # DSM write session in pending state
        session_key = f"session:{session_id}"
        ok, ver = dsm.write(session_key, {
            "session_id": session_id,
            "subscriber_id": req.subscriber_id,
            "status": "PENDING",
            "created_at": now(),
            "resources": req.resources
        }, expected_version=None)
        if not ok:
            status_code = "500"
            return JSONResponse({"error": "dsm write failed"}, status_code=500)

        # Persist minimal record for reporting and recovery
        session = SessionRecord(
            session_id=session_id,
            subscriber_id=req.subscriber_id,
            status="PENDING",
            created_at=now(),
            resources=req.resources,
            version=ver
        )
        upsert_session(session)

        if protocol == "2pc":
            decision = await run_2pc(tx_id, session_id, req.subscriber_id, req.resources)
        else:
            decision = await run_3pc(tx_id, session_id, req.subscriber_id, req.resources)

        if decision == "commit":
            # Update DSM and DB
            cur = dsm.read(session_key)
            ok2, ver2 = dsm.write(session_key, {**cur.value, "status": "ACTIVE"}, expected_version=cur.version if cur else ver)
            session.status = "ACTIVE"
            session.version = ver2 if ok2 else session.version
            upsert_session(session)
            publish_event("session_committed", {"session_id": session_id, "subscriber_id": req.subscriber_id, "protocol": protocol})
            TX_COUNT.labels("session_manager", protocol, "commit").inc()
            return {"session_id": session_id, "tx_id": tx_id, "status": "ACTIVE", "protocol": protocol}
        else:
            cur = dsm.read(session_key)
            if cur:
                dsm.write(session_key, {**cur.value, "status": "ABORTED"}, expected_version=cur.version)
            session.status = "ABORTED"
            upsert_session(session)
            publish_event("session_aborted", {"session_id": session_id, "subscriber_id": req.subscriber_id, "protocol": protocol})
            TX_COUNT.labels("session_manager", protocol, "abort").inc()
            status_code = "409"
            return JSONResponse({"error": "transaction aborted", "tx_id": tx_id, "session_id": session_id, "protocol": protocol}, status_code=409)
    except Exception as e:
        status_code = "500"
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        dt = time.time() - t0
        REQ_LATENCY.labels("session_manager", "/sessions/start", "POST", status_code).observe(dt)
        REQ_COUNT.labels("session_manager", "/sessions/start", "POST", status_code).inc()

async def run_2pc(tx_id: str, session_id: str, subscriber_id: str, resources: int) -> str:
    log_tx(tx_id, "2pc", "PREPARE")
    prepare = {"tx_id": tx_id, "session_id": session_id, "subscriber_id": subscriber_id, "resources": resources}

    st1, r1 = await participant_call(RESOURCE_MANAGER_URL, "/tx/prepare", prepare, timeout_s=3.0)
    st2, r2 = await participant_call(BILLING_SERVICE_URL, "/tx/prepare", prepare, timeout_s=3.0)

    if st1 != 200 or st2 != 200 or (not r1.get("ok", False)) or (not r2.get("ok", False)):
        log_tx(tx_id, "2pc", "ABORT", decision="abort")
        await participant_call(RESOURCE_MANAGER_URL, "/tx/abort", {"tx_id": tx_id}, timeout_s=3.0)
        await participant_call(BILLING_SERVICE_URL, "/tx/abort", {"tx_id": tx_id}, timeout_s=3.0)
        return "abort"

    log_tx(tx_id, "2pc", "COMMIT", decision="commit")
    await participant_call(RESOURCE_MANAGER_URL, "/tx/commit", {"tx_id": tx_id}, timeout_s=3.0)
    await participant_call(BILLING_SERVICE_URL, "/tx/commit", {"tx_id": tx_id}, timeout_s=3.0)
    return "commit"

async def run_3pc(tx_id: str, session_id: str, subscriber_id: str, resources: int) -> str:
    # Simplified 3PC: PREPARE -> PRECOMMIT -> COMMIT
    log_tx(tx_id, "3pc", "PREPARE")
    prepare = {"tx_id": tx_id, "session_id": session_id, "subscriber_id": subscriber_id, "resources": resources}

    st1, r1 = await participant_call(RESOURCE_MANAGER_URL, "/tx/prepare", prepare, timeout_s=3.0)
    st2, r2 = await participant_call(BILLING_SERVICE_URL, "/tx/prepare", prepare, timeout_s=3.0)
    if st1 != 200 or st2 != 200 or (not r1.get("ok", False)) or (not r2.get("ok", False)):
        log_tx(tx_id, "3pc", "ABORT", decision="abort")
        await participant_call(RESOURCE_MANAGER_URL, "/tx/abort", {"tx_id": tx_id}, timeout_s=3.0)
        await participant_call(BILLING_SERVICE_URL, "/tx/abort", {"tx_id": tx_id}, timeout_s=3.0)
        return "abort"

    log_tx(tx_id, "3pc", "PRECOMMIT")
    st1b, r1b = await participant_call(RESOURCE_MANAGER_URL, "/tx/precommit", {"tx_id": tx_id}, timeout_s=3.0)
    st2b, r2b = await participant_call(BILLING_SERVICE_URL, "/tx/precommit", {"tx_id": tx_id}, timeout_s=3.0)
    if st1b != 200 or st2b != 200 or (not r1b.get("ok", False)) or (not r2b.get("ok", False)):
        # In 3PC, failures around precommit are handled carefully. For demo, abort.
        log_tx(tx_id, "3pc", "ABORT", decision="abort")
        await participant_call(RESOURCE_MANAGER_URL, "/tx/abort", {"tx_id": tx_id}, timeout_s=3.0)
        await participant_call(BILLING_SERVICE_URL, "/tx/abort", {"tx_id": tx_id}, timeout_s=3.0)
        return "abort"

    log_tx(tx_id, "3pc", "COMMIT", decision="commit")
    await participant_call(RESOURCE_MANAGER_URL, "/tx/commit", {"tx_id": tx_id}, timeout_s=3.0)
    await participant_call(BILLING_SERVICE_URL, "/tx/commit", {"tx_id": tx_id}, timeout_s=3.0)
    return "commit"

@app.post("/demo/deadlock")
async def deadlock_demo():
    """Deterministic deadlock demo.
    Two tasks acquire locks in opposite order, then one aborts via timeout.
    """
    lock_a = "lock:resA"
    lock_b = "lock:resB"
    ttl_ms = 3000

    async def acquire(name: str, first: str, second: str):
        t0 = time.time()
        got1 = r.set(first, name, nx=True, px=ttl_ms)
        if not got1:
            return {"name": name, "result": "failed_first"}
        await asyncio.sleep(0.4)
        deadline = time.time() + 1.2
        while time.time() < deadline:
            got2 = r.set(second, name, nx=True, px=ttl_ms)
            if got2:
                r.delete(second); r.delete(first)
                return {"name": name, "result": "acquired_both", "t": time.time() - t0}
            await asyncio.sleep(0.1)
        r.delete(first)
        return {"name": name, "result": "deadlock_timeout_abort", "t": time.time() - t0}

    t1 = asyncio.create_task(acquire("tx1", lock_a, lock_b))
    t2 = asyncio.create_task(acquire("tx2", lock_b, lock_a))
    res1, res2 = await asyncio.gather(t1, t2)
    if res1.get("result") == "deadlock_timeout_abort" or res2.get("result") == "deadlock_timeout_abort":
        DEADLOCKS.labels("session_manager", "timeout_abort").inc()
    return {"deadlock_demo": True, "tx1": res1, "tx2": res2}

@app.get("/sessions")

async def sessions():
    rows = conn.execute("SELECT session_id, subscriber_id, status, resources, created_at, version FROM sessions ORDER BY created_at DESC LIMIT 200").fetchall()
    return {"sessions": [dict(r) for r in rows]}

@app.get("/tx/{tx_id}")
async def tx(tx_id: str):
    row = fetch_one(conn, "SELECT * FROM tx_log WHERE tx_id=?", (tx_id,))
    if row is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    return dict(row)

@app.get("/metrics")
async def metrics():
    return HTMLResponse(generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST)
