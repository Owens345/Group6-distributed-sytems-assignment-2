import os
import asyncio
import json
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import httpx
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from common.config import (
    REDIS_URL, HMAC_SECRET,
    SESSION_MANAGER_URL, SESSION_MANAGER_URL_2,
    RESOURCE_MANAGER_URL, BILLING_SERVICE_URL, ANALYTICS_SERVICE_URL
)


from common.security import sign_payload
from common.models import SessionCreateRequest
from common.dsm import DSM
from common.metrics import REQ_LATENCY, REQ_COUNT

app = FastAPI(title="Edge Gateway", version="1.0")

app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")

dsm = DSM(REDIS_URL, namespace="edge_cache")

async def call_session_manager_signed(payload: dict, timeout_s: float = 10.0):
    """Try primary then secondary session manager.
    If primary returns not-leader, automatically fail over for demo reliability.
    """
    urls = [SESSION_MANAGER_URL]
    if SESSION_MANAGER_URL_2:
        urls.append(SESSION_MANAGER_URL_2)
    last = None
    for u in urls:
        try:
            r = await call_signed(f"{u}/sessions/start", payload, timeout_s=timeout_s)
            if r.status_code == 409:
                try:
                    body = r.json()
                    if isinstance(body, dict) and body.get("hint") == "retry":
                        last = r
                        continue
                except Exception:
                    pass
            return r
        except Exception as e:
            last = e
            continue
    if isinstance(last, Exception):
        raise last
    return last


async def call_signed(url: str, payload: dict, timeout_s: float = 3.0):
    signature = sign_payload(payload, HMAC_SECRET)
    body = {"payload": payload, "signature": signature}
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        return await client.post(url, json=body)

@app.get("/", response_class=HTMLResponse)
async def index():
    with open(os.path.join(os.path.dirname(__file__), "static", "index.html"), "r", encoding="utf-8") as f:
        return f.read()

@app.get("/api/health")
async def health():
    return {
        "edge": "ok",
        "session_manager_url": SESSION_MANAGER_URL,
        "resource_manager_url": RESOURCE_MANAGER_URL,
        "billing_service_url": BILLING_SERVICE_URL,
        "analytics_service_url": ANALYTICS_SERVICE_URL,
    }

@app.post("/api/sessions/start")
async def start_session(req: SessionCreateRequest):
    import time
    t0 = time.time()
    status_code = "200"
    try:
        payload = req.model_dump()
        # Edge cache example: if same subscriber recently created session, serve hint
        cache_key = f"recent:{payload['subscriber_id']}"
        cached = dsm.read(cache_key)
        if cached is not None:
            payload["edge_hint_recent_session"] = cached.value

        r = await call_session_manager_signed(payload)
        status_code = str(r.status_code)
        data = r.json()
        # Store recent session id for the subscriber in edge cache
        if r.status_code == 200 and "session_id" in data:
            dsm.write(cache_key, data["session_id"], expected_version=cached.version if cached else None)
        return JSONResponse(data, status_code=r.status_code)
    finally:
        dt = time.time() - t0
        REQ_LATENCY.labels("edge_gateway", "/api/sessions/start", "POST", status_code).observe(dt)
        REQ_COUNT.labels("edge_gateway", "/api/sessions/start", "POST", status_code).inc()

@app.get("/api/sessions")
async def list_sessions():
    import time
    t0 = time.time()
    status_code = "200"
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            for u in [SESSION_MANAGER_URL, SESSION_MANAGER_URL_2]:
                if not u:
                    continue
                try:
                    r = await client.get(f"{u}/sessions")
                    if r.status_code == 200:
                        break
                except Exception:
                    continue
        status_code = str(r.status_code)
        return JSONResponse(r.json(), status_code=r.status_code)
    finally:
        dt = time.time() - t0
        REQ_LATENCY.labels("edge_gateway", "/api/sessions", "GET", status_code).observe(dt)
        REQ_COUNT.labels("edge_gateway", "/api/sessions", "GET", status_code).inc()

@app.post("/api/demo/faults")
async def set_faults(request: Request):
    """Pass through demo toggles to core services.

    Body example:
    {
      "session_manager": {"slow_ms": 0, "random_abort_pct": 0},
      "resource_manager": {"slow_ms": 0, "fail_prepare_pct": 0},
      "billing_service": {"slow_ms": 0, "fail_prepare_pct": 0}
    }
    """
    body = await request.json()
    payload = body
    results = {}
    async with httpx.AsyncClient(timeout=3.0) as client:
        for name, url in [
            ("session_manager", SESSION_MANAGER_URL),
            ("resource_manager", RESOURCE_MANAGER_URL),
            ("billing_service", BILLING_SERVICE_URL),
        ]:
            if name in payload:
                signature = sign_payload(payload[name], HMAC_SECRET)
                r = await client.post(f"{url}/admin/faults", json={"payload": payload[name], "signature": signature})
                results[name] = {"status": r.status_code, "body": r.json()}
    return results

@app.post("/api/demo/load")
async def run_load(request: Request):
    """Simple load generator for presentation.
    Body: { "n": 50, "concurrency": 10, "protocol": "2pc" }
    """
    body = await request.json()
    n = int(body.get("n", 50))
    c = int(body.get("concurrency", 10))
    protocol = body.get("protocol", "2pc")

    sem = asyncio.Semaphore(c)
    lock = asyncio.Lock()
    results = {"ok": 0, "fail": 0}

    async def one(i: int):
        async with sem:
            sub = f"sub_{i%25}"
            req = {
                "subscriber_id": sub,
                "resources": 1,
                "protocol": protocol,
                "traffic_class": "DATA",
            }
            try:
                r = await call_session_manager_signed(req, timeout_s=15.0)
                async with lock:
                    if r.status_code == 200:
                        results["ok"] += 1
                    else:
                        results["fail"] += 1
            except Exception:
                async with lock:
                    results["fail"] += 1

    await asyncio.gather(*[one(i) for i in range(n)])
    return results

@app.post("/api/demo/deadlock")
async def deadlock_demo():
    async with httpx.AsyncClient(timeout=6.0) as client:
        urls = [SESSION_MANAGER_URL]
        if SESSION_MANAGER_URL_2:
            urls.append(SESSION_MANAGER_URL_2)
        for u in urls:
            try:
                r = await client.post(f"{u}/demo/deadlock")
                if r.status_code == 200:
                    return r.json()
            except Exception:
                continue
    return JSONResponse({"error": "session manager unreachable"}, status_code=503)

@app.get("/api/demo/status")

async def demo_status():
    async with httpx.AsyncClient(timeout=2.0) as client:
        async def get(url):
            try:
                r = await client.get(url)
                return r.status_code, r.json()
            except Exception as e:
                return 0, {"error": str(e)}
        sm = await get(f"{SESSION_MANAGER_URL}/status")
        rm = await get(f"{RESOURCE_MANAGER_URL}/status")
        bs = await get(f"{BILLING_SERVICE_URL}/status")
        an = await get(f"{ANALYTICS_SERVICE_URL}/status")
    return {"session_manager": sm, "resource_manager": rm, "billing_service": bs, "analytics_service": an}

@app.get("/metrics")
async def metrics():
    return HTMLResponse(generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST)
