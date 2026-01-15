import time
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import httpx

app = FastAPI(title="Malicious Node Simulator", version="1.0")

@app.post("/attack/invalid_signature")
async def invalid_signature(target: str = "http://localhost:8001/sessions/start"):
    payload = {"subscriber_id": "sub_attacker", "resources": 1, "protocol": "2pc"}
    bad_sig = "deadbeef" * 8
    async with httpx.AsyncClient(timeout=3.0) as client:
        r = await client.post(target, json={"payload": payload, "signature": bad_sig})
    return {"target": target, "status": r.status_code, "body": r.json()}
