import hmac
import hashlib
import json
from typing import Any, Dict

def canonical_json(data: Dict[str, Any]) -> bytes:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

def sign_payload(payload: Dict[str, Any], secret: str) -> str:
    msg = canonical_json(payload)
    return hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()

def verify_payload(payload: Dict[str, Any], signature: str, secret: str) -> bool:
    expected = sign_payload(payload, secret)
    return hmac.compare_digest(expected, signature)
