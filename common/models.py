from pydantic import BaseModel, Field
from typing import Optional, Literal, Dict, Any

class SignedRequest(BaseModel):
    payload: Dict[str, Any]
    signature: str

class ParticipantVote(BaseModel):
    ok: bool
    reason: Optional[str] = None

class TxPrepareRequest(BaseModel):
    tx_id: str
    session_id: str
    subscriber_id: str
    resources: int = 1

class TxDecision(BaseModel):
    tx_id: str
    decision: Literal["commit", "abort"]

class TxPrecommit(BaseModel):
    tx_id: str

class SessionCreateRequest(BaseModel):
    subscriber_id: str = Field(min_length=1)
    resources: int = Field(default=1, ge=1, le=10)
    protocol: Literal["2pc", "3pc"] = "2pc"
    traffic_class: Literal["VOICE","DATA"] = "DATA"

class SessionRecord(BaseModel):
    session_id: str
    subscriber_id: str
    status: str
    created_at: float
    resources: int
    version: int
