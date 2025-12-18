from pydantic import BaseModel
from datetime import datetime
from typing import Dict, Any, List

class EventBase(BaseModel):
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: Dict[str, Any]

class EventCreate(EventBase):
    pass

class EventResponse(EventBase):
    processed_at: datetime
    class Config:
        from_attributes = True

class StatsResponse(BaseModel):
    total_received_queued: int
    unique_processed_db: int
    estimated_duplicate_dropped: int
    topics_count: List[Dict[str, Any]]
    uptime_seconds: float