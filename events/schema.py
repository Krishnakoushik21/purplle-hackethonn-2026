"""Canonical event schema for the Store Intelligence API."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class EventType(str, Enum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"
    ZONE_ENTER = "ZONE_ENTER"
    ZONE_EXIT = "ZONE_EXIT"
    ZONE_DWELL = "ZONE_DWELL"
    BILLING_QUEUE_JOIN = "BILLING_QUEUE_JOIN"
    BILLING_QUEUE_ABANDON = "BILLING_QUEUE_ABANDON"
    REENTRY = "REENTRY"


ZONE_EVENT_TYPES = {
    EventType.ZONE_ENTER,
    EventType.ZONE_EXIT,
    EventType.ZONE_DWELL,
    EventType.BILLING_QUEUE_JOIN,
    EventType.BILLING_QUEUE_ABANDON,
}


class StoreEvent(BaseModel):
    """Event contract described in the challenge problem statement."""

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(default_factory=lambda: str(uuid4()), min_length=1)
    store_id: str = Field(min_length=1)
    camera_id: str = Field(min_length=1)
    visitor_id: str = Field(min_length=1)
    event_type: EventType
    timestamp: datetime
    zone_id: Optional[str] = None
    dwell_ms: int = Field(default=0, ge=0)
    is_staff: bool = False
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("event_type", mode="before")
    @classmethod
    def normalize_event_type(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip().upper()
        return value

    @field_validator("timestamp")
    @classmethod
    def ensure_utc_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def validate_zone_fields(self) -> "StoreEvent":
        if self.event_type in ZONE_EVENT_TYPES and not self.zone_id:
            raise ValueError(f"zone_id is required for {self.event_type.value}")
        if self.event_type != EventType.ZONE_DWELL and self.dwell_ms < 0:
            raise ValueError("dwell_ms must be non-negative")
        return self

    def to_dict(self) -> dict:
        payload = self.model_dump(mode="json")
        payload["timestamp"] = self.timestamp.isoformat().replace("+00:00", "Z")
        return payload
