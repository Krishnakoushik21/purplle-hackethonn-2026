"""Normalize official PDF events, sample JSONL events, and legacy demo events."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict

from events.schema import EventType, StoreEvent


EVENT_TYPE_ALIASES = {
    "ENTRY": EventType.ENTRY,
    "EXIT": EventType.EXIT,
    "ZONE_ENTER": EventType.ZONE_ENTER,
    "ZONE_ENTERED": EventType.ZONE_ENTER,
    "ZONE_EXIT": EventType.ZONE_EXIT,
    "ZONE_EXITED": EventType.ZONE_EXIT,
    "ZONE_DWELL": EventType.ZONE_DWELL,
    "BILLING_QUEUE_JOIN": EventType.BILLING_QUEUE_JOIN,
    "BILLING_QUEUE_ABANDON": EventType.BILLING_QUEUE_ABANDON,
    "REENTRY": EventType.REENTRY,
    "QUEUE_COMPLETED": EventType.BILLING_QUEUE_JOIN,
    "QUEUE_ABANDONED": EventType.BILLING_QUEUE_ABANDON,
    "PERSON_ENTER_STORE": EventType.ENTRY,
    "PERSON_EXIT_STORE": EventType.EXIT,
    "DWELL_ALERT": EventType.ZONE_DWELL,
    "QUEUE_DETECTED": EventType.BILLING_QUEUE_JOIN,
}

CANONICAL_FIELDS = {
    "event_id",
    "store_id",
    "camera_id",
    "visitor_id",
    "event_type",
    "timestamp",
    "zone_id",
    "dwell_ms",
    "is_staff",
    "confidence",
    "metadata",
}


def _stable_event_id(raw: Dict[str, Any]) -> str:
    serialized = json.dumps(raw, sort_keys=True, default=str, separators=(",", ":"))
    digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    return f"evt_{digest[:32]}"


def _first(raw: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        value = raw.get(key)
        if value is not None and value != "":
            return value
    return default


def _visitor_id(raw: Dict[str, Any], camera_id: str) -> str:
    visitor_id = _first(raw, "visitor_id", "id_token")
    if visitor_id:
        return str(visitor_id)
    track_id = raw.get("track_id")
    if track_id is not None:
        return f"VIS_{camera_id}_{track_id}"
    raise ValueError("visitor_id, id_token, or track_id is required")


def _metadata(raw: Dict[str, Any]) -> Dict[str, Any]:
    metadata = dict(raw.get("metadata") or {})
    for key, value in raw.items():
        if key not in CANONICAL_FIELDS and value is not None:
            metadata.setdefault(key, value)
    return metadata


def normalize_event(raw: Dict[str, Any]) -> StoreEvent:
    """Convert a supported event payload into the canonical challenge schema."""

    if not isinstance(raw, dict):
        raise ValueError("event must be a JSON object")

    raw_type = str(raw.get("event_type", "")).strip().upper()
    event_type = EVENT_TYPE_ALIASES.get(raw_type)
    if not event_type:
        supported = ", ".join(sorted(EVENT_TYPE_ALIASES))
        raise ValueError(f"unsupported event_type '{raw.get('event_type')}'. Supported: {supported}")

    store_id = str(_first(raw, "store_id", "store_code", default="")).strip()
    camera_id = str(_first(raw, "camera_id", default="")).strip()
    timestamp = _first(
        raw,
        "timestamp",
        "event_timestamp",
        "event_time",
        "queue_join_ts",
        "queue_exit_ts",
    )
    if not store_id:
        raise ValueError("store_id or store_code is required")
    if not camera_id:
        raise ValueError("camera_id is required")
    if not timestamp:
        raise ValueError("timestamp is required")

    metadata = _metadata(raw)
    if raw_type == "QUEUE_COMPLETED":
        metadata.setdefault("queue_completed", True)
        metadata.setdefault("abandoned", False)
    elif raw_type == "QUEUE_ABANDONED":
        metadata.setdefault("abandoned", True)

    dwell_ms = _first(raw, "dwell_ms", default=None)
    if dwell_ms is None and raw.get("dwell_seconds") is not None:
        dwell_ms = round(float(raw["dwell_seconds"]) * 1000)
    if dwell_ms is None:
        dwell_ms = 0

    zone_id = _first(raw, "zone_id", "to_zone_id", default=None)
    confidence = _first(raw, "confidence", "conf", default=1.0)

    canonical = {
        "event_id": str(_first(raw, "event_id", "queue_event_id", default=_stable_event_id(raw))),
        "store_id": store_id,
        "camera_id": camera_id,
        "visitor_id": _visitor_id(raw, camera_id),
        "event_type": event_type,
        "timestamp": timestamp,
        "zone_id": str(zone_id) if zone_id is not None else None,
        "dwell_ms": int(float(dwell_ms)),
        "is_staff": bool(raw.get("is_staff", False)),
        "confidence": float(confidence),
        "metadata": metadata,
    }
    return StoreEvent.model_validate(canonical)
