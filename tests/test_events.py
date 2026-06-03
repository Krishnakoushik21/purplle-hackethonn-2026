# PROMPT: Generate tests for a Pydantic event schema that must accept the PDF
# contract and normalize a contradictory official sample JSONL format.
# CHANGES MADE: Added deterministic-id assertions and a missing-zone failure
# because idempotency and partial rejection are evaluator-facing requirements.

from __future__ import annotations

import pytest
from pydantic import ValidationError

from events.normalizer import normalize_event
from events.schema import EventType, StoreEvent


def test_canonical_event_normalizes_timestamp_to_utc():
    event = StoreEvent(
        event_id="evt-1",
        store_id="STORE_1",
        camera_id="CAM_ENTRY",
        visitor_id="VIS_1",
        event_type="entry",
        timestamp="2026-04-10T10:00:00+05:30",
        confidence=0.75,
    )

    assert event.event_type == EventType.ENTRY
    assert event.to_dict()["timestamp"] == "2026-04-10T04:30:00Z"


def test_sample_entry_event_is_normalized_with_deterministic_id():
    raw = {
        "event_type": "entry",
        "id_token": "ID_60001",
        "store_code": "store_1076",
        "camera_id": "cam1",
        "event_timestamp": "2026-03-08T18:10:05.120000",
        "is_staff": False,
        "gender_pred": "F",
        "age_pred": 28,
    }

    first = normalize_event(raw)
    second = normalize_event(raw)

    assert first.event_type == EventType.ENTRY
    assert first.visitor_id == "ID_60001"
    assert first.store_id == "store_1076"
    assert first.event_id == second.event_id
    assert first.metadata["gender_pred"] == "F"


def test_sample_zone_event_uses_camera_scoped_track_visitor_id():
    event = normalize_event(
        {
            "event_type": "zone_entered",
            "track_id": 101,
            "store_id": "ST1076",
            "camera_id": "CAM2",
            "zone_id": "Z01",
            "event_time": "2026-03-08T18:10:45.280000",
        }
    )

    assert event.event_type == EventType.ZONE_ENTER
    assert event.visitor_id == "VIS_CAM2_101"
    assert event.zone_id == "Z01"


def test_zone_event_requires_zone_id():
    with pytest.raises(ValidationError):
        StoreEvent(
            event_id="evt-zone",
            store_id="STORE_1",
            camera_id="CAM_FLOOR",
            visitor_id="VIS_1",
            event_type=EventType.ZONE_ENTER,
            timestamp="2026-04-10T10:00:00Z",
        )


def test_unknown_event_type_is_rejected():
    with pytest.raises(ValueError, match="unsupported event_type"):
        normalize_event(
            {
                "event_type": "customer_smiled",
                "store_id": "STORE_1",
                "camera_id": "CAM1",
                "visitor_id": "VIS1",
                "timestamp": "2026-04-10T10:00:00Z",
            }
        )
