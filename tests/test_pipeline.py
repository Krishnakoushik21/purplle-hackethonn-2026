# PROMPT: Generate focused tests for an entry-threshold and polygon-zone
# mapping layer used after person tracking.
# CHANGES MADE: Kept the tests model-free so they verify business event logic
# without downloading YOLO weights or requiring a GPU.

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from events.schema import EventType, StoreEvent
from services.detector.pipeline import (
    CameraProcessor,
    EventEmitter,
    parse_utc_timestamp,
    point_in_zone,
    threshold_crossing,
)


class FakeEmitter:
    def __init__(self):
        self.events = []

    async def emit(self, event):
        self.events.append(event)


def detection(track_id: int, bottom_y: float, confidence: float = 0.8) -> dict:
    return {
        "track_id": track_id,
        "bbox": [40.0, max(0.0, bottom_y * 100 - 30), 60.0, bottom_y * 100],
        "conf": confidence,
    }


def test_threshold_crossing_respects_inbound_direction():
    threshold = {"axis": "y", "position": 0.3, "inbound_direction": "increasing"}

    assert threshold_crossing((0.4, 0.2), (0.4, 0.4), threshold) == "inbound"
    assert threshold_crossing((0.4, 0.4), (0.4, 0.2), threshold) == "outbound"
    assert threshold_crossing((0.4, 0.1), (0.4, 0.2), threshold) is None


def test_polygon_zone_uses_bottom_center_point_coordinates():
    zone = {
        "polygon": [[0.1, 0.1], [0.7, 0.1], [0.7, 0.7], [0.1, 0.7]],
    }

    assert point_in_zone(0.4, 0.4, zone)
    assert not point_in_zone(0.9, 0.4, zone)


def test_bbox_zone_and_timestamp_parsing():
    assert point_in_zone(0.5, 0.5, {"bbox": [0.1, 0.1, 0.9, 0.9]})
    assert parse_utc_timestamp("2026-04-10T10:00:00+05:30").isoformat() == "2026-04-10T04:30:00+00:00"
    assert parse_utc_timestamp("2026-04-10T10:00:00").tzinfo == timezone.utc


def test_entry_camera_emits_entry_exit_and_reentry_from_crossings():
    emitter = FakeEmitter()
    processor = CameraProcessor(
        camera_id="CAM_ENTRY",
        store_id="STORE_1",
        emitter=emitter,
        camera_config={
            "role": "entry",
            "entry_threshold": {"axis": "y", "position": 0.3, "inbound_direction": "increasing"},
            "zones": [],
        },
        process_every_n=1,
    )
    sequence = iter(
        [
            [detection(1, 0.2)],
            [detection(1, 0.4)],
            [detection(1, 0.2)],
            [detection(1, 0.4)],
        ]
    )
    processor._detect_and_track = lambda _: next(sequence)
    frame = np.zeros((100, 100, 3), dtype=np.uint8)
    start = datetime(2026, 4, 10, tzinfo=timezone.utc)

    async def run():
        for second in range(4):
            await processor.process_frame(frame, float(second), start)

    asyncio.run(run())

    assert [event.event_type for event in emitter.events] == [
        EventType.ENTRY,
        EventType.EXIT,
        EventType.REENTRY,
    ]


def test_floor_camera_emits_zone_enter_dwell_and_lost_exit():
    emitter = FakeEmitter()
    processor = CameraProcessor(
        camera_id="CAM_FLOOR",
        store_id="STORE_1",
        emitter=emitter,
        camera_config={
            "role": "floor",
            "zones": [
                {
                    "id": "Z1",
                    "name": "Shelf",
                    "type": "SHELF",
                    "is_revenue_zone": True,
                    "dept": "skin",
                    "bbox": [0.0, 0.0, 1.0, 1.0],
                }
            ],
        },
        process_every_n=1,
        dwell_event_seconds=1,
        lost_grace_seconds=0.5,
    )
    sequence = iter([[detection(2, 0.5)], [detection(2, 0.5)], []])
    processor._detect_and_track = lambda _: next(sequence)
    frame = np.zeros((100, 100, 3), dtype=np.uint8)
    start = datetime(2026, 4, 10, tzinfo=timezone.utc)

    async def run():
        await processor.process_frame(frame, 0.0, start)
        await processor.process_frame(frame, 1.1, start)
        await processor.process_frame(frame, 2.0, start)

    asyncio.run(run())

    assert [event.event_type for event in emitter.events] == [
        EventType.ZONE_ENTER,
        EventType.ZONE_DWELL,
        EventType.ZONE_EXIT,
    ]
    assert emitter.events[1].dwell_ms == 1000
    assert emitter.events[2].metadata["track_lost"] is True


def test_billing_and_staff_camera_events_include_business_metadata():
    emitter = FakeEmitter()
    processor = CameraProcessor(
        camera_id="CAM_BILLING",
        store_id="STORE_1",
        emitter=emitter,
        camera_config={
            "role": "billing",
            "zones": [
                {
                    "id": "BILLING",
                    "name": "Billing Queue",
                    "type": "BILLING",
                    "is_revenue_zone": True,
                    "dept": "billing",
                    "bbox": [0.0, 0.0, 1.0, 1.0],
                }
            ],
        },
        process_every_n=1,
    )
    processor._detect_and_track = lambda _: [detection(3, 0.5)]
    frame = np.zeros((100, 100, 3), dtype=np.uint8)
    asyncio.run(processor.process_frame(frame, 0.0, datetime.now(timezone.utc)))

    assert [event.event_type for event in emitter.events] == [
        EventType.ZONE_ENTER,
        EventType.BILLING_QUEUE_JOIN,
    ]
    assert emitter.events[1].metadata["queue_depth"] == 1
    assert emitter.events[0].metadata["zone_type"] == "BILLING"

    staff_emitter = FakeEmitter()
    staff_processor = CameraProcessor(
        camera_id="CAM_STAFF",
        store_id="STORE_1",
        emitter=staff_emitter,
        camera_config={
            "role": "staff",
            "zones": [
                {
                    "id": "BACKROOM",
                    "name": "Backroom",
                    "type": "STAFF",
                    "is_revenue_zone": False,
                    "dept": "operations",
                    "bbox": [0.0, 0.0, 1.0, 1.0],
                }
            ],
        },
        process_every_n=1,
    )
    staff_processor._detect_and_track = lambda _: [detection(4, 0.5)]
    asyncio.run(staff_processor.process_frame(frame, 0.0, datetime.now(timezone.utc)))
    assert staff_emitter.events[0].is_staff is True


def test_event_emitter_writes_canonical_jsonl(tmp_path: Path):
    output = tmp_path / "events.jsonl"
    emitter = EventEmitter(redis_url=None, output_path=str(output))
    event = StoreEvent(
        event_id="evt-jsonl",
        store_id="STORE_1",
        camera_id="CAM1",
        visitor_id="VIS1",
        event_type=EventType.ENTRY,
        timestamp="2026-04-10T10:00:00Z",
    )

    async def run():
        await emitter.connect()
        await emitter.emit(event)
        await emitter.close()

    asyncio.run(run())
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["event_type"] == "ENTRY"
    assert payload["event_id"] == "evt-jsonl"
