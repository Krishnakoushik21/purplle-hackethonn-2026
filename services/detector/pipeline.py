"""YOLO + ByteTrack CCTV processing pipeline that emits canonical store events."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

import cv2
import numpy as np
import redis.asyncio as aioredis

from config.zones import (
    DWELL_EVENT_SECONDS,
    STORE_ID,
    TRACK_LOST_GRACE_SECONDS,
    get_camera_config,
)
from events.schema import EventType, StoreEvent


logger = logging.getLogger(__name__)


def parse_utc_timestamp(value: Optional[str]) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def point_in_zone(cx: float, cy: float, zone: dict) -> bool:
    if "polygon" in zone:
        polygon = np.asarray(zone["polygon"], dtype=np.float32)
        return cv2.pointPolygonTest(polygon, (float(cx), float(cy)), False) >= 0
    x1, y1, x2, y2 = zone["bbox"]
    return x1 <= cx <= x2 and y1 <= cy <= y2


def resolve_zone(cx: float, cy: float, camera_config: dict) -> Optional[dict]:
    for zone in camera_config.get("zones", []):
        if point_in_zone(cx, cy, zone):
            return zone
    return None


def threshold_crossing(
    previous: Tuple[float, float],
    current: Tuple[float, float],
    threshold: Optional[dict],
) -> Optional[str]:
    if not threshold:
        return None
    axis = str(threshold.get("axis", "y")).lower()
    index = 0 if axis == "x" else 1
    position = float(threshold.get("position", 0.5))
    direction = str(threshold.get("inbound_direction", "increasing")).lower()
    before = previous[index]
    after = current[index]

    crossed_increasing = before < position <= after
    crossed_decreasing = before > position >= after
    if direction == "increasing":
        if crossed_increasing:
            return "inbound"
        if crossed_decreasing:
            return "outbound"
    else:
        if crossed_decreasing:
            return "inbound"
        if crossed_increasing:
            return "outbound"
    return None


@dataclass
class TrackRecord:
    track_id: int
    visitor_id: str
    first_seen: float
    last_seen: float
    point: Tuple[float, float]
    confidence: float
    zone: Optional[dict] = None
    zone_entered_at: Optional[float] = None
    last_dwell_emit_at: Optional[float] = None
    session_seq: int = 0
    entry_emitted: bool = False
    exit_emitted: bool = False
    is_staff: bool = False


class EventEmitter:
    """Emit events to Redis Streams and/or an output JSONL file."""

    STREAM_KEY = "store:events"

    def __init__(self, redis_url: Optional[str], output_path: Optional[str]):
        self.redis_url = redis_url
        self.output_path = Path(output_path) if output_path else None
        self._redis: Optional[aioredis.Redis] = None
        self._file = None

    async def connect(self) -> None:
        if self.redis_url:
            self._redis = await aioredis.from_url(self.redis_url, decode_responses=True)
            await self._redis.ping()
            logger.info("Connected to Redis at %s", self.redis_url)
        if self.output_path:
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            self._file = self.output_path.open("a", encoding="utf-8")
            logger.info("Writing events to %s", self.output_path)

    async def emit(self, event: StoreEvent) -> None:
        payload = event.to_dict()
        logger.info(
            "event type=%s store_id=%s camera_id=%s visitor_id=%s zone_id=%s confidence=%.3f",
            event.event_type.value,
            event.store_id,
            event.camera_id,
            event.visitor_id,
            event.zone_id,
            event.confidence,
        )
        if self._redis:
            redis_payload = {
                key: json.dumps(value) if isinstance(value, (dict, list, bool)) else str(value)
                for key, value in payload.items()
                if value is not None
            }
            await self._redis.xadd(self.STREAM_KEY, redis_payload, maxlen=50000)
        if self._file:
            self._file.write(json.dumps(payload, separators=(",", ":")) + "\n")
            self._file.flush()

    async def close(self) -> None:
        if self._redis:
            await self._redis.aclose()
        if self._file:
            self._file.close()


class CameraProcessor:
    """Process one camera clip and convert tracks into behavioral events."""

    def __init__(
        self,
        camera_id: str,
        store_id: str,
        emitter: EventEmitter,
        camera_config: dict,
        model_name: str = "yolov8n.pt",
        process_every_n: int = 3,
        dwell_event_seconds: int = DWELL_EVENT_SECONDS,
        lost_grace_seconds: float = TRACK_LOST_GRACE_SECONDS,
        allow_mock: bool = False,
    ):
        self.camera_id = camera_id
        self.store_id = store_id
        self.emitter = emitter
        self.camera_config = camera_config
        self.model_name = model_name
        self.process_every_n = max(1, process_every_n)
        self.dwell_event_seconds = dwell_event_seconds
        self.lost_grace_seconds = lost_grace_seconds
        self.allow_mock = allow_mock
        self._model = None
        self._frame_idx = 0
        self._tracks: Dict[int, TrackRecord] = {}
        self._exited_track_ids: set[int] = set()

    def load_models(self) -> None:
        try:
            from ultralytics import YOLO

            self._model = YOLO(self.model_name)
            logger.info("[%s] Loaded %s with ByteTrack", self.camera_id, self.model_name)
        except Exception:
            if not self.allow_mock:
                raise
            logger.warning("[%s] Model unavailable; mock detections explicitly enabled", self.camera_id)

    def _detect_and_track(self, frame: np.ndarray) -> List[dict]:
        if self._model is None:
            return self._mock_detections(frame)
        results = self._model.track(
            frame,
            persist=True,
            classes=[0],
            conf=0.25,
            iou=0.5,
            tracker="bytetrack.yaml",
            verbose=False,
        )
        persons = []
        if results and results[0].boxes is not None:
            boxes = results[0].boxes
            for index in range(len(boxes)):
                if boxes.id is None:
                    continue
                x1, y1, x2, y2 = boxes.xyxy[index].tolist()
                persons.append(
                    {
                        "track_id": int(boxes.id[index]),
                        "bbox": [x1, y1, x2, y2],
                        "conf": float(boxes.conf[index]),
                    }
                )
        return persons

    def _mock_detections(self, frame: np.ndarray) -> List[dict]:
        height, width = frame.shape[:2]
        phase = (self._frame_idx % 300) / 300
        x1 = width * 0.45
        y1 = height * max(0.0, phase - 0.2)
        return [
            {
                "track_id": 1,
                "bbox": [x1, y1, x1 + width * 0.08, min(height, y1 + height * 0.25)],
                "conf": 0.82,
            }
        ]

    async def process_frame(self, frame: np.ndarray, video_seconds: float, event_time: datetime) -> None:
        self._frame_idx += 1
        if self._frame_idx % self.process_every_n != 0:
            return

        height, width = frame.shape[:2]
        detections = self._detect_and_track(frame)
        seen_ids = set()
        for person in detections:
            track_id = person["track_id"]
            seen_ids.add(track_id)
            x1, y1, x2, y2 = person["bbox"]
            point = (((x1 + x2) / 2) / width, y2 / height)
            confidence = float(person["conf"])
            zone = resolve_zone(point[0], point[1], self.camera_config)

            record = self._tracks.get(track_id)
            if not record:
                record = TrackRecord(
                    track_id=track_id,
                    visitor_id=f"VIS_{self.camera_id}_{track_id:06d}",
                    first_seen=video_seconds,
                    last_seen=video_seconds,
                    point=point,
                    confidence=confidence,
                    is_staff=self.camera_config.get("role") == "staff",
                )
                self._tracks[track_id] = record
            else:
                await self._handle_threshold_crossing(record, point, event_time)

            record.last_seen = video_seconds
            record.confidence = confidence
            await self._handle_zone_change(record, zone, point, event_time, video_seconds)
            await self._emit_due_dwell(record, point, event_time, video_seconds)
            record.point = point

        await self._expire_lost_tracks(seen_ids, video_seconds, event_time)

    async def _handle_threshold_crossing(
        self,
        record: TrackRecord,
        point: Tuple[float, float],
        event_time: datetime,
    ) -> None:
        if self.camera_config.get("role") != "entry":
            return
        crossing = threshold_crossing(
            record.point,
            point,
            self.camera_config.get("entry_threshold"),
        )
        if crossing == "inbound":
            event_type = EventType.REENTRY if record.track_id in self._exited_track_ids else EventType.ENTRY
            record.entry_emitted = True
            record.exit_emitted = False
            await self._emit_event(record, event_type, event_time, confidence=record.confidence)
        elif crossing == "outbound" and record.entry_emitted and not record.exit_emitted:
            record.exit_emitted = True
            self._exited_track_ids.add(record.track_id)
            await self._emit_event(record, EventType.EXIT, event_time, confidence=record.confidence)

    async def _handle_zone_change(
        self,
        record: TrackRecord,
        zone: Optional[dict],
        point: Tuple[float, float],
        event_time: datetime,
        video_seconds: float,
    ) -> None:
        old_zone_id = record.zone["id"] if record.zone else None
        new_zone_id = zone["id"] if zone else None
        if old_zone_id == new_zone_id:
            return

        if record.zone:
            dwell_ms = max(0, round((video_seconds - (record.zone_entered_at or video_seconds)) * 1000))
            await self._emit_zone_event(
                record,
                EventType.ZONE_EXIT,
                record.zone,
                point,
                event_time,
                dwell_ms=dwell_ms,
            )

        record.zone = zone
        record.zone_entered_at = video_seconds if zone else None
        record.last_dwell_emit_at = video_seconds if zone else None

        if zone:
            await self._emit_zone_event(record, EventType.ZONE_ENTER, zone, point, event_time)
            if self.camera_config.get("role") == "billing" or zone.get("type") == "BILLING":
                queue_depth = self._queue_depth(zone["id"])
                await self._emit_zone_event(
                    record,
                    EventType.BILLING_QUEUE_JOIN,
                    zone,
                    point,
                    event_time,
                    metadata={"queue_depth": queue_depth},
                )

    async def _emit_due_dwell(
        self,
        record: TrackRecord,
        point: Tuple[float, float],
        event_time: datetime,
        video_seconds: float,
    ) -> None:
        if not record.zone or record.last_dwell_emit_at is None:
            return
        while video_seconds - record.last_dwell_emit_at >= self.dwell_event_seconds:
            record.last_dwell_emit_at += self.dwell_event_seconds
            continuous_ms = round(
                (record.last_dwell_emit_at - (record.zone_entered_at or record.last_dwell_emit_at)) * 1000
            )
            await self._emit_zone_event(
                record,
                EventType.ZONE_DWELL,
                record.zone,
                point,
                event_time,
                dwell_ms=self.dwell_event_seconds * 1000,
                metadata={"continuous_dwell_ms": continuous_ms},
            )

    async def _expire_lost_tracks(
        self,
        seen_ids: set[int],
        video_seconds: float,
        event_time: datetime,
    ) -> None:
        expired = [
            track_id
            for track_id, record in self._tracks.items()
            if track_id not in seen_ids and video_seconds - record.last_seen >= self.lost_grace_seconds
        ]
        for track_id in expired:
            record = self._tracks.pop(track_id)
            if record.zone:
                dwell_ms = max(0, round((video_seconds - (record.zone_entered_at or video_seconds)) * 1000))
                await self._emit_zone_event(
                    record,
                    EventType.ZONE_EXIT,
                    record.zone,
                    record.point,
                    event_time,
                    dwell_ms=dwell_ms,
                    metadata={"track_lost": True},
                )

    def _queue_depth(self, zone_id: str) -> int:
        return sum(
            1
            for record in self._tracks.values()
            if record.zone and record.zone["id"] == zone_id
        )

    async def _emit_event(
        self,
        record: TrackRecord,
        event_type: EventType,
        event_time: datetime,
        zone_id: Optional[str] = None,
        dwell_ms: int = 0,
        confidence: Optional[float] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        record.session_seq += 1
        event_metadata = {"session_seq": record.session_seq}
        event_metadata.update(metadata or {})
        await self.emitter.emit(
            StoreEvent(
                event_id=str(uuid4()),
                store_id=self.store_id,
                camera_id=self.camera_id,
                visitor_id=record.visitor_id,
                event_type=event_type,
                timestamp=event_time,
                zone_id=zone_id,
                dwell_ms=dwell_ms,
                is_staff=record.is_staff,
                confidence=confidence if confidence is not None else record.confidence,
                metadata=event_metadata,
            )
        )

    async def _emit_zone_event(
        self,
        record: TrackRecord,
        event_type: EventType,
        zone: dict,
        point: Tuple[float, float],
        event_time: datetime,
        dwell_ms: int = 0,
        metadata: Optional[dict] = None,
    ) -> None:
        zone_metadata = {
            "zone_name": zone.get("name"),
            "zone_type": zone.get("type"),
            "is_revenue_zone": zone.get("is_revenue_zone", False),
            "sku_zone": zone.get("dept") or zone.get("name"),
            "zone_hotspot_x": round(point[0], 4),
            "zone_hotspot_y": round(point[1], 4),
        }
        zone_metadata.update(metadata or {})
        await self._emit_event(
            record,
            event_type,
            event_time,
            zone_id=zone["id"],
            dwell_ms=dwell_ms,
            metadata=zone_metadata,
        )

    async def run_video(
        self,
        source: str | int,
        clip_start: datetime,
        realtime: bool = False,
    ) -> None:
        capture = cv2.VideoCapture(source)
        if not capture.isOpened():
            raise RuntimeError(f"Cannot open video source: {source}")
        fps = capture.get(cv2.CAP_PROP_FPS) or 25.0
        logger.info(
            "[%s] Processing source=%s role=%s fps=%.2f",
            self.camera_id,
            source,
            self.camera_config.get("role"),
            fps,
        )
        frame_number = 0
        started = time.perf_counter()
        try:
            while True:
                ok, frame = capture.read()
                if not ok:
                    break
                video_seconds = frame_number / fps
                event_time = clip_start + timedelta(seconds=video_seconds)
                await self.process_frame(frame, video_seconds, event_time)
                frame_number += 1
                if realtime:
                    target_elapsed = frame_number / fps
                    sleep_for = target_elapsed - (time.perf_counter() - started)
                    if sleep_for > 0:
                        await asyncio.sleep(sleep_for)
                else:
                    await asyncio.sleep(0)
        finally:
            capture.release()
        logger.info("[%s] Completed %d frames", self.camera_id, frame_number)


async def run_demo(store_id: str, emitter: EventEmitter, duration_seconds: int) -> None:
    config = get_camera_config("CAM1", store_id)
    processor = CameraProcessor(
        camera_id="CAM1",
        store_id=store_id,
        emitter=emitter,
        camera_config=config,
        process_every_n=1,
        allow_mock=True,
    )
    frame = np.zeros((480, 640, 3), dtype=np.uint8)
    clip_start = datetime.now(timezone.utc)
    for frame_number in range(duration_seconds * 10):
        seconds = frame_number / 10
        await processor.process_frame(frame, seconds, clip_start + timedelta(seconds=seconds))
        await asyncio.sleep(0.1)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Store Intelligence Detection Pipeline")
    parser.add_argument("--camera", default="CAM1")
    parser.add_argument("--source", default="0")
    parser.add_argument("--store-id", default=STORE_ID)
    parser.add_argument("--layout", default=None, help="Path to supplied store_layout.json")
    parser.add_argument("--clip-start", default=None, help="ISO-8601 UTC timestamp for frame zero")
    parser.add_argument("--redis-url", default="redis://localhost:6379")
    parser.add_argument("--events-out", default=None, help="Optional JSONL output path")
    parser.add_argument("--model", default="yolov8n.pt")
    parser.add_argument("--process-every-n", type=int, default=3)
    parser.add_argument("--realtime", action="store_true")
    parser.add_argument("--demo", action="store_true")
    parser.add_argument("--demo-duration", type=int, default=120)
    parser.add_argument("--no-redis", action="store_true")
    args = parser.parse_args()

    emitter = EventEmitter(None if args.no_redis else args.redis_url, args.events_out)
    await emitter.connect()
    try:
        if args.demo:
            await run_demo(args.store_id, emitter, args.demo_duration)
            return

        camera_config = get_camera_config(args.camera, args.store_id, args.layout)
        processor = CameraProcessor(
            camera_id=args.camera,
            store_id=args.store_id,
            emitter=emitter,
            camera_config=camera_config,
            model_name=args.model,
            process_every_n=args.process_every_n,
        )
        processor.load_models()
        source: str | int = int(args.source) if args.source.isdigit() else args.source
        await processor.run_video(
            source=source,
            clip_start=parse_utc_timestamp(args.clip_start),
            realtime=args.realtime,
        )
    finally:
        await emitter.close()


if __name__ == "__main__":
    asyncio.run(main())
