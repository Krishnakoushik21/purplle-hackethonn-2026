#!/usr/bin/env python3
"""Generate synthetic sample events and actual API response artifacts."""

from __future__ import annotations

import csv
import json
import tempfile
from pathlib import Path

from fastapi.testclient import TestClient

from services.api import main
from services.api.analytics import POSLoader, StoreAnalytics
from services.api.event_store import EventStore


ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "artifacts"


def sample_event(
    event_id: str,
    event_type: str,
    visitor_id: str,
    timestamp: str,
    *,
    camera_id: str,
    zone_id: str | None = None,
    dwell_ms: int = 0,
    is_staff: bool = False,
    metadata: dict | None = None,
) -> dict:
    return {
        "event_id": event_id,
        "store_id": "ST1008",
        "camera_id": camera_id,
        "visitor_id": visitor_id,
        "event_type": event_type,
        "timestamp": timestamp,
        "zone_id": zone_id,
        "dwell_ms": dwell_ms,
        "is_staff": is_staff,
        "confidence": 0.91,
        "metadata": metadata or {},
    }


def build_events() -> list[dict]:
    return [
        sample_event("sample-001", "ENTRY", "VIS_CAM3_000101", "2026-04-10T14:39:55Z", camera_id="CAM3"),
        sample_event(
            "sample-002",
            "ZONE_ENTER",
            "VIS_CAM3_000101",
            "2026-04-10T14:40:20Z",
            camera_id="CAM2",
            zone_id="Z_CAM2_WALL_SHELF",
            metadata={"zone_name": "CAM2 Cosmetics Wall", "sku_zone": "makeup"},
        ),
        sample_event(
            "sample-003",
            "ZONE_DWELL",
            "VIS_CAM3_000101",
            "2026-04-10T14:40:50Z",
            camera_id="CAM2",
            zone_id="Z_CAM2_WALL_SHELF",
            dwell_ms=30000,
            metadata={"continuous_dwell_ms": 30000, "sku_zone": "makeup"},
        ),
        sample_event(
            "sample-004",
            "ZONE_EXIT",
            "VIS_CAM3_000101",
            "2026-04-10T14:41:20Z",
            camera_id="CAM2",
            zone_id="Z_CAM2_WALL_SHELF",
            dwell_ms=60000,
            metadata={"zone_name": "CAM2 Cosmetics Wall", "sku_zone": "makeup"},
        ),
        sample_event(
            "sample-005",
            "BILLING_QUEUE_JOIN",
            "VIS_CAM3_000101",
            "2026-04-10T14:42:00Z",
            camera_id="CAM5",
            zone_id="Z_BILLING",
            metadata={"queue_depth": 5, "zone_name": "Billing Counter Queue"},
        ),
        sample_event("sample-006", "EXIT", "VIS_CAM3_000101", "2026-04-10T14:44:00Z", camera_id="CAM3"),
        sample_event("sample-007", "ENTRY", "STAFF_CAM4_000001", "2026-04-10T14:39:00Z", camera_id="CAM4", is_staff=True),
        sample_event("sample-008", "ENTRY", "VIS_CAM3_000102", "2026-04-10T14:43:30Z", camera_id="CAM3"),
        sample_event(
            "sample-009",
            "ZONE_ENTER",
            "VIS_CAM3_000102",
            "2026-04-10T14:43:45Z",
            camera_id="CAM1",
            zone_id="Z_CAM1_DISPLAY",
            metadata={"zone_name": "CAM1 Center Display", "sku_zone": "sales-floor"},
        ),
    ]


def main_entry() -> None:
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    events = build_events()
    events_path = ARTIFACTS / "sample_events.jsonl"
    events_path.write_text(
        "\n".join(json.dumps(event, separators=(",", ":")) for event in events) + "\n",
        encoding="utf-8",
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        pos_path = temp / "pos.csv"
        with pos_path.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=["store_id", "transaction_id", "timestamp", "basket_value_inr"],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "store_id": "ST1008",
                    "transaction_id": "TXN_SAMPLE_1",
                    "timestamp": "2026-04-10T14:43:00Z",
                    "basket_value_inr": "1299.00",
                }
            )

        main.event_store = EventStore(str(temp / "events.db"))
        main.analytics = StoreAnalytics(POSLoader(str(pos_path)))
        main.REDIS_URL = ""
        with TestClient(main.app) as client:
            ingest = client.post("/events/ingest", json={"events": events}).json()
            responses = {
                "POST /events/ingest": ingest,
                "GET /stores/ST1008/metrics": client.get("/stores/ST1008/metrics").json(),
                "GET /stores/ST1008/funnel": client.get("/stores/ST1008/funnel").json(),
                "GET /stores/ST1008/heatmap": client.get("/stores/ST1008/heatmap").json(),
                "GET /stores/ST1008/anomalies": client.get("/stores/ST1008/anomalies").json(),
                "GET /health": client.get("/health").json(),
            }
    (ARTIFACTS / "sample_api_responses.json").write_text(
        json.dumps(responses, indent=2),
        encoding="utf-8",
    )
    print(f"Wrote {events_path}")
    print(f"Wrote {ARTIFACTS / 'sample_api_responses.json'}")


if __name__ == "__main__":
    main_entry()
