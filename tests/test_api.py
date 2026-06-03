# PROMPT: Generate FastAPI acceptance tests for idempotent batch ingestion,
# staff exclusion, re-entry-safe funnel metrics, POS conversion, anomalies, and
# stale-feed health.
# CHANGES MADE: Replaced mocks with a real temporary SQLite database and CSV so
# the tests exercise the same persistence and correlation path used in Docker.

from __future__ import annotations

import csv
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from services.api import main
from services.api.analytics import POSLoader, StoreAnalytics
from services.api.event_store import EventStore


def event(
    event_id: str,
    event_type: str,
    visitor_id: str,
    timestamp: str,
    *,
    store_id: str = "STORE_1",
    zone_id: str | None = None,
    is_staff: bool = False,
    dwell_ms: int = 0,
    metadata: dict | None = None,
) -> dict:
    return {
        "event_id": event_id,
        "store_id": store_id,
        "camera_id": "CAM_TEST",
        "visitor_id": visitor_id,
        "event_type": event_type,
        "timestamp": timestamp,
        "zone_id": zone_id,
        "dwell_ms": dwell_ms,
        "is_staff": is_staff,
        "confidence": 0.9,
        "metadata": metadata or {},
    }


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    pos_path = tmp_path / "pos.csv"
    with pos_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["store_id", "transaction_id", "timestamp", "basket_value_inr"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "store_id": "STORE_1",
                "transaction_id": "TXN_1",
                "timestamp": "2026-04-10T10:04:00Z",
                "basket_value_inr": "1200",
            }
        )

    monkeypatch.setattr(main, "event_store", EventStore(str(tmp_path / "events.db")))
    monkeypatch.setattr(main, "analytics", StoreAnalytics(POSLoader(str(pos_path))))
    monkeypatch.setattr(main, "REDIS_URL", "")
    with TestClient(main.app) as test_client:
        yield test_client


def core_events() -> list[dict]:
    return [
        event("e1", "ENTRY", "VIS_1", "2026-04-10T10:00:00Z"),
        event("e2", "ZONE_ENTER", "VIS_1", "2026-04-10T10:01:00Z", zone_id="SKINCARE"),
        event(
            "e3",
            "ZONE_EXIT",
            "VIS_1",
            "2026-04-10T10:02:00Z",
            zone_id="SKINCARE",
            dwell_ms=60000,
        ),
        event(
            "e4",
            "BILLING_QUEUE_JOIN",
            "VIS_1",
            "2026-04-10T10:03:00Z",
            zone_id="BILLING",
            metadata={"queue_depth": 5},
        ),
        event("e5", "REENTRY", "VIS_1", "2026-04-10T10:03:30Z"),
        event("e6", "ENTRY", "STAFF_1", "2026-04-10T10:00:00Z", is_staff=True),
    ]


def test_ingest_is_idempotent_and_partially_rejects_malformed_events(client: TestClient):
    malformed = {"event_type": "ENTRY"}
    response = client.post("/events/ingest", json={"events": core_events() + [malformed]})

    assert response.status_code == 207
    assert response.json()["accepted_count"] == 6
    assert response.json()["rejected_count"] == 1

    duplicate = client.post("/events/ingest", json=core_events())
    assert duplicate.status_code == 200
    assert duplicate.json()["accepted_count"] == 0
    assert duplicate.json()["duplicate_count"] == 6


def test_metrics_funnel_heatmap_and_anomalies_use_customer_sessions(client: TestClient):
    assert client.post("/events/ingest", json=core_events()).status_code == 200

    metrics = client.get("/stores/STORE_1/metrics").json()
    assert metrics["unique_visitors"] == 1
    assert metrics["converted_visitors"] == 1
    assert metrics["conversion_rate"] == 100.0
    assert metrics["queue_depth"] == 5

    funnel = client.get("/stores/STORE_1/funnel").json()
    assert [stage["count"] for stage in funnel["stages"]] == [1, 1, 1, 1]

    heatmap = client.get("/stores/STORE_1/heatmap").json()
    assert heatmap["data_confidence"] == "LOW"
    assert heatmap["zones"][0]["zone_id"] == "SKINCARE"

    anomalies = client.get("/stores/STORE_1/anomalies").json()["anomalies"]
    assert anomalies[0]["type"] == "BILLING_QUEUE_SPIKE"
    assert anomalies[0]["suggested_action"]


def test_zero_purchase_store_returns_zero_conversion_instead_of_error(client: TestClient):
    response = client.post(
        "/events/ingest",
        json=[event("s2-e1", "ENTRY", "VIS_2", "2026-04-10T10:00:00Z", store_id="STORE_2")],
    )
    assert response.status_code == 200

    metrics = client.get("/stores/STORE_2/metrics").json()
    assert metrics["unique_visitors"] == 1
    assert metrics["conversion_rate"] == 0.0


def test_health_reports_stale_replayed_feed(client: TestClient):
    assert client.post("/events/ingest", json=core_events()).status_code == 200

    health = client.get("/health").json()
    assert health["database"] == "OK"
    assert health["stores"]["STORE_1"]["feed_status"] == "STALE_FEED"
    assert health["status"] == "DEGRADED"


def test_compatibility_routes_and_recent_event_filter(client: TestClient):
    assert client.post("/events/ingest", json=core_events()).status_code == 200

    summary = client.get("/api/v1/kpi/summary?store_id=STORE_1").json()
    assert summary["cumulative_footfall"] == 1
    assert summary["zone_footfall"]["SKINCARE"] == 1

    recent = client.get("/api/v1/events/recent?store_id=STORE_1&event_type=ENTRY").json()
    assert recent["count"] == 2
    assert all(item["event_type"] == "ENTRY" for item in recent["events"])

    conversion = client.get("/api/v1/pos/conversion?store_id=STORE_1").json()
    assert conversion["conversion_rate_pct"] == 100.0
    assert client.get("/").status_code == 200


def test_ingest_rejects_non_list_and_oversized_batches(client: TestClient):
    assert client.post("/events/ingest", json={"not_events": []}).status_code == 422
    assert client.post("/events/ingest", json=[{} for _ in range(501)]).status_code == 422
