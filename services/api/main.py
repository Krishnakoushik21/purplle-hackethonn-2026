"""Purplle Store Intelligence API."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, List, Optional
from uuid import uuid4

import redis.asyncio as aioredis
from fastapi import Body, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from events.normalizer import normalize_event
from events.schema import StoreEvent
from services.api.analytics import POSLoader, StoreAnalytics, parse_timestamp
from services.api.event_store import EventStore


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("store-intelligence-api")

EVENT_DB_PATH = os.getenv("EVENT_DB_PATH", "/data/store-intelligence.db")
POS_CSV = os.getenv("POS_CSV", "/data/pos_transactions.csv")
REDIS_URL = os.getenv("REDIS_URL", "")
STALE_FEED_MINUTES = 10

event_store = EventStore(EVENT_DB_PATH)
analytics = StoreAnalytics(POSLoader(POS_CSV))


class WSManager:
    def __init__(self) -> None:
        self._clients: List[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self._clients.append(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self._clients:
            self._clients.remove(websocket)

    async def broadcast(self, payload: dict) -> None:
        message = json.dumps(payload, default=str)
        dead = []
        for websocket in list(self._clients):
            try:
                await websocket.send_text(message)
            except Exception:
                dead.append(websocket)
        for websocket in dead:
            self.disconnect(websocket)


ws_manager = WSManager()


async def _redis_consumer() -> None:
    """Consume detector events when Redis is configured; API ingestion remains primary."""

    last_id = "0-0"
    while True:
        redis_client = None
        try:
            redis_client = await aioredis.from_url(REDIS_URL, decode_responses=True)
            await redis_client.ping()
            logger.info("redis_stream_consumer_connected url=%s", REDIS_URL)
            while True:
                streams = await redis_client.xread({"store:events": last_id}, count=100, block=1000)
                for _, entries in streams:
                    accepted_events = []
                    for message_id, raw in entries:
                        last_id = message_id
                        try:
                            accepted_events.append(normalize_event(_decode_redis_event(raw)))
                        except (ValueError, ValidationError) as exc:
                            logger.warning("redis_event_rejected id=%s error=%s", message_id, exc)
                    if accepted_events:
                        event_store.insert_many(accepted_events)
                        await _broadcast_stores({event.store_id for event in accepted_events})
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("redis_stream_consumer_unavailable error=%s", exc)
            await asyncio.sleep(2)
        finally:
            if redis_client:
                await redis_client.aclose()


def _decode_redis_event(raw: dict) -> dict:
    decoded = {}
    for key, value in raw.items():
        try:
            decoded[key] = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            decoded[key] = value
    return decoded


async def _broadcast_stores(store_ids: set[str]) -> None:
    for store_id in store_ids:
        events = event_store.list_events(store_id)
        await ws_manager.broadcast(_dashboard_kpi(store_id, events))


def _dashboard_kpi(store_id: str, events: list[dict]) -> dict:
    metrics = analytics.metrics(store_id, events)
    heatmap = analytics.heatmap(store_id, events)
    live = analytics.live_state(store_id, events)
    return {
        **live,
        "timestamp": metrics["last_event_timestamp"],
        "cumulative_footfall": metrics["unique_visitors"],
        "avg_dwell_by_zone": {
            zone_id: round(dwell_ms / 1000, 1)
            for zone_id, dwell_ms in metrics["avg_dwell_per_zone_ms"].items()
        },
        "heatmap": {zone["zone_id"]: zone["normalized_score"] for zone in heatmap["zones"]},
        "recent_anomalies": analytics.anomalies(store_id, events)["anomalies"],
    }


@asynccontextmanager
async def lifespan(_: FastAPI):
    event_store.connect()
    redis_task = asyncio.create_task(_redis_consumer()) if REDIS_URL else None
    logger.info("event_store_connected path=%s", EVENT_DB_PATH)
    try:
        yield
    finally:
        if redis_task:
            redis_task.cancel()
            await asyncio.gather(redis_task, return_exceptions=True)
        event_store.close()


app = FastAPI(
    title="Purplle Store Intelligence API",
    version="2.0.0",
    description="Raw CCTV events to real-time offline retail intelligence.",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def structured_request_logging(request: Request, call_next):
    trace_id = request.headers.get("x-trace-id", str(uuid4()))
    request.state.trace_id = trace_id
    request.state.event_count = 0
    path_parts = [part for part in request.url.path.split("/") if part]
    request.state.store_id = (
        path_parts[1] if len(path_parts) >= 2 and path_parts[0] == "stores" else None
    )
    started = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        response.headers["x-trace-id"] = trace_id
        return response
    finally:
        logger.info(
            "request trace_id=%s store_id=%s endpoint=%s latency_ms=%.2f event_count=%s status_code=%s",
            trace_id,
            getattr(request.state, "store_id", None),
            request.url.path,
            (time.perf_counter() - started) * 1000,
            getattr(request.state, "event_count", 0),
            status_code,
        )


@app.exception_handler(sqlite3.Error)
async def database_error_handler(request: Request, exc: sqlite3.Error):
    logger.exception("database_error trace_id=%s", getattr(request.state, "trace_id", None))
    return JSONResponse(
        status_code=503,
        content={
            "error": {
                "code": "DATABASE_UNAVAILABLE",
                "message": "The event store is temporarily unavailable.",
                "trace_id": getattr(request.state, "trace_id", None),
            }
        },
    )


@app.post("/events/ingest", summary="Validate, deduplicate, and store up to 500 events")
async def ingest_events(request: Request, payload: Any = Body(...)):
    raw_events = payload.get("events") if isinstance(payload, dict) and "events" in payload else payload
    if not isinstance(raw_events, list):
        raise HTTPException(422, "Payload must be a JSON array or an object with an 'events' array")
    if len(raw_events) > 500:
        raise HTTPException(422, "A maximum of 500 events may be ingested per request")

    request.state.event_count = len(raw_events)
    valid_events: List[StoreEvent] = []
    errors = []
    for index, raw_event in enumerate(raw_events):
        try:
            valid_events.append(normalize_event(raw_event))
        except (ValueError, ValidationError, TypeError) as exc:
            errors.append({"index": index, "message": str(exc)})

    accepted, duplicates = event_store.insert_many(valid_events)
    store_ids = {event.store_id for event in valid_events}
    request.state.store_id = ",".join(sorted(store_ids)) if store_ids else None
    if accepted:
        await _broadcast_stores(store_ids)

    response = {
        "accepted_count": accepted,
        "duplicate_count": duplicates,
        "rejected_count": len(errors),
        "errors": errors,
    }
    return JSONResponse(status_code=207 if errors else 200, content=response)


@app.get("/stores/{store_id}/metrics", summary="Current store metrics")
async def store_metrics(store_id: str):
    return analytics.metrics(store_id, event_store.list_events(store_id))


@app.get("/stores/{store_id}/funnel", summary="Session-level conversion funnel")
async def store_funnel(store_id: str):
    return analytics.funnel(store_id, event_store.list_events(store_id))


@app.get("/stores/{store_id}/heatmap", summary="Normalized zone heatmap")
async def store_heatmap(store_id: str):
    return analytics.heatmap(store_id, event_store.list_events(store_id))


@app.get("/stores/{store_id}/anomalies", summary="Active operational anomalies")
async def store_anomalies(store_id: str):
    return analytics.anomalies(store_id, event_store.list_events(store_id))


@app.get("/health", summary="Service and feed health")
async def health():
    event_store.ping()
    now = datetime.now(timezone.utc)
    stores = {}
    stale = False
    for store_id, timestamp in event_store.last_event_timestamps().items():
        lag_minutes = max(0.0, (now - parse_timestamp(timestamp)).total_seconds() / 60)
        feed_status = "STALE_FEED" if lag_minutes > STALE_FEED_MINUTES else "OK"
        stale = stale or feed_status == "STALE_FEED"
        stores[store_id] = {
            "last_event_timestamp": timestamp,
            "feed_status": feed_status,
            "lag_minutes": round(lag_minutes, 2),
        }
    return {
        "status": "DEGRADED" if stale else "OK",
        "database": "OK",
        "stores": stores,
        "timestamp": now.isoformat().replace("+00:00", "Z"),
    }


# ---------------------------------------------------------------------------
# Compatibility endpoints used by the existing dashboard.
# ---------------------------------------------------------------------------
@app.get("/api/v1/kpi/summary")
async def legacy_kpi_summary(store_id: str = Query("ST1008")):
    events = event_store.list_events(store_id)
    return _dashboard_kpi(store_id, events)


@app.get("/api/v1/footfall/live")
async def legacy_footfall_live(store_id: str = Query("ST1008")):
    events = event_store.list_events(store_id)
    metrics = analytics.metrics(store_id, events)
    live = analytics.live_state(store_id, events)
    return {
        "store_id": store_id,
        "total_in_store": live["total_in_store"],
        "cumulative_footfall": metrics["unique_visitors"],
        "timestamp": metrics["last_event_timestamp"],
    }


@app.get("/api/v1/zones/heatmap")
async def legacy_heatmap(store_id: str = Query("ST1008")):
    response = analytics.heatmap(store_id, event_store.list_events(store_id))
    return {
        "store_id": store_id,
        "heatmap": [
            {"zone_id": zone["zone_id"], "heat_pct": zone["normalized_score"]}
            for zone in response["zones"]
        ],
    }


@app.get("/api/v1/anomalies")
async def legacy_anomalies(store_id: str = Query("ST1008")):
    return analytics.anomalies(store_id, event_store.list_events(store_id))


@app.get("/api/v1/events/recent")
async def legacy_recent_events(
    store_id: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
    event_type: Optional[str] = None,
):
    events = event_store.recent_events(store_id=store_id, limit=limit, event_type=event_type)
    return {"store_id": store_id, "count": len(events), "events": events}


@app.get("/api/v1/pos/conversion")
async def legacy_pos_conversion(store_id: str = Query("ST1008")):
    metrics = analytics.metrics(store_id, event_store.list_events(store_id))
    return {
        "store_id": store_id,
        "cumulative_footfall": metrics["unique_visitors"],
        "converted_visitors": metrics["converted_visitors"],
        "conversion_rate_pct": metrics["conversion_rate"],
        "alert": metrics["conversion_rate"] < 5.0,
    }


@app.websocket("/ws/live")
async def websocket_live(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            await asyncio.sleep(30)
            await websocket.send_text(json.dumps({"type": "ping"}))
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)


@app.get("/")
async def root():
    return {
        "message": "Purplle Store Intelligence API v2.0",
        "docs": "/docs",
        "ingest": "/events/ingest",
    }
