"""SQLite-backed idempotent event storage."""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Iterable, List, Optional

from events.schema import StoreEvent


SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id       TEXT PRIMARY KEY,
    store_id       TEXT NOT NULL,
    camera_id      TEXT NOT NULL,
    visitor_id     TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    timestamp      TEXT NOT NULL,
    zone_id        TEXT,
    dwell_ms       INTEGER NOT NULL DEFAULT 0,
    is_staff       INTEGER NOT NULL DEFAULT 0,
    confidence     REAL NOT NULL DEFAULT 1.0,
    metadata_json  TEXT NOT NULL DEFAULT '{}',
    raw_json       TEXT NOT NULL,
    created_at     TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_events_store_time
    ON events(store_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_events_store_visitor
    ON events(store_id, visitor_id);
CREATE INDEX IF NOT EXISTS idx_events_store_type
    ON events(store_id, event_type);
"""


class EventStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            if self._conn:
                self._conn.close()
                self._conn = None

    def ping(self) -> bool:
        with self._lock:
            self._require_conn().execute("SELECT 1").fetchone()
        return True

    def insert_many(self, events: Iterable[StoreEvent]) -> tuple[int, int]:
        accepted = 0
        duplicates = 0
        with self._lock:
            conn = self._require_conn()
            for event in events:
                payload = event.to_dict()
                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO events (
                        event_id, store_id, camera_id, visitor_id, event_type,
                        timestamp, zone_id, dwell_ms, is_staff, confidence,
                        metadata_json, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.event_id,
                        event.store_id,
                        event.camera_id,
                        event.visitor_id,
                        event.event_type.value,
                        payload["timestamp"],
                        event.zone_id,
                        event.dwell_ms,
                        int(event.is_staff),
                        event.confidence,
                        json.dumps(event.metadata, sort_keys=True, default=str),
                        json.dumps(payload, sort_keys=True, default=str),
                    ),
                )
                if cursor.rowcount == 1:
                    accepted += 1
                else:
                    duplicates += 1
            conn.commit()
        return accepted, duplicates

    def list_events(self, store_id: Optional[str] = None) -> List[dict]:
        with self._lock:
            conn = self._require_conn()
            if store_id:
                rows = conn.execute(
                    "SELECT raw_json FROM events WHERE store_id = ? ORDER BY timestamp, event_id",
                    (store_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT raw_json FROM events ORDER BY timestamp, event_id"
                ).fetchall()
        return [json.loads(row["raw_json"]) for row in rows]

    def recent_events(
        self,
        store_id: Optional[str] = None,
        limit: int = 50,
        event_type: Optional[str] = None,
    ) -> List[dict]:
        clauses = []
        params: list[object] = []
        if store_id:
            clauses.append("store_id = ?")
            params.append(store_id)
        if event_type:
            clauses.append("event_type = ?")
            params.append(event_type.upper())
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        with self._lock:
            rows = self._require_conn().execute(
                f"SELECT raw_json FROM events {where} ORDER BY timestamp DESC, event_id DESC LIMIT ?",
                params,
            ).fetchall()
        return [json.loads(row["raw_json"]) for row in rows]

    def last_event_timestamps(self) -> dict[str, str]:
        with self._lock:
            rows = self._require_conn().execute(
                """
                SELECT store_id, MAX(timestamp) AS last_timestamp
                FROM events
                GROUP BY store_id
                ORDER BY store_id
                """
            ).fetchall()
        return {row["store_id"]: row["last_timestamp"] for row in rows}

    def _require_conn(self) -> sqlite3.Connection:
        if not self._conn:
            raise sqlite3.OperationalError("event database is not connected")
        return self._conn
