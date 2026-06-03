"""Session-based store intelligence metrics derived from canonical events."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd

from events.schema import EventType


QUEUE_SPIKE_THRESHOLD = 4
DEAD_ZONE_MINUTES = 30
POS_MATCH_WINDOW_MINUTES = 5


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def customer_events(events: Sequence[dict]) -> List[dict]:
    return [event for event in events if not event.get("is_staff", False)]


def latest_event_date(events: Sequence[dict]) -> Optional[date]:
    if not events:
        return None
    return max(parse_timestamp(event["timestamp"]) for event in events).date()


def events_for_date(events: Sequence[dict], window_date: Optional[date]) -> List[dict]:
    if not window_date:
        return []
    return [
        event
        for event in events
        if parse_timestamp(event["timestamp"]).date() == window_date
    ]


class POSLoader:
    """Load either the challenge POS schema or the provided retail export."""

    def __init__(self, csv_path: str):
        self.csv_path = csv_path

    def load(self) -> List[dict]:
        path = Path(self.csv_path)
        if not path.exists():
            return []
        df = pd.read_csv(path)
        df.columns = [str(column).strip().lower() for column in df.columns]
        if df.empty or "store_id" not in df.columns:
            return []

        transaction_col = self._find_column(df, "transaction_id", "order_id", "invoice_number")
        value_col = self._find_column(df, "basket_value_inr", "total_amount", "gmv")
        timestamp_col = self._find_column(df, "timestamp")

        if timestamp_col:
            timestamps = pd.to_datetime(df[timestamp_col], utc=True, errors="coerce")
        else:
            date_col = self._find_column(df, "order_date")
            time_col = self._find_column(df, "order_time")
            if not date_col or not time_col:
                return []
            timestamps = pd.to_datetime(
                df[date_col].astype(str) + " " + df[time_col].astype(str),
                dayfirst=True,
                utc=True,
                errors="coerce",
            )

        normalized = []
        for index, row in df.iterrows():
            timestamp = timestamps.iloc[index]
            if pd.isna(timestamp):
                continue
            normalized.append(
                {
                    "store_id": str(row["store_id"]),
                    "transaction_id": str(row[transaction_col]) if transaction_col else str(index),
                    "timestamp": timestamp.to_pydatetime(),
                    "basket_value_inr": float(row[value_col]) if value_col else 0.0,
                }
            )
        return normalized

    @staticmethod
    def _find_column(df: pd.DataFrame, *candidates: str) -> Optional[str]:
        for candidate in candidates:
            if candidate in df.columns:
                return candidate
        return None


class StoreAnalytics:
    def __init__(self, pos_loader: POSLoader):
        self.pos_loader = pos_loader

    def metrics(self, store_id: str, events: Sequence[dict]) -> dict:
        all_customer = customer_events(events)
        window_date = latest_event_date(all_customer)
        current = events_for_date(all_customer, window_date)
        visitors = self._session_visitors(current)
        zone_stats = self._zone_stats(current)
        purchased = self._purchased_visitors(store_id, current, window_date)
        queue_joiners = self._visitors_for_types(current, {EventType.BILLING_QUEUE_JOIN.value})
        abandoners = self._visitors_for_types(current, {EventType.BILLING_QUEUE_ABANDON.value})

        return {
            "store_id": store_id,
            "window_date": window_date.isoformat() if window_date else None,
            "unique_visitors": len(visitors),
            "conversion_rate": round(len(purchased) / len(visitors) * 100, 2) if visitors else 0.0,
            "converted_visitors": len(purchased),
            "avg_dwell_per_zone_ms": {
                zone_id: values["avg_dwell_ms"] for zone_id, values in zone_stats.items()
            },
            "queue_depth": self._current_queue_depth(current),
            "abandonment_rate": (
                round(len(abandoners) / len(queue_joiners) * 100, 2) if queue_joiners else 0.0
            ),
            "last_event_timestamp": max(
                (event["timestamp"] for event in current),
                default=None,
            ),
        }

    def funnel(self, store_id: str, events: Sequence[dict]) -> dict:
        current = events_for_date(customer_events(events), latest_event_date(customer_events(events)))
        entry = self._visitors_for_types(current, {EventType.ENTRY.value, EventType.REENTRY.value})
        zone = self._visitors_for_types(
            current,
            {EventType.ZONE_ENTER.value, EventType.ZONE_DWELL.value, EventType.ZONE_EXIT.value},
        )
        billing = self._visitors_for_types(
            current,
            {EventType.BILLING_QUEUE_JOIN.value, EventType.BILLING_QUEUE_ABANDON.value},
        )
        purchased = self._purchased_visitors(store_id, current, latest_event_date(current))
        stages = [
            ("ENTRY", entry),
            ("ZONE_VISIT", zone),
            ("BILLING_QUEUE", billing),
            ("PURCHASE", purchased),
        ]

        response = []
        previous_count: Optional[int] = None
        for name, visitor_ids in stages:
            count = len(visitor_ids)
            drop_off = 0.0
            if previous_count:
                drop_off = round(max(0, previous_count - count) / previous_count * 100, 2)
            response.append({"stage": name, "count": count, "drop_off_pct": drop_off})
            previous_count = count
        return {"store_id": store_id, "unit": "visitor_session", "stages": response}

    def heatmap(self, store_id: str, events: Sequence[dict]) -> dict:
        current = events_for_date(customer_events(events), latest_event_date(customer_events(events)))
        visitors = self._session_visitors(current)
        zone_stats = self._zone_stats(current)
        max_visits = max((stats["visit_frequency"] for stats in zone_stats.values()), default=1)
        zones = []
        for zone_id, stats in sorted(zone_stats.items()):
            zones.append(
                {
                    "zone_id": zone_id,
                    "visit_frequency": stats["visit_frequency"],
                    "avg_dwell_ms": stats["avg_dwell_ms"],
                    "normalized_score": round(stats["visit_frequency"] / max_visits * 100, 2),
                }
            )
        return {
            "store_id": store_id,
            "session_count": len(visitors),
            "data_confidence": "LOW" if len(visitors) < 20 else "HIGH",
            "zones": zones,
        }

    def anomalies(self, store_id: str, events: Sequence[dict]) -> dict:
        all_customer = customer_events(events)
        window_date = latest_event_date(all_customer)
        current = events_for_date(all_customer, window_date)
        if not current:
            return {"store_id": store_id, "anomalies": []}

        latest_ts = max(parse_timestamp(event["timestamp"]) for event in current)
        anomalies = []
        queue_depth = self._current_queue_depth(current)
        if queue_depth >= QUEUE_SPIKE_THRESHOLD:
            anomalies.append(
                {
                    "type": "BILLING_QUEUE_SPIKE",
                    "severity": "CRITICAL" if queue_depth >= QUEUE_SPIKE_THRESHOLD * 2 else "WARN",
                    "detected_at": latest_ts.isoformat().replace("+00:00", "Z"),
                    "suggested_action": "Open an additional billing counter and redirect available staff.",
                    "details": {"queue_depth": queue_depth, "threshold": QUEUE_SPIKE_THRESHOLD},
                }
            )

        zone_last_visit: Dict[str, datetime] = {}
        for event in current:
            if event.get("zone_id") and event["event_type"] in {
                EventType.ZONE_ENTER.value,
                EventType.ZONE_DWELL.value,
                EventType.ZONE_EXIT.value,
            }:
                zone_last_visit[event["zone_id"]] = parse_timestamp(event["timestamp"])
        for zone_id, last_visit in sorted(zone_last_visit.items()):
            inactive_minutes = (latest_ts - last_visit).total_seconds() / 60
            if inactive_minutes >= DEAD_ZONE_MINUTES:
                anomalies.append(
                    {
                        "type": "DEAD_ZONE",
                        "severity": "INFO",
                        "detected_at": latest_ts.isoformat().replace("+00:00", "Z"),
                        "suggested_action": "Review merchandising, visibility, and staff coverage for this zone.",
                        "details": {
                            "zone_id": zone_id,
                            "minutes_without_visit": round(inactive_minutes, 1),
                        },
                    }
                )

        conversion = self.metrics(store_id, events)["conversion_rate"]
        prior_rates = self._prior_conversion_rates(store_id, all_customer, window_date)
        if prior_rates:
            baseline = sum(prior_rates) / len(prior_rates)
            if baseline > 0 and conversion < baseline * 0.8:
                anomalies.append(
                    {
                        "type": "CONVERSION_DROP",
                        "severity": "WARN",
                        "detected_at": latest_ts.isoformat().replace("+00:00", "Z"),
                        "suggested_action": "Check billing availability and inspect high-dwell zones for friction.",
                        "details": {
                            "conversion_rate": conversion,
                            "seven_day_average": round(baseline, 2),
                        },
                    }
                )
        return {"store_id": store_id, "anomalies": anomalies}

    def live_state(self, store_id: str, events: Sequence[dict]) -> dict:
        """Derived state used by the live dashboard compatibility routes."""

        current = events_for_date(customer_events(events), latest_event_date(customer_events(events)))
        in_store: Set[str] = set()
        active_zone: Dict[str, str] = {}
        zone_footfall: Dict[str, int] = defaultdict(int)
        hourly_footfall: Dict[int, int] = defaultdict(int)
        dept_footfall: Dict[str, int] = defaultdict(int)

        for event in sorted(current, key=lambda item: parse_timestamp(item["timestamp"])):
            visitor_id = event["visitor_id"]
            event_type = event["event_type"]
            if event_type in {EventType.ENTRY.value, EventType.REENTRY.value}:
                in_store.add(visitor_id)
                hourly_footfall[parse_timestamp(event["timestamp"]).hour] += 1
            elif event_type == EventType.EXIT.value:
                in_store.discard(visitor_id)
                active_zone.pop(visitor_id, None)
            elif event_type == EventType.ZONE_ENTER.value and event.get("zone_id"):
                active_zone[visitor_id] = event["zone_id"]
                zone_footfall[event["zone_id"]] += 1
                dept = event.get("metadata", {}).get("sku_zone")
                if dept:
                    dept_footfall[str(dept)] += 1
            elif event_type == EventType.ZONE_EXIT.value and event.get("zone_id"):
                if active_zone.get(visitor_id) == event["zone_id"]:
                    active_zone.pop(visitor_id, None)

        zone_occupancy: Dict[str, int] = defaultdict(int)
        for zone_id in active_zone.values():
            zone_occupancy[zone_id] += 1
        return {
            "store_id": store_id,
            "total_in_store": len(in_store),
            "zone_occupancy": dict(zone_occupancy),
            "zone_footfall": dict(zone_footfall),
            "hourly_footfall": dict(hourly_footfall),
            "dept_footfall": dict(dept_footfall),
            "peak_zone": max(zone_occupancy, key=zone_occupancy.get) if zone_occupancy else None,
        }

    def _prior_conversion_rates(
        self,
        store_id: str,
        events: Sequence[dict],
        window_date: Optional[date],
    ) -> List[float]:
        if not window_date:
            return []
        rates = []
        for days_ago in range(1, 8):
            target = window_date - timedelta(days=days_ago)
            daily = events_for_date(events, target)
            visitors = self._session_visitors(daily)
            if visitors:
                purchased = self._purchased_visitors(store_id, daily, target)
                rates.append(len(purchased) / len(visitors) * 100)
        return rates

    def _purchased_visitors(
        self,
        store_id: str,
        events: Sequence[dict],
        window_date: Optional[date],
    ) -> Set[str]:
        if not window_date:
            return set()
        billing_events = [
            event
            for event in events
            if event["event_type"] == EventType.BILLING_QUEUE_JOIN.value
        ]
        candidates: List[Tuple[datetime, str]] = sorted(
            (parse_timestamp(event["timestamp"]), event["visitor_id"])
            for event in billing_events
        )
        transactions = sorted(
            transaction["timestamp"]
            for transaction in self.pos_loader.load()
            if transaction["store_id"] == store_id
            and transaction["timestamp"].date() == window_date
        )

        matched: Set[str] = set()
        for transaction_ts in transactions:
            eligible = [
                (timestamp, visitor_id)
                for timestamp, visitor_id in candidates
                if visitor_id not in matched
                and timedelta(0) <= transaction_ts - timestamp <= timedelta(minutes=POS_MATCH_WINDOW_MINUTES)
            ]
            if eligible:
                _, visitor_id = max(eligible, key=lambda item: item[0])
                matched.add(visitor_id)
        return matched

    @staticmethod
    def _session_visitors(events: Sequence[dict]) -> Set[str]:
        return {event["visitor_id"] for event in events if event.get("visitor_id")}

    @staticmethod
    def _visitors_for_types(events: Sequence[dict], event_types: Set[str]) -> Set[str]:
        return {
            event["visitor_id"]
            for event in events
            if event.get("visitor_id") and event["event_type"] in event_types
        }

    @staticmethod
    def _current_queue_depth(events: Sequence[dict]) -> int:
        queue_events = [
            event
            for event in events
            if event["event_type"]
            in {EventType.BILLING_QUEUE_JOIN.value, EventType.BILLING_QUEUE_ABANDON.value}
        ]
        if not queue_events:
            return 0
        latest = max(queue_events, key=lambda event: parse_timestamp(event["timestamp"]))
        queue_depth = latest.get("metadata", {}).get("queue_depth")
        if queue_depth is not None:
            return max(0, int(queue_depth))
        joins = {
            event["visitor_id"]
            for event in queue_events
            if event["event_type"] == EventType.BILLING_QUEUE_JOIN.value
        }
        left = {
            event["visitor_id"]
            for event in queue_events
            if event["event_type"] == EventType.BILLING_QUEUE_ABANDON.value
            or event.get("metadata", {}).get("queue_completed")
        }
        return max(0, len(joins - left))

    @staticmethod
    def _zone_stats(events: Sequence[dict]) -> Dict[str, dict]:
        active: Dict[Tuple[str, str], datetime] = {}
        intervals: Dict[str, List[int]] = defaultdict(list)
        visitors: Dict[str, Set[str]] = defaultdict(set)
        last_ts = max((parse_timestamp(event["timestamp"]) for event in events), default=None)

        for event in sorted(events, key=lambda item: parse_timestamp(item["timestamp"])):
            zone_id = event.get("zone_id")
            visitor_id = event.get("visitor_id")
            if not zone_id or not visitor_id:
                continue
            event_type = event["event_type"]
            timestamp = parse_timestamp(event["timestamp"])
            key = (visitor_id, zone_id)
            if event_type in {
                EventType.ZONE_ENTER.value,
                EventType.ZONE_DWELL.value,
                EventType.ZONE_EXIT.value,
            }:
                visitors[zone_id].add(visitor_id)
            if event_type == EventType.ZONE_ENTER.value:
                active.setdefault(key, timestamp)
            elif event_type == EventType.ZONE_DWELL.value and event.get("dwell_ms", 0):
                intervals[zone_id].append(int(event["dwell_ms"]))
            elif event_type == EventType.ZONE_EXIT.value:
                start = active.pop(key, None)
                if start:
                    intervals[zone_id].append(max(0, int((timestamp - start).total_seconds() * 1000)))
                elif event.get("dwell_ms", 0):
                    intervals[zone_id].append(int(event["dwell_ms"]))

        if last_ts:
            for (_, zone_id), start in active.items():
                intervals[zone_id].append(max(0, int((last_ts - start).total_seconds() * 1000)))

        zone_ids = set(visitors) | set(intervals)
        return {
            zone_id: {
                "visit_frequency": len(visitors.get(zone_id, set())),
                "avg_dwell_ms": round(
                    sum(intervals.get(zone_id, [])) / len(intervals[zone_id]), 2
                )
                if intervals.get(zone_id)
                else 0.0,
            }
            for zone_id in zone_ids
        }
