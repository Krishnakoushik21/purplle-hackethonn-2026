# Design

## Architecture Overview

This project turns raw CCTV clips into an operational intelligence surface. The
detection service reads a clip frame by frame, runs YOLO person detection, and
uses ByteTrack to assign a persistent track ID while the person remains visible.
The business event layer then converts geometry into meaning. Entry cameras use
a directional threshold crossing, floor cameras use bottom-center zone
membership, and billing cameras emit queue joins when tracked visitors enter a
billing zone. Events are validated against a canonical Pydantic schema before
they leave the detector.

The detector can publish events to Redis Streams for live operation and can
also write JSONL for offline evaluation. The FastAPI service consumes Redis
events when Redis is available, but its primary public contract is
`POST /events/ingest`. This makes the system testable without requiring the
detection process and supports batch replay of preprocessed clips. Every event
is stored in SQLite with `event_id` as a primary key, so retries are safe and
duplicate events do not inflate metrics.

Analytics are computed at the visitor-session level. Staff events are excluded
before metrics are calculated. Re-entry events retain the same visitor ID and
therefore do not create a second unique visitor. POS transactions have no
customer identity, so purchase conversion is estimated by matching a visitor
who entered the billing queue within the five minutes before a transaction at
the same store. Heatmaps combine zone visit frequency with dwell information,
and anomaly responses include an operational action rather than only a label.

The live dashboard remains connected through WebSocket broadcasts and
compatibility API routes. The judged API routes are independent of dashboard
rendering, which keeps correctness testable even when the UI is unavailable.

## Data Flow

```text
Video frame
  -> YOLO person boxes
  -> ByteTrack track IDs
  -> bottom-center point / threshold crossing
  -> canonical StoreEvent
  -> Redis Stream and/or JSONL
  -> POST /events/ingest compatibility normalizer
  -> SQLite idempotent event store
  -> session analytics
  -> REST API and WebSocket dashboard
```

## Handling Uncertainty and Edge Cases

Low-confidence person detections are retained with their confidence value
instead of being silently suppressed. A track is not treated as an exit after
one missed frame; it receives a configurable grace period to tolerate short
occlusion. A new tracker ID is not automatically counted as a store entry.
Only directional threshold crossings on an entry camera emit `ENTRY` or
`EXIT`. Group entry is naturally counted per tracked person. Empty stores and
missing POS files return valid zero-valued metrics.

The supplied footage differs from the problem statement. It has five short
clips rather than three long angles, and the official sample JSONL contradicts
the PDF event schema. The API therefore normalizes both formats into one
canonical model. This is intentional compatibility, not a second analytics
schema.

## AI-Assisted Decisions

An LLM initially suggested treating every new ByteTrack ID as a store entry.
Visual review and challenge criteria showed that this would dramatically
inflate footfall on every floor camera, so that suggestion was rejected and
replaced with directional threshold crossing.

An LLM also suggested TimescaleDB because the data is time-series shaped. That
is a reasonable future scaling option, but it added an unnecessary container
and made idempotent evaluator tests harder. SQLite was chosen for the challenge
because the PDF explicitly permits it, event volume is small, and a unique
primary key gives a clear retry contract.

Finally, AI helped identify a schema mismatch between the problem statement and
the downloaded sample events. Rather than choosing one and risking test
failure, the implementation accepts both and stores a single canonical
representation. The compatibility layer is covered by tests using deterministic
IDs for sample events that do not provide `event_id`.

## Operational Notes

The health endpoint reports the last event timestamp for each store and marks a
feed stale after ten minutes. Request middleware emits structured log fields:
trace ID, store ID, endpoint, latency, event count, and status code. Database
errors return a structured HTTP 503 response without exposing a raw stack
trace. Docker Compose starts the API, Redis, and dashboard without requiring
video files or POS data.
