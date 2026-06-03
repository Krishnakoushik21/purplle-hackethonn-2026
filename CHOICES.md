# Choices

## 1. Detection Model and Tracking

Options considered were YOLOv8 with ByteTrack, a larger YOLO model with
DeepSORT, and a VLM-driven frame classification approach. AI suggested YOLOv8n
because it is fast, widely supported, and simple to run in a container. I
agreed with that starting point, but not with the suggestion to use the default
high confidence threshold and discard uncertain detections. Retail CCTV has
partial occlusion, wide camera angles, and small people in the frame. The
pipeline uses a lower detection threshold and preserves confidence on every
event so downstream analytics can reason about uncertainty.

ByteTrack was chosen because it can associate lower-confidence detections
without a separate re-identification model and is built into Ultralytics. A
DeepSORT or OSNet-based approach would improve cross-camera identity, but it
would add model weight, tuning effort, and a larger failure surface during a
short challenge. The current limitation is explicit: visitor IDs are stable
inside one camera clip, while cross-camera re-identification remains future
work.

## 2. Event Schema Design

The PDF defines uppercase canonical events with `event_id`, `visitor_id`,
`timestamp`, `dwell_ms`, `is_staff`, `confidence`, and metadata. The downloaded
sample JSONL instead uses lowercase event names, multiple timestamp field names,
`id_token` for entry events, `track_id` for zone events, and specialized queue
records. AI initially suggested matching the PDF exactly and ignoring the
sample. I overrode that suggestion because the sample is described as the
expected detection output and may be used by automated tests.

The API accepts both formats through a normalization layer and stores only the
canonical model. Missing sample `event_id` values are generated from a stable
hash of the raw payload, which makes replay idempotent. The detector itself
emits only the canonical PDF format. This avoids spreading compatibility rules
through metrics code while reducing the risk created by contradictory official
materials.

## 3. API Persistence and Event Streaming

Options considered were Redis-only state, Redis Streams plus TimescaleDB, and
Redis Streams plus SQLite. AI suggested TimescaleDB because the events are
time-series data and future multi-store queries would benefit from hypertables.
That choice is defensible at production scale, but the challenge acceptance
gate values a system that starts reliably and handles retries correctly.

SQLite was chosen as the source of truth for the API because the challenge
explicitly permits it, the supplied event volume is small, and `event_id` can
be enforced as a primary key with no extra service. Redis Streams remains the
live event transport between detector and API. The public ingestion endpoint is
also first-class, so events can be replayed from JSONL and the evaluator can
test the API without starting a GPU pipeline.

This architecture has a clear upgrade path. At larger scale, SQLite would be
replaced by PostgreSQL or TimescaleDB, API instances would use a shared
database, and Redis consumer groups would distribute event ingestion. The
current design deliberately optimizes for correctness, explainability, and a
clean `docker compose up` experience.
