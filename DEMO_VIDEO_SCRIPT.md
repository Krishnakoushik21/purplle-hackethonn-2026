# Demo Video Script

Target length: 2 to 3 minutes.

## 1. Open With the Architecture

Show `artifacts/architecture.svg` and say:

> This system starts from raw CCTV footage, detects and tracks people, converts
> their movement into structured events, streams those events through Redis,
> ingests them idempotently into FastAPI, and exposes live retail intelligence.

## 2. Run Actual Footage

Show the real clip command:

```bash
python services/detector/pipeline.py \
  --camera CAM3 \
  --source "local-data/videos/CAM 3.mp4" \
  --layout local-data/store_layout.json \
  --clip-start "2026-04-10T20:09:45+05:30" \
  --events-out generated-events/CAM3.jsonl
```

Show:

- Person boxes and persistent tracker IDs
- A visitor crossing the entry threshold
- `ENTRY`, `EXIT`, `ZONE_ENTER`, or `BILLING_QUEUE_JOIN` events being emitted

## 3. Show the Event Stream

Open `generated-events/CAM3.jsonl` or Redis CLI and point out:

- Unique `event_id`
- Stable `visitor_id`
- UTC timestamp
- Zone ID
- Staff flag
- Confidence

## 4. Show the API

Open `http://localhost:8000/docs` and call:

- `POST /events/ingest`
- `GET /stores/ST1008/metrics`
- `GET /stores/ST1008/funnel`
- `GET /stores/ST1008/heatmap`
- `GET /stores/ST1008/anomalies`
- `GET /health`

Mention that ingestion is idempotent and malformed events receive partial
errors instead of failing the whole batch.

## 5. Show the Dashboard

Open `http://localhost:3000` and show:

- Occupancy updating
- Footfall
- Conversion rate
- Zone heatmap
- Queue alert
- Live event stream

## 6. Close With Trade-offs

Say:

> I chose ByteTrack for lightweight tracking, Redis Streams for low operational
> overhead at this scale, and SQLite for a reliable challenge acceptance path.
> The next production upgrade is cross-camera re-identification and stronger
> staff classification on customer-facing cameras.

After uploading the video to YouTube or Drive, add the URL to `README.md`.
