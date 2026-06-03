# Submission Checklist

## Before Uploading to GitHub

- [ ] Confirm no CCTV footage, dataset ZIPs, or challenge CSV files are present.
- [ ] Copy your local POS file to `local-data/pos_transactions.csv` only for local testing.
- [ ] Copy the supplied layout to `local-data/store_layout.json` only for local testing.
- [ ] Calibrate the entry threshold and zone polygons against the actual clips.
- [ ] Run `pytest -q` and confirm all tests pass.
- [ ] Run `docker compose up --build` on a clean machine.
- [ ] Confirm `GET /health` returns JSON.
- [ ] Confirm `POST /events/ingest` accepts `artifacts/sample_events.jsonl`.
- [ ] Confirm `GET /stores/ST1008/metrics` returns JSON.
- [ ] Confirm the dashboard opens at `http://localhost:3000`.

## Required Repository Contents

- [x] YOLOv8 + ByteTrack detection and tracking pipeline
- [x] Threshold and zone event generation
- [x] Redis Streams integration
- [x] FastAPI required endpoints
- [x] Live dashboard
- [x] Docker Compose one-command startup
- [x] README with architecture, setup, API docs, and trade-offs
- [x] `DESIGN.md`
- [x] `CHOICES.md`
- [x] Architecture image
- [x] Sample generated events
- [x] Sample API responses
- [x] Dashboard screenshot
- [ ] Demo video URL added to README

## Final Quality Checks

- [ ] Explain the difference between track ID and visitor session ID.
- [ ] Explain why new tracks are not automatically counted as store entries.
- [ ] Explain the current cross-camera re-identification limitation.
- [ ] Explain why SQLite was chosen for the challenge and what changes at scale.
- [ ] Explain how POS transactions are correlated without customer identity.
- [ ] Be ready to discuss how staff exclusion can be improved beyond CAM4.

## Do Not Commit

- CCTV footage
- Dataset ZIP files
- Supplied POS transaction CSV files
- Supplied store layout files
- Model weights
- Generated databases
