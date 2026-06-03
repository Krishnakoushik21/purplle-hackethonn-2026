#!/usr/bin/env python3
"""Replay a JSONL event file into POST /events/ingest in bounded batches."""

from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from pathlib import Path


def post_batch(api_url: str, events: list[dict]) -> dict:
    body = json.dumps({"events": events}).encode("utf-8")
    request = urllib.request.Request(
        f"{api_url.rstrip('/')}/events/ingest",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8")
        raise RuntimeError(f"Ingest failed with HTTP {exc.code}: {detail}") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay JSONL events into the Store Intelligence API")
    parser.add_argument("events_file", type=Path)
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args()

    lines = [line for line in args.events_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    events = [json.loads(line) for line in lines]
    accepted = duplicates = rejected = 0
    for start in range(0, len(events), args.batch_size):
        response = post_batch(args.api_url, events[start : start + args.batch_size])
        accepted += response.get("accepted_count", 0)
        duplicates += response.get("duplicate_count", 0)
        rejected += response.get("rejected_count", 0)
    print(f"accepted={accepted} duplicates={duplicates} rejected={rejected}")


if __name__ == "__main__":
    main()
