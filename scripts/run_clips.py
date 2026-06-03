#!/usr/bin/env python3
"""Run the detector sequentially for all clips listed in a manifest."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Process all camera clips in a manifest")
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--layout", type=Path, default=None)
    parser.add_argument("--store-id", default="ST1008")
    parser.add_argument("--redis-url", default="redis://localhost:6379")
    parser.add_argument("--events-dir", type=Path, default=Path("generated-events"))
    parser.add_argument("--realtime", action="store_true")
    args = parser.parse_args()

    clips = json.loads(args.manifest.read_text(encoding="utf-8"))
    args.events_dir.mkdir(parents=True, exist_ok=True)
    for clip in clips:
        camera_id = clip["camera_id"]
        command = [
            sys.executable,
            "services/detector/pipeline.py",
            "--camera",
            camera_id,
            "--source",
            clip["source"],
            "--store-id",
            args.store_id,
            "--clip-start",
            clip["clip_start"],
            "--redis-url",
            args.redis_url,
            "--events-out",
            str(args.events_dir / f"{camera_id}.jsonl"),
        ]
        if args.layout:
            command.extend(["--layout", str(args.layout)])
        if args.realtime:
            command.append("--realtime")
        print(f"Processing {camera_id}: {clip['source']}")
        subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
