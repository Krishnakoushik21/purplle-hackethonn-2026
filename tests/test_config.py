# PROMPT: Generate tests for a flexible store layout loader that can use a
# supplied JSON layout while retaining a safe fallback camera configuration.
# CHANGES MADE: Added missing-file behavior and normalized revenue-zone checks
# because layout calibration errors should fail loudly.

from __future__ import annotations

import json
from pathlib import Path

import pytest

from config.zones import get_camera_config


def test_get_camera_config_uses_fallback_for_known_camera():
    config = get_camera_config("CAM3")

    assert config["role"] == "entry"
    assert config["entry_threshold"]["position"] == 0.3


def test_get_camera_config_loads_and_normalizes_layout(tmp_path: Path):
    layout = {
        "stores": [
            {
                "store_id": "STORE_1",
                "cameras": [
                    {
                        "camera_id": "CAM_X",
                        "camera_type": "floor",
                        "zones": [
                            {
                                "zone_id": "Z_X",
                                "zone_name": "Display",
                                "zone_type": "display",
                                "is_revenue_zone": "Yes",
                                "coordinates": [[0, 0], [1, 0], [1, 1], [0, 1]],
                            }
                        ],
                    }
                ],
            }
        ]
    }
    path = tmp_path / "layout.json"
    path.write_text(json.dumps(layout), encoding="utf-8")

    config = get_camera_config("CAM_X", "STORE_1", str(path))

    assert config["role"] == "floor"
    assert config["zones"][0]["id"] == "Z_X"
    assert config["zones"][0]["type"] == "DISPLAY"
    assert config["zones"][0]["is_revenue_zone"] is True
    assert "polygon" in config["zones"][0]


def test_get_camera_config_missing_layout_fails_loudly(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        get_camera_config("CAM_X", "STORE_1", str(tmp_path / "missing.json"))
