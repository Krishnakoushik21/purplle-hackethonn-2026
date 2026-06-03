"""Store layout loading and fallback camera calibration."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


STORE_ID = "ST1008"
STORE_NAME = "Brigade Road"

# These fallback regions keep the demo runnable. Real submissions should pass
# --layout with zones calibrated against the supplied store layout and clips.
CAMERA_ZONES = {
    "CAM1": {
        "role": "floor",
        "description": "Sales floor wall shelf and display camera",
        "zones": [
            {
                "id": "Z_CAM1_WALL_SHELF",
                "name": "CAM1 Wall Shelf",
                "type": "SHELF",
                "is_revenue_zone": True,
                "dept": "sales-floor",
                "polygon": [[0.0, 0.0], [0.82, 0.0], [0.82, 0.70], [0.0, 0.70]],
            },
            {
                "id": "Z_CAM1_DISPLAY",
                "name": "CAM1 Center Display",
                "type": "DISPLAY",
                "is_revenue_zone": True,
                "dept": "sales-floor",
                "polygon": [[0.0, 0.70], [0.82, 0.70], [0.82, 1.0], [0.0, 1.0]],
            },
        ],
    },
    "CAM2": {
        "role": "floor",
        "description": "Sales floor cosmetics wall and center display camera",
        "zones": [
            {
                "id": "Z_CAM2_WALL_SHELF",
                "name": "CAM2 Cosmetics Wall",
                "type": "SHELF",
                "is_revenue_zone": True,
                "dept": "makeup",
                "polygon": [[0.18, 0.0], [1.0, 0.0], [1.0, 0.76], [0.18, 0.76]],
            },
            {
                "id": "Z_CAM2_DISPLAY",
                "name": "CAM2 Center Display",
                "type": "DISPLAY",
                "is_revenue_zone": True,
                "dept": "makeup",
                "polygon": [[0.0, 0.45], [0.58, 0.45], [0.58, 1.0], [0.0, 1.0]],
            },
        ],
    },
    "CAM3": {
        "role": "entry",
        "description": "Entrance and exit threshold camera",
        "entry_threshold": {
            "axis": "y",
            "position": 0.30,
            "inbound_direction": "increasing",
        },
        "zones": [
            {
                "id": "Z_ENTRANCE",
                "name": "Entrance Threshold",
                "type": "ENTRY",
                "is_revenue_zone": False,
                "dept": "general",
                "polygon": [[0.08, 0.0], [0.68, 0.0], [0.68, 0.72], [0.08, 0.72]],
            },
        ],
    },
    "CAM4": {
        "role": "staff",
        "description": "Backroom and staff service area camera",
        "zones": [
            {
                "id": "Z_BACKROOM",
                "name": "Backroom",
                "type": "STAFF",
                "is_revenue_zone": False,
                "dept": "operations",
                "bbox": [0.0, 0.0, 1.0, 1.0],
            },
        ],
    },
    "CAM5": {
        "role": "billing",
        "description": "Billing counter and customer queue camera",
        "zones": [
            {
                "id": "Z_BILLING",
                "name": "Billing Counter Queue",
                "type": "BILLING",
                "is_revenue_zone": True,
                "dept": "billing",
                "polygon": [[0.12, 0.0], [0.68, 0.0], [0.68, 0.62], [0.12, 0.62]],
            },
        ],
    },
}


def get_camera_config(
    camera_id: str,
    store_id: str = STORE_ID,
    layout_path: Optional[str] = None,
) -> dict:
    """Return normalized camera configuration from a layout JSON or fallback."""

    if layout_path:
        loaded = _load_from_layout(Path(layout_path), store_id, camera_id)
        if loaded:
            return loaded
    return CAMERA_ZONES.get(camera_id, {"role": "floor", "zones": []})


def _load_from_layout(path: Path, store_id: str, camera_id: str) -> Optional[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Store layout not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    store = _select_store(data, store_id)
    camera = _select_camera(store, camera_id)
    if not camera:
        return None

    zones = camera.get("zones") or _zones_for_camera(store, camera_id)
    return {
        "role": str(camera.get("role") or camera.get("camera_type") or "floor").lower(),
        "description": camera.get("description", ""),
        "entry_threshold": camera.get("entry_threshold") or camera.get("threshold"),
        "zones": [_normalize_zone(zone) for zone in zones],
    }


def _select_store(data: Any, store_id: str) -> dict:
    if isinstance(data, dict) and "stores" in data:
        stores = data["stores"]
    else:
        stores = data
    if isinstance(stores, dict):
        if store_id in stores and isinstance(stores[store_id], dict):
            return stores[store_id]
        if stores.get("store_id") == store_id:
            return stores
    if isinstance(stores, list):
        for store in stores:
            if str(store.get("store_id") or store.get("id")) == store_id:
                return store
    return data if isinstance(data, dict) else {}


def _select_camera(store: dict, camera_id: str) -> Optional[dict]:
    cameras = store.get("cameras") or store.get("camera_coverage") or {}
    if isinstance(cameras, dict):
        camera = cameras.get(camera_id)
        if isinstance(camera, dict):
            return camera
    if isinstance(cameras, list):
        for camera in cameras:
            if str(camera.get("camera_id") or camera.get("id")) == camera_id:
                return camera
    return None


def _zones_for_camera(store: dict, camera_id: str) -> Iterable[dict]:
    zones = store.get("zones") or []
    return [
        zone
        for zone in zones
        if camera_id
        in {
            str(value)
            for value in (
                zone.get("camera_ids")
                or zone.get("cameras")
                or [zone.get("camera_id")]
            )
            if value is not None
        }
    ]


def _normalize_zone(zone: dict) -> dict:
    zone_id = str(zone.get("zone_id") or zone.get("id") or zone.get("name"))
    zone_type = str(zone.get("zone_type") or zone.get("type") or "SHELF").upper()
    revenue_value = zone.get("is_revenue_zone", zone_type in {"SHELF", "DISPLAY", "BILLING"})
    if isinstance(revenue_value, str):
        revenue_value = revenue_value.strip().lower() in {"yes", "true", "1"}
    normalized: Dict[str, Any] = {
        "id": zone_id,
        "name": str(zone.get("zone_name") or zone.get("name") or zone_id),
        "type": zone_type,
        "is_revenue_zone": bool(revenue_value),
        "dept": str(zone.get("dept") or zone.get("department") or ""),
    }
    if zone.get("polygon"):
        normalized["polygon"] = zone["polygon"]
    elif zone.get("bbox"):
        normalized["bbox"] = zone["bbox"]
    elif zone.get("coordinates"):
        normalized["polygon"] = zone["coordinates"]
    else:
        raise ValueError(f"Zone {zone_id} is missing polygon or bbox coordinates")
    return normalized


PEAK_HOURS = [12, 13, 16, 17, 19, 20]
DWELL_EVENT_SECONDS = 30
QUEUE_PERSON_THRESHOLD = 4
TRACK_LOST_GRACE_SECONDS = 1.0
