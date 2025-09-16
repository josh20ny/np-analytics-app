# ============================
# app/planning_center/checkins_location_model/derive.py
# ============================
from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pytz

# Map location roots to ministry keys used across the app
MINISTRY_KEYWORDS = {
    "waumba": "WaumbaLand",
    "upstreet": "UpStreet",
    "transit": "Transit",
    "insideout": "InsideOut",
}

CENTRAL_TZ = pytz.timezone("America/Chicago")

def _ts(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)

def choose_event_time_for_checkin(included_idx: Dict[tuple, Dict[str, Any]], checkin_created_at: datetime) -> Optional[Dict[str, Any]]:
    """Select EventTime whose [shows_at, hides_at] window contains the checkin time."""
    best = None
    best_width = None
    for (t, _id), obj in included_idx.items():
        if t != "EventTime":
            continue
        a = obj.get("attributes") or {}
        shows = _ts(a.get("shows_at"))
        hides = _ts(a.get("hides_at"))
        if not shows or not hides:
            continue
        if shows <= checkin_created_at <= hides:
            width = (hides - shows).total_seconds()
            if best_width is None or width < best_width:
                best = obj
                best_width = width
    return best

def derive_service_bucket(evt_time: Optional[Dict[str, Any]], fallback_created_at_utc: datetime) -> str:
    """
    Return a human-readable label (e.g., '9:00am').
    This function MUST return a string — never an object.
    """
    try:
        if isinstance(evt_time, dict):
            a = evt_time.get("attributes") or {}
            label = (a.get("name") or a.get("label") or "").strip()
            if isinstance(label, str) and label:
                return label

            starts = _ts(a.get("starts_at"))
            if starts:
                local = starts.astimezone(CENTRAL_TZ)
                return local.strftime("%I:%M%p").lstrip("0").lower()
    except Exception:
        # fall through to fallback path
        pass

    local = fallback_created_at_utc.astimezone(CENTRAL_TZ)
    return local.strftime("%I:%M%p").lstrip("0").lower()

def derive_ministry_key(location_name_chain: str) -> Optional[str]:
    s = (location_name_chain or "").lower()
    for k, v in MINISTRY_KEYWORDS.items():
        if k in s:
            return v
    return None
