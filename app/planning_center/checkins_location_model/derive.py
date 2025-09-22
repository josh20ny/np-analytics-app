# ============================
# app/planning_center/checkins_location_model/derive.py
# ============================
from __future__ import annotations
from datetime import datetime, timezone, time
from typing import Any, Dict, Optional, Iterable, List

import pytz

# Map location roots to ministry keys used across the app
MINISTRY_KEYWORDS = {
    "waumba": "Waumba Land",
    "upstreet": "UpStreet",
    "transit": "Transit",
    "insideout": "InsideOut",
}

SERVICE_WINDOWS = {
    "9:30 AM": (time(8, 30),  time(10, 30)),
    "11:00 AM":(time(10, 30), time(12, 30)),
    "4:30 PM": (time(15, 15), time(17, 30)),
}

def _bucket_from_dt(dt_utc: datetime) -> str:
    local = dt_utc.astimezone(CENTRAL_TZ)
    t = local.time()
    for label, (start, end) in SERVICE_WINDOWS.items():
        if start <= t < end:
            return label
    return ""

CENTRAL_TZ = pytz.timezone("America/Chicago")

# Prefer leaf-closest match; if multiple, apply a deterministic priority.
PREFERRED_ORDER = ["InsideOut", "Transit", "UpStreet", "Waumba Land"]
MINISTRY_PATTERNS = {
    "InsideOut":  ["insideout", "inside out", "inside-out", "io"],
    "Transit":    ["transit"],
    "UpStreet":   ["upstreet", "up street", "up-street"],
    "Waumba Land": ["waumba land", "waumbaland", "waumba"],
}

def derive_ministry_from_chain(names: List[str]) -> Optional[str]:
    """
    Resolve ministry using the location ancestor chain.
    1) Scan leaf -> root; the first match wins (closest ancestor).
    2) If none matched by proximity, scan the joined string using a deterministic
       priority so 'UpStreet' beats 'Waumba Land' when both appear (combo labels).
    3) Heuristic: grade/Kinder terms imply UpStreet when ambiguous.
    """
    # 1) Leaf -> root proximity
    for raw in (names or []):
        s = (raw or "").lower()
        for ministry in PREFERRED_ORDER:
            for pat in MINISTRY_PATTERNS[ministry]:
                if pat in s:
                    return ministry

    # 2) Global match with deterministic priority
    joined = " ".join((n or "").lower() for n in (names or []))
    for ministry in PREFERRED_ORDER:
        for pat in MINISTRY_PATTERNS[ministry]:
            if pat in joined:
                return ministry

    # 3) Grade/Kinder heuristic → UpStreet
    if any(tok in joined for tok in ["kinder", "1st", "2nd", "3rd", "4th", "5th"]):
        return "UpStreet"

    return None

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
    Return one of: '9:30 AM' | '11:00 AM' | '4:30 PM' (or '' if unknown).
    """
    try:
        if isinstance(evt_time, dict):
            a = evt_time.get("attributes") or {}
            # Some orgs label event times as the bucket already (e.g., "11:00 AM")
            label = (a.get("label") or a.get("name") or "").strip()
            if label in ("9:30 AM", "11:00 AM", "4:30 PM"):
                return label
            starts = _ts(a.get("starts_at")) or _ts(a.get("shows_at")) or _ts(a.get("hides_at"))
            if starts:
                b = _bucket_from_dt(starts)
                if b:
                    return b
    except Exception:
        # fall through to fallback path
        pass
    return _bucket_from_dt(fallback_created_at_utc)

def derive_ministry_key(location_name_chain: str) -> Optional[str]:
    s = (location_name_chain or "").lower()
    for k, v in MINISTRY_KEYWORDS.items():
        if k in s:
            return v
    return None

def service_from_location_chain(names: Iterable[str]) -> Optional[str]:
    """
    Some ministries (e.g., Transit) model services as locations: '9:30 Service', '11:00 Service'.
    Look for a time token in any piece of the chain.
    """
    joined = " ".join((n or "").lower() for n in names)
    if "4:30" in joined:
        return "4:30 PM"
    if "9:30" in joined:
        return "9:30 AM"
    if "11:00" in joined or "11am" in joined or "11:00am" in joined:
        return "11:00 AM"
    return None
