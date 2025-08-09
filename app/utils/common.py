from __future__ import annotations
from typing import TYPE_CHECKING
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Any, Dict, Generator, Iterable, Optional, Tuple
from zoneinfo import ZoneInfo
import time
import math
import requests
if TYPE_CHECKING:
    # type-only import so runtime doesn't require it yet
    from app.models import AdultAttendanceMetrics

# ─────────────────────────────
# Time & Date helpers
# ─────────────────────────────
CENTRAL_TZ = ZoneInfo("America/Chicago")

def now_cst() -> datetime:
    return datetime.now(tz=CENTRAL_TZ)

def get_last_sunday_cst() -> date:
    today = now_cst()
    return (today - timedelta(days=(today.weekday() + 1) % 7)).date()

def week_bounds_for(d: date, tz: ZoneInfo = CENTRAL_TZ) -> Tuple[date, date]:
    """Return Monday..Sunday (inclusive) for the week containing d, in given tz."""
    weekday = d.weekday()  # Monday=0
    monday = d - timedelta(days=weekday)
    sunday = monday + timedelta(days=6)
    return monday, sunday

def get_previous_week_dates_cst() -> Tuple[str, str]:
    """Previous completed Mon..Sun window in CST, ISO strings."""
    last_sun = get_last_sunday_cst()
    last_mon = last_sun - timedelta(days=6)
    return last_mon.isoformat(), last_sun.isoformat()

# Keep your existing UTC flavor if you still need it:
def get_previous_week_dates() -> Tuple[str, str]:
    today = datetime.utcnow().date()
    last_sunday = today - timedelta(days=today.weekday() + 1)
    last_monday = last_sunday - timedelta(days=6)
    return last_monday.isoformat(), last_sunday.isoformat()

# Google Sheets/Excel date parsing
def excel_serial_to_date(n: float | int) -> date:
    # Excel/Sheets “day zero” offset
    return (datetime(1899, 12, 30) + timedelta(days=int(n))).date()

def parse_sheet_date(raw: Any) -> Optional[date]:
    """Accepts ISO string or Excel serial; returns date or None."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return excel_serial_to_date(raw)
    try:
        # Fast path for YYYY-MM-DD
        if isinstance(raw, str) and len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
            y, m, d = raw.split("-")
            return date(int(y), int(m), int(d))
        # Fallback: very light parser without bringing in dateutil
        return datetime.fromisoformat(str(raw)).date()
    except Exception:
        return None

# ─────────────────────────────
# Math / display helpers
# ─────────────────────────────
def safe_percent(numer: float, denom: float, precision: int = 2) -> float:
    if not denom:
        return 0.0
    return round((numer / denom) * 100.0, precision)

def cents_to_dollars(cents: int, precision: int = 2) -> float:
    return round(cents / 100.0, precision)


def compute_adult_attendance_metrics(chair_count: int, a930: int, a1100: int) -> "AdultAttendanceMetrics":
    total = a930 + a1100
    pc_930  = round((a930  / chair_count) * 100, 2) if chair_count else 0.0
    pc_1100 = round((a1100 / chair_count) * 100, 2) if chair_count else 0.0
    pd_930  = round((a930  / total) * 100, 2) if total else 0.0
    pd_1100 = round((a1100 / total) * 100, 2) if total else 0.0

    # local import avoids circular import at module load time
    from app.models import AdultAttendanceMetrics
    return AdultAttendanceMetrics(total, pc_930, pc_1100, pd_930, pd_1100)

# ─────────────────────────────
# HTTP / pagination helpers
# ─────────────────────────────
def request_json(method: str, url: str, *, headers=None, params=None, json_body=None,
                 timeout: int = 30, retries: int = 2, backoff: float = 0.6) -> Dict[str, Any]:
    """Tiny wrapper with naive retries + exponential backoff."""
    attempt = 0
    while True:
        try:
            r = requests.request(method.upper(), url, headers=headers, params=params, json=json_body, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt >= retries:
                raise
            time.sleep(backoff * (2 ** attempt))
            attempt += 1

def paginate_next_links(url: str, *, headers=None, params=None, timeout: int = 30) -> Generator[Dict[str, Any], None, None]:
    """
    Yield successive JSON pages for APIs that provide `links.next` (e.g., Planning Center).
    First call passes `params`, subsequent calls rely on the `next` URL.
    """
    first = True
    while url:
        data = request_json("GET", url, headers=headers, params=(params if first else None), timeout=timeout)
        yield data
        url = (data.get("links") or {}).get("next")
        first = False

# ─────────────────────────────
# Mailchimp auth helper (Basic)
# ─────────────────────────────
from requests.auth import HTTPBasicAuth
def mailchimp_auth(user: str, api_key: str) -> HTTPBasicAuth:
    # Per Mailchimp, username can be any non-empty string; often "anystring" or "user"
    return HTTPBasicAuth(user, api_key)
