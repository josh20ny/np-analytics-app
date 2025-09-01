# app/cadence/service.py
from __future__ import annotations
from dataclasses import dataclass
from statistics import median
from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy.orm import Session

from app.cadence.constants import (
    DEFAULT_ROLLING_DAYS,
    BUCKET_TARGETS,
    bucket_days,
)
from app.utils.common import get_last_sunday_cst, week_bounds_for
from . import dao

# ──────────────────────────────────────────────────────────────────────────────
# Pure-Python helpers for cadence stats
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class CadenceStats:
    samples_n: int
    median_days: Optional[int]
    iqr_days: Optional[int]
    bucket: str  # weekly | biweekly | monthly | 6weekly | irregular

def _to_date(val):
    if val is None:
        return None
    if isinstance(val, date):
        return val
    return date.fromisoformat(str(val))

def _days_between(sorted_dates: Sequence[date]) -> List[int]:
    return [ (sorted_dates[i] - sorted_dates[i-1]).days for i in range(1, len(sorted_dates)) ]

def _iqr(nums: Sequence[int]) -> Optional[int]:
    if not nums:
        return None
    s = sorted(nums)
    n = len(s)
    q1 = s[n//4]
    q3 = s[(3*n)//4]
    return int(q3 - q1)

def _nearest_bucket(median_days: Optional[int]) -> str:
    if median_days is None:
        return "irregular"
    target = min(BUCKET_TARGETS, key=lambda t: abs(t - median_days))
    if target == 7:   return "weekly"
    if target == 14:  return "biweekly"
    if target == 30:  return "monthly"
    return "6weekly"

def _calc_stats(dates: Sequence[date]) -> CadenceStats:
    uniq = sorted(set(dates))
    if len(uniq) == 0:
        return CadenceStats(0, None, None, "none")
    if len(uniq) == 1:
        return CadenceStats(1, None, None, "one_off")
    gaps = _days_between(uniq)
    med = int(round(median(gaps))) if gaps else None
    if med is not None and med > 42:
        return CadenceStats(len(uniq), med, _iqr(gaps), "irregular")
    return CadenceStats(len(uniq), med, _iqr(gaps), _nearest_bucket(med))

def _missed_cycles(last_seen: Optional[date], bucket: str, as_of: date) -> int:
    if not last_seen or bucket in ("irregular", "one_off"):
        return 0
    d = bucket_days(bucket)
    if not d or d <= 0:
        return 0
    delta_days = (as_of - last_seen).days
    cycles = max(0, delta_days // d - 0)  # conservative
    return int(cycles)

def _build_rows_for_signal(
    person_events: Dict[str, List[date]],
    signal: str,
    as_of: date
) -> List[Tuple]:
    """
    Convert events -> person_cadence rows (see upsert_person_cadence signature).
    """
    rows: List[Tuple] = []
    for pid, dates in person_events.items():
        dates = [_to_date(d) for d in dates if d]
        if not dates:
            continue

        stats = _calc_stats(dates)
        last_seen = max(dates) if dates else None

        if stats.samples_n == 1:
            bucket = "one_off"
            median_days = None
            iqr_days = None
        else:
            median_days = stats.median_days
            iqr_days = stats.iqr_days
            if median_days is not None and median_days > 42:
                bucket = "irregular"
            else:
                bucket = stats.bucket

        expected_next = None
        if last_seen and bucket not in ("irregular","one_off"):
            expected_next = last_seen + timedelta(days=bucket_days(bucket))

        missed = _missed_cycles(last_seen, bucket, as_of)

        rows.append((
            pid,               # person_id
            signal,            # signal
            median_days,       # median_interval_days
            iqr_days,          # iqr_days
            expected_next,     # expected_next_date
            last_seen,         # last_seen_date
            0,                 # current_streak (not tracked here)
            missed,            # missed_cycles
            bucket,            # bucket
            stats.samples_n,   # samples_n
            "median",          # calc_method
            None               # campus_id (unknown at this stage)
        ))
    return rows

# ──────────────────────────────────────────────────────────────────────────────
# Public service methods used by routes
# ──────────────────────────────────────────────────────────────────────────────

def rebuild_person_cadence(
    db: Session,
    *,
    since: Optional[date] = None,
    signals: Iterable[str] = ("give","attend","group"),
    rolling_days: int = DEFAULT_ROLLING_DAYS,
    as_of: Optional[date] = None,
) -> Dict[str, int]:
    as_of = as_of or get_last_sunday_cst()
    totals = {"give": 0, "attend": 0, "group": 0}

    # Give
    if "give" in signals:
        give_events = dao.fetch_giving_events(db, since, as_of=as_of, rolling_days=rolling_days)
        rows = _build_rows_for_signal(give_events, "give", as_of)
        totals["give"] = dao.upsert_person_cadence(rows)

    # Attend (adult proxy via kid check-ins)
    if "attend" in signals:
        att_events = dao.fetch_adult_attendance_events(since, as_of=as_of, rolling_days=rolling_days)
        rows = _build_rows_for_signal(att_events, "attend", as_of)
        totals["attend"] = dao.upsert_person_cadence(rows)

    # Group – status-based (active vs not) at as_of
    if "group" in signals:
        active = dao.fetch_group_active_as_of(as_of)
        rows: List[Tuple] = []
        for pid, is_active in active.items():
            bucket = "weekly" if is_active else "irregular"
            last_seen = as_of if is_active else None
            expected_next = last_seen + timedelta(days=bucket_days(bucket)) if last_seen else None
            rows.append((
                pid, "group", None, None, expected_next, last_seen,
                1 if is_active else 0,    # current_streak (best-effort)
                0,                        # missed_cycles
                bucket,
                1,                        # samples_n (status)
                "status_active_v1",
                None
            ))
        totals["group"] = dao.upsert_person_cadence(rows)

    return totals


def build_weekly_snapshot(db: Session, *, week_end: date, ensure_cadence: bool = True) -> Dict[str, int]:
    """Build snap_person_week for a target week (default: last Sunday CST)."""
    if ensure_cadence:
        rebuild_person_cadence(
            db, signals=("give","attend"), rolling_days=DEFAULT_ROLLING_DAYS, as_of=week_end
        )

    if not week_end:
        week_end = get_last_sunday_cst()
    week_start, wk_end = week_bounds_for(week_end)
    assert wk_end == week_end

    attended = dao.attended_adults_for_week(week_start, week_end)
    give_ontrack = dao.ontrack_give_for_week(week_start, week_end)
    serving_active = dao.fetch_serving_active_as_of(week_end)
    group_active   = dao.fetch_group_active_as_of(week_end)

    rows: List[Tuple] = []
    people = set(attended.keys()) | set(serving_active.keys()) | set(group_active.keys())
    for pid in people:
        att_cnt = int(attended.get(pid, 0))
        att_bool = att_cnt > 0
        give_on  = bool(give_ontrack.get(pid, True))
        served_on = bool(serving_active.get(pid))
        group_on  = bool(group_active.get(pid))
        engaged_tier = int(give_on) + int(served_on) + int(group_on)
        rows.append((
            pid, week_start, week_end,
            att_bool, give_on, served_on, group_on,
            engaged_tier, att_cnt, 0, 0,
            None
        ))

    affected = dao.upsert_snap_person_week(rows)
    return {"snap_rows_upserted": affected, "people": len(rows)}


def attendance_buckets(
    db: Session,
    *,
    window_days: int,
    exclude_lapsed: bool,
    week_end: Optional[date] = None,
) -> dict:
    # Ensure cadence is fresh for ATTEND within requested window
    rebuild_person_cadence(db, signals=("attend",), rolling_days=window_days)
    wk = week_end or get_last_sunday_cst()
    return dao.bucket_counts("attend", week_end=wk, exclude_lapsed=exclude_lapsed)

def build_weekly_report(db: Session, *, week_end: date | None, ensure_snapshot: bool,
                        persist_front_door: bool = True, rolling_days: int = DEFAULT_ROLLING_DAYS,
                        include_nla: bool = False):
    from . import analytics as _analytics
    return _analytics.build_weekly_report(
        db,
        week_end=week_end,
        ensure_snapshot=ensure_snapshot,
        persist_front_door=persist_front_door,
        rolling_days=rolling_days,
        include_nla=include_nla,
    )

def browse_cadences(
    *,
    signal: str,
    bucket: Optional[str],
    exclude_lapsed: bool,
    q: Optional[str],
    order_by: str,
    limit: int,
    offset: int,
) -> dict:
    return dao.list_cadences(
        signal=signal,
        bucket=bucket,
        exclude_lapsed=exclude_lapsed,
        q=q,
        order_by=order_by,
        limit=limit,
        offset=offset,
    )

def person_detail(person_id: str, days: int = 180) -> dict:
    prof = dao.person_profile(person_id)
    cadences = dao.person_cadences(person_id)
    weeks = dao.person_recent_weeks(person_id, days=days)
    # Normalize to a dict keyed by signal (keep list too if you prefer)
    cadence_by_signal = {c["signal"]: c for c in cadences}
    full_name = (prof.get("first_name","") + " " + prof.get("last_name","")).strip() if prof else ""
    return {
        "person": {
            "person_id": prof.get("person_id") if prof else str(person_id),
            "name": full_name,
            "email": prof.get("email") if prof else None,
        },
        "cadences": cadence_by_signal,
        "recent_weeks": weeks,
    }
