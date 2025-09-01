# app/cadence/analytics.py
from __future__ import annotations
from datetime import date, timedelta
from typing import Optional, Dict

from sqlalchemy.orm import Session

from app.cadence.constants import DEFAULT_ROLLING_DAYS
from app.utils.common import get_last_sunday_cst, week_bounds_for
from . import dao
from . import service  # to call snapshot

def _adult_attendance_avg_4w_direct(as_of: date) -> int:
    # last 4 Sundays including `as_of`
    sundays = [as_of - timedelta(days=7*i) for i in range(4)]
    totals = []
    for s in sundays:
        ws, we = week_bounds_for(s)
        # attended_adults_for_week returns a dict keyed by person_id
        totals.append(len(dao.attended_adults_for_week(ws, we)))
    return int(sum(totals) / len(totals)) if totals else 0

def build_weekly_report(
    db: Session,
    *,
    week_end: Optional[date] = None,
    ensure_snapshot: bool = True,
    persist_front_door: bool = True,
    rolling_days: int = DEFAULT_ROLLING_DAYS,
    include_nla: bool = False,   # ← new: don't include NLA rows by default
) -> dict:
    as_of = week_end or get_last_sunday_cst()
    week_start, as_of = week_bounds_for(as_of)

    if ensure_snapshot:
        service.build_weekly_snapshot(db, week_end=as_of, ensure_cadence=True)

    attend_buckets = dao.bucket_counts("attend", week_end=as_of, exclude_lapsed=True)
    give_buckets   = dao.bucket_counts("give",   week_end=as_of, exclude_lapsed=True)

    engaged_counts = dao.engaged_tier_counts(as_of)
    fd             = dao.front_door_counts(as_of)
    back_door      = {"downshifts": dao.downshifts_count(as_of)}
    att_avg_4w     = _adult_attendance_avg_4w_direct(as_of)

    # Phase 5: lapses + NLA
    lapse_info = dao.detect_and_upsert_lapses_for_week(as_of)
    lapses     = dao.fetch_new_lapses_for_week(as_of, limit=100)

    # Keep the table fresh for CSV export, but don't dump rows into the report
    _ = dao.refresh_no_longer_attends_flat(as_of, inactivity_days=180)
    nla_total = dao.nla_count(as_of)

    payload = {
        "week_start": week_start.isoformat(),
        "week_end":   as_of.isoformat(),
        "cadence_buckets": {"attend": attend_buckets, "give": give_buckets},
        "engaged": {
            "engaged0": engaged_counts.get(0, 0),
            "engaged1": engaged_counts.get(1, 0),
            "engaged2": engaged_counts.get(2, 0),
            "engaged3": engaged_counts.get(3, 0),
        },
        "front_door": fd,
        "back_door":  back_door | {
            "lapses_new": lapse_info["inserted"],
            "lapses_by_signal": lapse_info["by_signal"],
            "nla_count": nla_total,         # ← just a count
        },
        "lapses": lapses,                    # person-level sample (newly lapsed this week)
        "as_of": as_of.isoformat(),
        "adult_attendance_avg_4w": att_avg_4w,
        "notes": [],
    }

    # Only include the heavy NLA array if explicitly requested
    if include_nla:
        payload["no_longer_attends"] = dao.sample_nla(as_of, limit=100)
    return payload