# app/cadence/routes.py
from __future__ import annotations
from datetime import date
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

router = APIRouter(prefix="/analytics/cadence", tags=["Analytics"])

from app.db import get_db
from app.cadence.constants import DEFAULT_ROLLING_DAYS
from . import service
from . import exports

router = APIRouter(prefix="/analytics/cadence", tags=["Analytics"])

@router.get("/rebuild", response_model=dict)
def api_rebuild_cadence(
    signals: str = Query("give,attend,group"),
    since: str | None = Query(None),
    rolling_days: int = Query(DEFAULT_ROLLING_DAYS, ge=30, le=730),
    week_end: str | None = Query(None),
    db: Session = Depends(get_db),
):
    sigs = [s.strip().lower() for s in signals.split(",") if s.strip()]
    since_dt = date.fromisoformat(since) if since else None
    week_end_dt = date.fromisoformat(week_end) if week_end else None
    totals = service.rebuild_person_cadence(
        db, since=since_dt, signals=sigs, rolling_days=rolling_days, as_of=week_end_dt
    )
    return {
        "status": "ok",
        "signals": sigs,
        "since": str(since_dt) if since_dt else None,
        "week_end": str(week_end_dt) if week_end_dt else None,
        "rolling_days": rolling_days,
        **totals,
    }

@router.get("/snap-week", response_model=dict)
def api_snap_week(
    week_end: str | None = Query(None),
    ensure_cadence: bool = Query(True, description="Rebuild cadence before snapshot"),
    db: Session = Depends(get_db),
):
    week_end_dt = date.fromisoformat(week_end) if week_end else None
    if not week_end_dt:
        from app.utils.common import get_last_sunday_cst
        week_end_dt = get_last_sunday_cst()
    res = service.build_weekly_snapshot(db, week_end=week_end_dt, ensure_cadence=ensure_cadence)
    from app.utils.common import week_bounds_for
    ws, we = week_bounds_for(week_end_dt)
    return {"status": "ok", "week_start": str(ws), "week_end": str(we), **res}

@router.get("/attendance-buckets", response_model=dict)
def api_attendance_buckets(
    window_days: int = Query(DEFAULT_ROLLING_DAYS, ge=30, le=730),
    exclude_lapsed: bool = Query(True),
    db: Session = Depends(get_db),
):
    counts = service.attendance_buckets(db, window_days=window_days, exclude_lapsed=exclude_lapsed)
    return {"status": "ok", "window_days": window_days, "exclude_lapsed": exclude_lapsed, "counts": counts}

@router.get("/weekly-report", response_model=dict)
def api_weekly_report(
    week_end: str | None = Query(None),
    ensure_snapshot: bool = Query(False),
    rolling_days: int = Query(DEFAULT_ROLLING_DAYS),
    include_nla: bool = Query(False, description="Include NLA people array (heavy)"),
    db: Session = Depends(get_db),
):
    week_end_dt = date.fromisoformat(week_end) if week_end else None
    report = service.build_weekly_report(
        db, week_end=week_end_dt, ensure_snapshot=ensure_snapshot,
        rolling_days=rolling_days, include_nla=include_nla
    )
    return {"status": "ok", **report}

@router.get("/cadences", response_model=dict)
def api_list_cadences(
    signal: str = Query(..., pattern="^(attend|give)$"),
    bucket: str | None = Query(None, pattern="^(weekly|biweekly|monthly|6weekly|irregular)$"),
    exclude_lapsed: bool = Query(True),
    q: str | None = Query(None, description="Search name or email (ILIKE)"),
    order_by: str = Query("expected_next_date_asc", pattern="^(expected_next_date_asc|last_seen_desc|missed_cycles_desc|samples_desc)$"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    return service.browse_cadences(
        signal=signal,
        bucket=bucket,
        exclude_lapsed=exclude_lapsed,
        q=q,
        order_by=order_by,
        limit=limit,
        offset=offset,
    )

@router.get("/backdoor/export/downshifts.csv")
def export_downshifts(week_end: str | None = Query(None)):
    return exports.export_downshifts_csv(week_end)

@router.get("/backdoor/export/nla.csv")
def export_nla(week_end: str | None = Query(None)):
    return exports.export_nla_csv(week_end=week_end)

@router.get("/person/{person_id}", response_model=dict)
def api_person_cadence(person_id: str, days: int = Query(180, ge=30, le=730)):
    return service.person_detail(person_id, days=days)
