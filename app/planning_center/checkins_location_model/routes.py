# app/planning_center/checkins_location_model/routes.py
from __future__ import annotations

from datetime import date as _date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional, Tuple
import logging

import anyio
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from app.db import get_db
from app.planning_center.oauth_routes import get_pco_headers

from .client import PCOCheckinsClient, acquire  # acquire(pool) -> asyncpg.Connection context
from .ingest import ingest_checkins_payload
from .locations import upsert_locations_from_payload
from .rollup import rollup_day  # <- our rollup function

log = logging.getLogger(__name__)
router = APIRouter(prefix="/planning-center/checkins-location", tags=["planning-center:checkins-location"])

# ---- Single source of truth for includes (drop 'location_label'; not supported) ----
CHECKINS_INCLUDE = "person,locations,event_times"

# ---- Time helpers ----
CST = ZoneInfo("America/Chicago")

def _as_date_or_last_sunday(svc_date: Optional[str]) -> _date:
    if svc_date:
        try:
            return datetime.fromisoformat(svc_date).date()
        except ValueError:
            raise HTTPException(status_code=400, detail="svc_date must be ISO format (YYYY-MM-DD)")
    today_cst = datetime.now(CST).date()
    return today_cst - timedelta(days=(today_cst.weekday() + 1) % 7)

async def _get_oauth_headers_async(db_sess: Session) -> dict:
    # get_pco_headers is sync (SQLAlchemy); run it off the event loop
    return await anyio.to_thread.run_sync(get_pco_headers, db_sess)

def _get_pool_or_500(request: Request):
    pool = getattr(request.app.state, "db_pool", None)
    if pool is None:
        raise HTTPException(500, "DB pool not configured on app.state.db_pool")
    return pool

def _cst_day_bounds_utc(d: _date) -> Tuple[str, str]:
    start_cst = datetime.combine(d, time(0, 0), tzinfo=CST)
    end_cst   = datetime.combine(d, time(23, 59, 59), tzinfo=CST)
    s = start_cst.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    e = end_cst.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return s, e

def _cst_pm_bounds_utc(d: _date) -> Tuple[str, str]:
    pm_start_cst = datetime.combine(d, time(15, 0), tzinfo=CST)  # 3:00 PM CST
    pm_end_cst   = datetime.combine(d, time(18, 0), tzinfo=CST)  # 6:00 PM CST
    s = pm_start_cst.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    e = pm_end_cst.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return s, e

@router.post("/ingest-day", response_model=dict)
async def ingest_day(
    request: Request,
    svc_date: Optional[str] = Query(None, description="ISO date (YYYY-MM-DD). Defaults to last Sunday (CST)."),
    db: Session = Depends(get_db),
):
    d = _as_date_or_last_sunday(svc_date)
    s_all, e_all = _cst_day_bounds_utc(d)

    headers = await _get_oauth_headers_async(db)
    client = PCOCheckinsClient(lambda: headers)
    pool = _get_pool_or_500(request)

    total_placed = 0
    total_unplaced = 0

    async with acquire(pool) as conn:
        # Single all-day pass
        async for payload in client.paginate_check_ins(
            created_at_gte=s_all,
            created_at_lte=e_all,
            include=CHECKINS_INCLUDE,
            per_page=200,
        ):
            placed, unplaced = await ingest_checkins_payload(conn, payload, client=client)
            total_placed += placed
            total_unplaced += unplaced

        # Return the *actual* distinct count for that day
        rec = await conn.fetchrow(
            """
            SELECT COUNT(*) AS c
            FROM pco_checkins_raw
            WHERE (created_at_pco AT TIME ZONE 'America/Chicago')::date = $1::date
            """,
            d,
        )
        day_total = int(rec["c"])

    return {
        "ok": True,
        "date": str(d),
        "raw_rows_total_for_date": day_total,
        "unplaced_logged": total_unplaced,
        "include": CHECKINS_INCLUDE,
    }


@router.post("/sync-locations", response_model=dict)
async def sync_locations(
    request: Request,
    svc_date: Optional[str] = Query(None, description="ISO date (YYYY-MM-DD). Defaults to last Sunday (CST)."),
    event_id: Optional[str] = Query(None, description="Optional PCO Event ID to filter locations"),
    db: Session = Depends(get_db),
):
    """
    Sync the PCO locations tree and refresh the closure table (pco_location_paths).
    Uses paginate_locations + upsert per page.
    """
    _ = _as_date_or_last_sunday(svc_date)  # date not needed here; keep signature consistent

    headers = await _get_oauth_headers_async(db)
    client = PCOCheckinsClient(lambda: headers)

    pool = _get_pool_or_500(request)
    processed_included = 0

    async for page in client.paginate_locations(event_id=event_id, per_page=200, include="parent,event"):
        async with acquire(pool) as conn:
            await upsert_locations_from_payload(conn, page)
        processed_included += len(page.get("included") or [])

    return {"ok": True, "included_processed": processed_included}

@router.post("/rollup-day", response_model=dict)
async def rollup_day_endpoint(
    request: Request,
    svc_date: Optional[str] = Query(None, description="ISO date (YYYY-MM-DD). Defaults to last Sunday (CST)."),
):
    """
    Compute set-based rollups for the given date into attendance_by_location_daily.
    """
    d = _as_date_or_last_sunday(svc_date)
    pool = _get_pool_or_500(request)

    async with acquire(pool) as conn:
        rows = await rollup_day(conn, d)

    return {"ok": True, "date": str(d), "rows_inserted": rows}
