# ============================
# app/planning_center/checkins_location_model/routes.py
# ============================
from __future__ import annotations
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, Optional, List

import httpx
import anyio
from fastapi import APIRouter, Depends, HTTPException, Request, Query
from sqlalchemy.orm import Session

from app.db import get_db
from app.planning_center.oauth_routes import get_pco_headers

from .client import PCOCheckinsClient, acquire
from .ingest import ingest_checkins_payload
from .rollup import rollup_day
from .locations import upsert_locations_from_payload
from app.planning_center.checkins_location_model.ingest import ingest_checkins_payload as _ingest_payload

import inspect, logging

log = logging.getLogger(__name__)


router = APIRouter(prefix="/planning-center/checkins-location", tags=["Planning Center – Checkins by Location"])


# --- OAuth bearer ---
async def _get_pco_bearer(db_sess: Session) -> str:
    headers = await anyio.to_thread.run_sync(get_pco_headers, db_sess)
    auth = headers.get("Authorization", "")
    return auth.split(" ", 1)[1] if auth.lower().startswith("bearer ") else auth


# --- Helpers ---
def _pool_or_conn(request: Request):
    pool = getattr(request.app.state, "db_pool", None)
    if pool is None:
        raise HTTPException(500, "DB pool not configured (app.state.db_pool)")
    return pool


def _iso_day_bounds(d: date) -> tuple[str, str]:
    start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(days=1) - timedelta(microseconds=1)
    return start.isoformat(), end.isoformat()


@router.post("/ingest-day")
async def ingest_for_day(
    request: Request,
    svc_date: Optional[date] = None,
    per_page: int = Query(200, ge=1, le=200),
    skip_raw: bool = Query(False),                 # <-- thread this flag through
    db_sess: Session = Depends(get_db),
) -> Dict[str, Any]:
    db = _pool_or_conn(request)
    if svc_date is None:
        svc_date = date.today()
    gte, lte = _iso_day_bounds(svc_date)

    # Prove which function we’re calling & whether skip_raw is set
    src_file = inspect.getsourcefile(ingest_checkins_payload)
    src_line = inspect.getsourcelines(ingest_checkins_payload)[1]
    log.error("[ROUTE CALL] ingest_checkins_payload from %s#%s skip_raw=%s", src_file, src_line, skip_raw)

    try:
        # IMPORTANT: use your existing OAuth helper (returns headers dict)
        client = PCOCheckinsClient(lambda: get_pco_headers(db_sess))

        placed_total = 0
        unplaced_total = 0
        async for page in client.paginate_check_ins(created_at_gte=gte, created_at_lte=lte, per_page=per_page):
            async with acquire(db) as conn:
                p, u = await ingest_checkins_payload(conn, page, client=client, skip_raw=skip_raw)
                placed_total += p
                unplaced_total += u

        return {"ok": True, "date": str(svc_date), "placed": placed_total, "unplaced": unplaced_total}
    except HTTPException:
        raise
    except Exception as e:
        # Surface the real error so we can see what’s wrong
        raise HTTPException(502, f"ingest failed: {e}")


@router.post("/ingest-range")
async def ingest_range(
    request: Request,
    start: date,
    end: date,
    per_page: int = 200,
    db_sess: Session = Depends(get_db),
) -> Dict[str, Any]:
    cur = start
    results = []
    while cur <= end:
        res = await ingest_for_day(request, svc_date=cur, per_page=per_page, db_sess=db_sess)
        results.append(res)
        cur = cur + timedelta(days=1)
    return {"ok": True, "days": results}


@router.post("/sync-locations")
async def sync_locations(
    request: Request,
    event_id: Optional[str] = None,
    per_page: int = 200,
    db_sess: Session = Depends(get_db),
) -> Dict[str, Any]:
    db = _pool_or_conn(request)

    async def get_bearer():
        return await _get_pco_bearer(db_sess)

    client = PCOCheckinsClient(get_bearer)
    processed = 0
    async for page in client.paginate_locations(event_id=event_id, per_page=per_page):
        async with acquire(db) as conn:
            await upsert_locations_from_payload(conn, page)
            processed += len(page.get("included") or [])
    return {"ok": True, "included_processed": processed}


@router.get("/health")
async def health(request: Request) -> Dict[str, Any]:
    db = _pool_or_conn(request)
    try:
        async with acquire(db) as conn:
            await conn.fetchval("SELECT 1")
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "detail": str(e)}
    

import inspect
from . import ingest as ingest_mod

@router.get("/debug-whoami")
async def debug_whoami() -> Dict[str, str]:
    return {
        "routes_file": __file__,
        "ingest_file": inspect.getsourcefile(ingest_mod) or "unknown",
        "ingest_version": getattr(ingest_mod, "INGEST_VERSION", "unset"),
    }


@router.post("/ingest-day-dryrun")
async def ingest_day_dryrun(
    request: Request,
    svc_date: Optional[date] = None,
    per_page: int = 50,
    sample: int = 12,
    db_sess: Session = Depends(get_db),
) -> Dict[str, Any]:
    if svc_date is None:
        svc_date = date.today()
    gte, lte = _iso_day_bounds(svc_date)

    client = PCOCheckinsClient(lambda: get_pco_headers(db_sess))
    findings: List[Dict[str, Any]] = []
    async for page in client.paginate_check_ins(created_at_gte=gte, created_at_lte=lte, per_page=per_page):
        included = page.get("included") or []
        idx = {((obj.get("type") or ""), (obj.get("id") or "")): obj for obj in included}
        for row in page.get("data") or []:
            if (row.get("type") or "").lower() != "checkin":
                continue
            a = row.get("attributes") or {}
            r = row.get("relationships") or {}
            checkin_id = row.get("id")
            evt_time_id = ((r.get("event_time") or {}).get("data") or {}).get("id")
            evt_time = idx.get(("EventTime", evt_time_id)) if evt_time_id else None

            from .derive import _ts, derive_service_bucket
            ts = _ts(a.get("created_at") or a.get("updated_at"))
            svc = derive_service_bucket(evt_time, ts) if ts else None

            findings.append({
                "checkin_id": checkin_id,
                "service_bucket_type": None if svc is None else type(svc).__name__,
                "service_bucket_preview": None if svc is None else (str(svc)[:80] + ("..." if len(str(svc)) > 80 else "")),
                "has_evt_time": bool(evt_time),
            })
            if len(findings) >= sample:
                return {"ok": True, "date": str(svc_date), "sample": findings}
    return {"ok": True, "date": str(svc_date), "sample": findings}