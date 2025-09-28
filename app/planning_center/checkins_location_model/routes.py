# app/planning_center/checkins_location_model/routes.py
from __future__ import annotations

from datetime import date as _date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, Literal
import logging

import anyio
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Path
from sqlalchemy.orm import Session

from app.db import get_db
from app.planning_center.oauth_routes import get_pco_headers
from app.utils.common import is_allowed_bucket

from .client import PCOCheckinsClient, acquire  # acquire(pool) -> asyncpg.Connection context
from .ingest import ingest_checkins_payload
from .locations import upsert_locations_from_payload
from .rollup import rollup_day  # <- our rollup function
from .audit import write_skip_audit
from .person_facts import build_person_fact_rows
from .legacy_bridge import write_legacy_slim


log = logging.getLogger(__name__)
router = APIRouter(prefix="/planning-center/checkins-location", tags=["planning-center:checkins-location"])

# ---- Single source of truth for includes (drop 'location_label'; not supported) ----
CHECKINS_INCLUDE = "person,locations,event_times"

# ---- Time helpers ----
CST = ZoneInfo("America/Chicago")

SERVICE_LABEL_BY_CODE = {"930": "9:30 AM", "1100": "11:00 AM", "1630": "4:30 PM"}

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

@router.post("/ingest-day", response_model=dict)
async def ingest_day(
    request: Request,
    svc_date: Optional[str] = Query(None, description="ISO date (YYYY-MM-DD). Defaults to last Sunday (CST)."),
    write_person_facts: bool = Query(True),
    log_person_facts: bool = Query(False),
    db: Session = Depends(get_db),
):
    d = _as_date_or_last_sunday(svc_date)
    s_all, e_all = _cst_day_bounds_utc(d)

    headers = await _get_oauth_headers_async(db)
    client = PCOCheckinsClient(lambda: headers)
    pool = _get_pool_or_500(request)

    total_placed = 0
    total_unplaced = 0
    person_facts_attempted = 0
    person_facts_upserted = 0  # executemany doesn't give rowcount

    async with acquire(pool) as conn:
        # 1) Pull check-ins for the day into raw table
        async for payload in client.paginate_check_ins(
            created_at_gte=s_all,
            created_at_lte=e_all,
            include=CHECKINS_INCLUDE,
            per_page=200,
        ):
            placed, unplaced = await ingest_checkins_payload(conn, payload, client=client)
            total_placed += placed
            total_unplaced += unplaced

        # 2) Distinct row count for the day
        day_total_rec = await conn.fetchrow(
            """
            SELECT COUNT(*) AS c
            FROM pco_checkins_raw pr
            WHERE (pr.created_at_pco AT TIME ZONE 'America/Chicago')::date = $1::date
            """,
            d,
        )
        day_total = int(day_total_rec["c"])

        # 3) Build normalized, de-duplicated facts from pco_checkins_raw
        #    Guardrails:
        #      - person_id present and not empty
        #      - person exists in pco_people (FK safety)
        rows = await conn.fetch(
            """
            WITH day_rows AS (
            SELECT
                pr.person_id::text                AS person_id,
                ($1)::date                        AS svc_date,
                pr.ministry_key::text             AS ministry_raw,
                pr.service_bucket::text           AS service_raw,
                pr.event_id::text                 AS event_id,
                NULL::text                        AS campus_id,
                pr.created_at_pco                 AS created_at_pco_utc
            FROM pco_checkins_raw pr
            WHERE (pr.created_at_pco AT TIME ZONE 'America/Chicago')::date = $1::date
                AND pr.person_id IS NOT NULL
                AND trim(pr.person_id::text) <> ''
                AND pr.ministry_key IS NOT NULL
                AND pr.service_bucket IS NOT NULL
                AND EXISTS (
                SELECT 1 FROM pco_people pp
                WHERE pp.person_id::text = pr.person_id::text
                )
            ),
            normalized AS (
            SELECT
                person_id,
                svc_date,
                -- ministry normalization
                CASE
                WHEN ministry_raw IN ('Waumba Land','WaumbaLand','Waumba land','WaumbaLand ') THEN 'Waumba Land'
                WHEN ministry_raw IN ('Upstreet','UpStreet','Up Street')                     THEN 'UpStreet'
                WHEN ministry_raw IN ('Transit ','Transit')                                  THEN 'Transit'
                WHEN ministry_raw IN ('Inside Out','InsideOut','InsideOut ')                 THEN 'InsideOut'
                ELSE ministry_raw
                END AS ministry_norm,

                -- service time normalization
                CASE
                WHEN service_raw IN ('930','9:30','9:30AM','9:30 am','09:30','09:30 AM')     THEN '9:30 AM'
                WHEN service_raw IN ('1100','11:00','11:00AM','11:00 am')                    THEN '11:00 AM'
                WHEN service_raw IN ('1630','4:30','4:30PM','4:30 pm','16:30','16:30 PM')   THEN '4:30 PM'
                ELSE service_raw
                END AS service_norm,

                event_id,
                campus_id,
                created_at_pco_utc
            FROM day_rows
            ),
            ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                PARTITION BY svc_date, person_id, ministry_norm, service_norm
                ORDER BY created_at_pco_utc ASC
                ) AS rn
            FROM normalized
            )
            SELECT
            person_id,
            svc_date,
            ministry_norm   AS ministry,
            service_norm    AS service_time,
            event_id,
            campus_id,
            (created_at_pco_utc AT TIME ZONE 'UTC')::timestamp AS created_at_utc
            FROM ranked
            WHERE rn = 1
            """,
            d,
)


        normalized_checkins = [dict(r) for r in rows]

        # ---- Bucket normalization + audit -----------------------------------
        # Keep only checkins whose (ministry, service_bucket) is allowed.
        # Everything else gets logged to skip audit.
        filtered: list[dict] = []
        skip_rows: list[dict] = []
        for r in normalized_checkins:
            if is_allowed_bucket(r["ministry"], r["service_time"]):
                filtered.append(r)
            else:
                skip_rows.append({
                    "reason": "invalid_service_bucket",
                    "ministry": r["ministry"],
                    "service_time": r["service_time"],
                    "person_id": r["person_id"],
                    "event_id": r.get("event_id"),
                    "campus_id": r.get("campus_id"),
                })

        # (Optional) write to skip audit; won't break if schema differs
        invalid_bucket_skips_written = await write_skip_audit(conn, d, skip_rows)
        invalid_bucket_skips = len(skip_rows)

        normalized_checkins = filtered

        # How many would have been candidates but are missing from pco_people? (for logs/observability)
        missing_people_rec = await conn.fetchrow(
            """
            SELECT COUNT(DISTINCT pr.person_id)::int AS missing_count
            FROM pco_checkins_raw pr
            WHERE (pr.created_at_pco AT TIME ZONE 'America/Chicago')::date = $1::date
              AND pr.person_id IS NOT NULL
              AND trim(pr.person_id::text) <> ''
              AND pr.ministry_key IS NOT NULL
              AND pr.service_bucket IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM pco_people pp
                WHERE pp.person_id::text = pr.person_id::text
              )
            """,
            d,
        )
        missing_people_count = int(missing_people_rec["missing_count"])

        # 4) Write person facts (exactly as legacy table expects)
        if write_person_facts and normalized_checkins:
            pf_rows = build_person_fact_rows(normalized_checkins)
            person_facts_attempted = len(pf_rows)

            insert_sql = """
            INSERT INTO f_checkins_person
              (person_id, svc_date, service_time, ministry, event_id, campus_id, created_at_utc)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (person_id, svc_date, ministry, service_time)
            DO UPDATE SET
              -- keep earliest created_at_utc
              created_at_utc = LEAST(f_checkins_person.created_at_utc, EXCLUDED.created_at_utc),
              -- prefer latest non-null event/campus (falls back to existing when EXCLUDED is null)
              event_id = COALESCE(EXCLUDED.event_id, f_checkins_person.event_id),
              campus_id = COALESCE(EXCLUDED.campus_id, f_checkins_person.campus_id)
            """

            await conn.executemany(insert_sql, pf_rows)
            person_facts_upserted = person_facts_attempted

            if log_person_facts:
                log.info("f_checkins_person upserts for %s: attempted=%d", d, person_facts_attempted)

    return {
        "ok": True,
        "date": str(d),
        "raw_rows_total_for_date": day_total,
        "unplaced_logged": total_unplaced,
        "include": CHECKINS_INCLUDE,
        "person_facts_attempted": person_facts_attempted,
        "person_facts_upserted": person_facts_upserted,
        "missing_people_skipped": missing_people_count,
        "invalid_bucket_skips": invalid_bucket_skips,
        "invalid_bucket_skips_written": invalid_bucket_skips_written,
    }

@router.post("/rollup-day", response_model=dict)
async def rollup_day_endpoint(
    request: Request,
    svc_date: Optional[str] = Query(None, description="ISO date (YYYY-MM-DD). Defaults to last Sunday (CST)."),
    write_legacy: bool = Query(True, description="Also write slim legacy totals (bridge)"),
    dedupe_scope: Literal["service","day"] = "service", #Optional Deduping checkins byt SERVICE (true capacity) or DAY (true uniqie people)
    log_duplicates: bool = True,
):
    d = _as_date_or_last_sunday(svc_date)
    pool = _get_pool_or_500(request)

    async with acquire(pool) as conn:
        # Keep these operations on the same connection (and transaction)
        async with conn.transaction():
            # 1) roll up into attendance_by_location_daily
            rows_inserted = await rollup_day(conn, d, dedupe_scope, log_duplicates)

            # 2) optional legacy bridge writes (same connection!)
            if write_legacy:
                await write_legacy_slim(conn, d)

            # 3) build nested JSON for the response
            recs = await conn.fetch(
                """
                SELECT
                  a.ministry_key,
                  a.service_bucket,
                  a.location_id,
                  COALESCE(l.name, CONCAT('Location ', a.location_id::text)) AS location_name,
                  a.total_attendance,
                  a.total_new
                FROM attendance_by_location_daily a
                LEFT JOIN pco_locations l
                  ON l.location_id = a.location_id
                WHERE a.date = $1::date
                ORDER BY a.ministry_key, a.service_bucket, a.total_attendance DESC, a.location_id
                """,
                d,
            )

    # Shape ministries -> services -> locations
    ministries: dict = {}
    for r in recs:
        m = r["ministry_key"]; s = r["service_bucket"]
        ministries.setdefault(m, {"total": 0, "services": {}})
        ministries[m]["services"].setdefault(s, {"total": 0, "locations": []})
        ministries[m]["services"][s]["locations"].append({
            "id": r["location_id"],
            "name": r["location_name"],
            "attendance": r["total_attendance"],
            "new": r["total_new"],
        })
        ministries[m]["total"] += r["total_attendance"]
        ministries[m]["services"][s]["total"] += r["total_attendance"]

    return {
        "ok": True,
        "date": str(d),
        "rows_inserted": rows_inserted,
        "ministries": ministries,
    }

# --- Read-only endpoints for the Assistant ---

@router.get("/day/{svc_date}", response_model=dict)
async def read_day(
    request: Request,
    svc_date: str = Path(..., description="YYYY-MM-DD"),
    ministry: Optional[str] = Query(None, description="Optional ministry filter (e.g., 'UpStreet', 'Transit', 'Waumba Land', 'InsideOut')"),
    include_persons: bool = Query(False, description="Also include person-level facts for the day"),
):
    """
    Read-only: return nested JSON for a given Sunday from precomputed tables.
    Does NOT recompute. Mirrors the shape from /rollup-day.
    """
    d = _as_date_or_last_sunday(svc_date)
    pool = _get_pool_or_500(request)

    async with acquire(pool) as conn:
        # Locations/rollup rows (precomputed)
        params = [d]
        filt = ""
        if ministry:
            filt = " AND a.ministry_key = $2 "
            params.append(ministry)

        recs = await conn.fetch(
            f"""
            SELECT
              a.ministry_key,
              a.service_bucket,
              a.location_id,
              COALESCE(l.name, CONCAT('Location ', a.location_id::text)) AS location_name,
              a.total_attendance,
              a.total_new
            FROM attendance_by_location_daily a
            LEFT JOIN pco_locations l ON l.location_id = a.location_id
            WHERE a.date = $1::date {filt}
            ORDER BY a.ministry_key, a.service_bucket, a.total_attendance DESC, a.location_id
            """,
            *params,
        )

        people = []
        if include_persons:
            # person-level facts (deduped) for that date, optionally filtered
            params_p = [d]
            filt_p = ""
            if ministry:
                filt_p = " AND p.ministry = $2 "
                params_p.append(ministry)

            people = await conn.fetch(
                f"""
                SELECT
                  p.person_id,
                  p.ministry,
                  CASE p.service_time
                    WHEN '930'  THEN '9:30 AM'
                    WHEN '1100' THEN '11:00 AM'
                    WHEN '1630' THEN '4:30 PM'
                    ELSE p.service_time
                  END AS service_time,
                  p.event_id,
                  p.campus_id,
                  p.created_at_utc
                FROM f_checkins_person p
                WHERE p.svc_date = $1::date {filt_p}
                ORDER BY p.ministry, service_time, p.person_id
                """,
                *params_p,
            )

    # Shape nested ministries/services/locations
    ministries: dict = {}
    for r in recs:
        m = r["ministry_key"]; s = r["service_bucket"]
        ministries.setdefault(m, {"total": 0, "services": {}})
        ministries[m]["services"].setdefault(s, {"total": 0, "locations": []})
        ministries[m]["services"][s]["locations"].append({
            "id": r["location_id"],
            "name": r["location_name"],
            "attendance": r["total_attendance"],
            "new": r["total_new"],
        })
        ministries[m]["total"] += r["total_attendance"]
        ministries[m]["services"][s]["total"] += r["total_attendance"]

    resp: dict = {
        "ok": True,
        "date": str(d),
        "ministries": ministries,
    }
    if include_persons:
        resp["persons"] = [
            {
                "person_id": r["person_id"],
                "ministry": r["ministry"],
                "service_time": r["service_time"],
                "event_id": r["event_id"],
                "campus_id": r["campus_id"],
                "created_at_utc": r["created_at_utc"].isoformat() if r["created_at_utc"] else None,
            } for r in people
        ]
    return resp


@router.get("/day/{svc_date}/rows", response_model=dict)
async def read_day_rows(
    request: Request,
    svc_date: str = Path(..., description="YYYY-MM-DD"),
    ministry: Optional[str] = Query(None),
    service: Optional[str] = Query(None, description="Optional service label: '9:30 AM'|'11:00 AM'|'4:30 PM'"),
):
    """
    Read-only: return a FLAT list of all location rows for a Sunday (Assistant-friendly).
    Perfect for “show me all rows” queries. No recompute.
    """
    d = _as_date_or_last_sunday(svc_date)
    pool = _get_pool_or_500(request)

    async with acquire(pool) as conn:
        params = [d]
        filt = []
        if ministry:
            params.append(ministry)
            filt.append(f"a.ministry_key = ${len(params)}")
        if service:
            params.append(service)
            filt.append(f"a.service_bucket = ${len(params)}")
        where_extra = (" AND " + " AND ".join(filt)) if filt else ""

        rows = await conn.fetch(
            f"""
            SELECT
              a.date,
              a.ministry_key,
              a.service_bucket,
              a.location_id,
              COALESCE(l.name, CONCAT('Location ', a.location_id::text)) AS location_name,
              a.total_attendance,
              a.total_new
            FROM attendance_by_location_daily a
            LEFT JOIN pco_locations l ON l.location_id = a.location_id
            WHERE a.date = $1::date {where_extra}
            ORDER BY a.ministry_key, a.service_bucket, a.total_attendance DESC, a.location_id
            """,
            *params,
        )

    return {
        "ok": True,
        "date": str(d),
        "rows": [
            {
                "date": str(r["date"]),
                "ministry": r["ministry_key"],
                "service": r["service_bucket"],
                "location_id": r["location_id"],
                "location": r["location_name"],
                "attendance": r["total_attendance"],
                "new": r["total_new"],
            } for r in rows
        ],
        "count": len(rows),
    }

