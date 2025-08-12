# app/planning_center/giving.py
from __future__ import annotations

import logging
import time
from decimal import Decimal
from datetime import date, timedelta, datetime, time as dtime, timezone
from typing import Dict, Tuple

from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session

from app.config import settings
from app.db import get_conn, get_db
from app.utils.common import (
    CENTRAL_TZ,
    get_previous_week_dates_cst,
    paginate_next_links,
)
from app.planning_center.oauth_routes import get_pco_headers

router = APIRouter(prefix="/planning-center/giving", tags=["Planning Center"])
log = logging.getLogger(__name__)

# ---------------------------
# Tunables
# ---------------------------
MAX_PER_PAGE = 100

# JSON:API date field to filter on (PCO UI commonly uses received_at)
DATE_FIELD = "received_at"  # could be "completed_at" if you prefer

# "gross" (default): sum General-fund designations
# "net": subtract proportional fee share from the General slice
DEFAULT_TOTAL_MODE = getattr(settings, "GIVING_TOTAL_MODE", "gross").lower()


def _base_url() -> str:
    return getattr(
        settings,
        "PLANNING_CENTER_BASE_URL",
        "https://api.planningcenteronline.com",
    ).rstrip("/")


def upsert_f_giving_person_week(rows: list[tuple]) -> int:
    """
    rows: (person_id, week_start, week_end, amount_cents_gross, amount_cents_net, gift_count, campus_id)
    Conflict key: (person_id, week_end)
    """
    if not rows:
        return 0
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.executemany(
            """
            INSERT INTO f_giving_person_week
              (person_id, week_start, week_end, amount_cents_gross, amount_cents_net, gift_count, campus_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (person_id, week_end) DO UPDATE SET
              week_start = EXCLUDED.week_start,
              amount_cents_gross = EXCLUDED.amount_cents_gross,
              amount_cents_net   = EXCLUDED.amount_cents_net,
              gift_count         = EXCLUDED.gift_count,
              campus_id          = COALESCE(f_giving_person_week.campus_id, EXCLUDED.campus_id)
            """,
            rows
        )
        n = cur.rowcount
        conn.commit()
        return n
    finally:
        cur.close()
        conn.close()


def _general_fund_id_str() -> str:
    fid = getattr(settings, "GENERAL_GIVING_FUND_ID", None)
    if fid is None:
        raise RuntimeError("GENERAL_GIVING_FUND_ID is required in settings.")
    return str(fid)


def _extract_general_designation_cents(item: dict, inc: dict, fund_id: str) -> int:
    """Sum designation amounts (in cents) that target the General fund."""
    rels = (item.get("relationships") or {})
    d_refs = ((rels.get("designations") or {}).get("data") or [])
    total = 0
    for ref in d_refs:
        des = inc.get((ref.get("type"), ref.get("id")))
        if not des:
            continue
        da = (des.get("attributes") or {})
        amt = int(da.get("amount_cents") or 0)
        f_id = da.get("fund_id") or (
            ((des.get("relationships") or {}).get("fund") or {}).get("data") or {}
        ).get("id")
        if f_id and str(f_id) == fund_id:
            total += amt
    return total


def _fee_share_for_general(
    donation_total_cents: int, donation_fee_cents: int, fee_covered: bool, general_cents: int
) -> int:
    """Allocate fee proportionally to the General slice of the donation."""
    if fee_covered or donation_total_cents <= 0 or donation_fee_cents <= 0 or general_cents <= 0:
        return 0
    # floor to avoid rounding up pennies
    return (donation_fee_cents * general_cents) // donation_total_cents


def _week_boundaries_to_utc_iso(week_start: date, week_end: date) -> Tuple[str, str]:
    """
    Convert CST week [Mon..Sun] to UTC closed-open ISO boundaries for JSON:API:
      [week_startT00:00:00 CST, week_end+1 T00:00:00 CST) -> 'Z' ISO strings
    """
    start_local = datetime.combine(week_start, dtime(0, 0), tzinfo=CENTRAL_TZ)
    end_local = datetime.combine(week_end + timedelta(days=1), dtime(0, 0), tzinfo=CENTRAL_TZ)
    start_iso = start_local.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    end_iso = end_local.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return start_iso, end_iso


def fetch_giving_data(
    week_start: date,
    week_end: date,
    db: Session,
    *,
    mode: str = DEFAULT_TOTAL_MODE,
    debug: bool = False,
) -> Tuple[int, int, Dict]:
    """
    Pull donations that touch the General fund within [week_start, week_end] (CST Mon..Sun),
    using JSON:API UTC ISO boundaries derived from CST midnights.

    Returns:
      total_cents, giving_units, debug_info

    Side effect:
      upserts per-person weekly rows into f_giving_person_week (gross/net/count).
    """
    headers = get_pco_headers(db)
    base_url = f"{_base_url()}/giving/v2/donations"
    fund_id = _general_fund_id_str()

    # Build CST → UTC boundaries for the whole week
    start_iso, next_day_iso = _week_boundaries_to_utc_iso(week_start, week_end)

    params = {
        # Local-accurate window (converted to UTC)
        f"where[{DATE_FIELD}][gte]": start_iso,
        f"where[{DATE_FIELD}][lt]":  next_day_iso,

        "per_page": MAX_PER_PAGE,
        "sort": f"-{DATE_FIELD}",

        # lean payloads
        "fields[donations]": "amount_cents,received_at,completed_at,created_at,fee_cents,fee_covered,refunded,payment_status",
        "fields[designations]": "amount_cents,fund_id",
        "include": "person,designations,designations.fund",
    }
    # NOTE: no 'filter[fund_id]' and no 'filter[succeeded]'; we enforce those in code to match the UI.

    dbg = {
        "seen": 0,
        "kept": 0,
        "skipped_unsuccessful": 0,
        "skipped_refunded_now": 0,
        "skipped_no_general_designation": 0,
        "fee_share_cents": 0,
        "pages": 0,
    }

    total_general_cents = 0

    # per-person weekly rollups (General slice)
    per_person_gross: Dict[str, int] = {}
    per_person_net:   Dict[str, int] = {}
    per_person_count: Dict[str, int] = {}

    page_num = 0
    t0 = time.perf_counter()

    try:
        for page in paginate_next_links(base_url, headers=headers, params=params):
            page_num += 1
            items = page.get("data") or []
            included = page.get("included") or []
            inc = {(i.get("type"), i.get("id")): i for i in included}

            for item in items:
                dbg["seen"] += 1
                attrs = (item.get("attributes") or {})

                # Skip failed/voided/refunded (UI shows "+ failed/refunded" separately)
                if attrs.get("refunded"):
                    dbg["skipped_refunded_now"] += 1
                    continue
                status = (attrs.get("payment_status") or "").lower()
                if status in ("failed", "voided", "refunded"):
                    dbg["skipped_unsuccessful"] += 1
                    continue

                # General slice for this donation (donation may have multiple funds)
                gen_cents = _extract_general_designation_cents(item, inc, fund_id)
                if gen_cents <= 0:
                    dbg["skipped_no_general_designation"] += 1
                    continue

                # Accumulate gross General
                total_general_cents += gen_cents

                # Proportional fee share → net
                donation_total = int(attrs.get("amount_cents") or 0)
                donation_fee   = abs(int(attrs.get("fee_cents") or 0))
                fee_covered    = bool(attrs.get("fee_covered"))
                fee_share      = _fee_share_for_general(donation_total, donation_fee, fee_covered, gen_cents)
                dbg["fee_share_cents"] += fee_share
                net_cents = max(gen_cents - fee_share, 0)

                # Donor person (may be missing)
                person = ((item.get("relationships") or {}).get("person") or {}).get("data") or {}
                pid = person.get("id")
                if pid:
                    per_person_gross[pid] = per_person_gross.get(pid, 0) + gen_cents
                    # when mode="gross" we still store net as the *net of the donation* (for completeness)
                    per_person_net[pid]   = per_person_net.get(pid, 0) + (net_cents if mode == "net" else gen_cents)
                    per_person_count[pid] = per_person_count.get(pid, 0) + 1

                dbg["kept"] += 1

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"PCO donations fetch failed: {e}")

    dbg["pages"] = page_num
    if debug:
        dbg["donors_count_keys"] = len(per_person_count)
        dbg["gifts_with_person_sum"] = sum(per_person_count.values())
        log.info("[giving] fetched week donations in %.2fs over %s pages", time.perf_counter() - t0, dbg["pages"])
        log.info(
            "[giving][debug] totals: %s",
            {"total_general_cents": total_general_cents, "fee_share_cents": dbg["fee_share_cents"], "donors_with_activity": len(per_person_count)},
        )

    # Build and upsert the per-person weekly rows
    rows = []
    for pid in per_person_count.keys():
        gross = per_person_gross.get(pid, 0)
        # if mode=gross, we still keep net column equal to gross for now (you can switch later)
        net = per_person_net.get(pid, 0) if mode == "net" else gross
        cnt   = per_person_count.get(pid, 0)
        rows.append((pid, week_start, week_end, gross, net, cnt, None))  # campus_id=None (single campus)

    affected = upsert_f_giving_person_week(rows)
    log.info("[giving] f_giving_person_week upserted=%s for week_end=%s (donors=%s)", affected, week_end, len(rows))

    # Unique donors with positive activity (General slice > 0)
    giving_units = len(per_person_count)

    # Weekly total to return
    total_cents = (sum(per_person_net.values()) if mode == "net" else total_general_cents)

    return total_cents, giving_units, dbg


def insert_giving_summary_to_db(
    week_start: date, week_end: date, total_giving: Decimal, giving_units: int
) -> None:
    """Idempotent upsert of the weekly summary row."""
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO weekly_giving_summary
                      (week_start, week_end, total_giving, giving_units)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (week_start) DO UPDATE SET
                      week_end      = EXCLUDED.week_end,
                      total_giving  = EXCLUDED.total_giving,
                      giving_units  = EXCLUDED.giving_units;
                    """,
                    (week_start, week_end, total_giving, giving_units),
                )
    finally:
        conn.close()


@router.get("/weekly-summary")
def weekly_summary(
    debug: bool = Query(False),
    mode: str = Query(DEFAULT_TOTAL_MODE, pattern="^(gross|net)$", description='Total mode: "gross" or "net"'),
    start: str | None = Query(None, description="Override week_start (YYYY-MM-DD)"),
    end: str | None = Query(None, description="Override week_end (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
):
    """
    Returns weekly giving totals for the *General* fund (settings.GENERAL_GIVING_FUND_ID).
    - Default = previous full Mon→Sun (CST) via get_previous_week_dates_cst().
    - mode=gross (default) sums General-designated amounts.
    - mode=net subtracts proportional fee share from the General slice.

    Side effect: upserts per-person rows to f_giving_person_week for this week.
    """
    if start and end:
        week_start = date.fromisoformat(start)
        week_end = date.fromisoformat(end)
    else:
        raw_start, raw_end = get_previous_week_dates_cst()
        week_start = date.fromisoformat(raw_start)
        week_end = date.fromisoformat(raw_end)

    total_cents, units, dbg = fetch_giving_data(week_start, week_end, db, mode=mode, debug=debug)
    total_amount = Decimal(total_cents) / Decimal(100)

    # Save (do not fail API if persistence errors)
    try:
        insert_giving_summary_to_db(week_start, week_end, total_amount, units)
    except Exception as e:
        log.warning("[giving][persist] upsert failed: %s", e)

    result = {
        "status": "success",
        "week_start": str(week_start),
        "week_end": str(week_end),
        "total_giving": float(total_amount),
        "giving_units": units,
        "mode": mode,
        "date_field": DATE_FIELD,
    }
    if debug:
        result["debug"] = dbg
    return result
