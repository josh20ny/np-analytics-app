# app/planning_center/giving.py

from __future__ import annotations

import logging
import time
from decimal import Decimal
from datetime import date, timedelta
from typing import Dict, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session

from app.config import settings
from app.db import get_conn, get_db
from app.google_sheets import get_previous_week_dates
from app.planning_center.oauth_routes import get_pco_headers

router = APIRouter(prefix="/planning-center/giving", tags=["Planning Center"])
log = logging.getLogger(__name__)

# ---------------------------
# Tunables
# ---------------------------
REQUEST_TIMEOUT_S = 30
MAX_PER_PAGE = 100

# Use documented JSON:API "where[received_at][...]" range filters
DATE_FIELD = "received_at"  # change to "completed_at" or "created_at" if you prefer

# "gross" (default): sum of General designations only
# "net": subtract proportional fee share from the General portion
DEFAULT_TOTAL_MODE = getattr(settings, "GIVING_TOTAL_MODE", "gross").lower()


def _base_url() -> str:
    return getattr(
        settings,
        "PLANNING_CENTER_BASE_URL",
        "https://api.planningcenteronline.com",
    ).rstrip("/")


def _general_fund_id_str() -> str:
    fid = getattr(settings, "GENERAL_GIVING_FUND_ID", None)
    if fid is None:
        raise RuntimeError("GENERAL_GIVING_FUND_ID is required in settings.")
    return str(fid)


def _requests_session() -> requests.Session:
    """Session with retry/backoff for 429/5xx, connection pooling."""
    retry = Retry(
        total=6,
        read=6,
        connect=4,
        backoff_factor=0.6,               # ~0.6, 1.2, 2.4, ...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=16)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _extract_general_designation_cents(
    item: dict, inc: dict, fund_id: str
) -> int:
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


def fetch_giving_data(
    week_start: date,
    week_end: date,
    db: Session,
    *,
    mode: str = DEFAULT_TOTAL_MODE,
    debug: bool = False,
) -> Tuple[int, int, Dict]:
    """
    Pull donations that *touch* the General fund within [week_start, week_end], inclusive,
    using JSON:API date-time range filters:

      where[received_at][gte] = week_startT00:00:00Z
      where[received_at][lt]  = (week_end + 1 day)T00:00:00Z

    Returns:
      total_cents, giving_units, debug_info
    """
    headers = get_pco_headers(db)
    base_url = f"{_base_url()}/giving/v2/donations"
    fund_id = _general_fund_id_str()
    session = _requests_session()

    # Build documented date filters (closed-open interval)
    start_iso = f"{week_start.isoformat()}T00:00:00Z"
    next_day_iso = f"{(week_end + timedelta(days=1)).isoformat()}T00:00:00Z"

    params = {
        "filter[fund_id]": fund_id,    # only donations that include General in any designation
        "filter[succeeded]": "true",   # succeeded only

        # ✅ JSON:API date range (no undocumented params)
        f"where[{DATE_FIELD}][gte]": start_iso,
        f"where[{DATE_FIELD}][lt]":  next_day_iso,

        "per_page": MAX_PER_PAGE,
        "sort": f"-{DATE_FIELD}",

        # keep payloads lean
        "fields[donations]": "amount_cents,received_at,completed_at,created_at,fee_cents,fee_covered,refunded,payment_status",
        "fields[designations]": "amount_cents",
        "include": "person,designations,designations.fund",
    }

    # Debug counters (returned only when debug=True)
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
    donor_net: Dict[str, int] = {}

    url = base_url
    page = 1
    t0 = time.perf_counter()

    while url:
        if debug:
            log.info("[giving] ▶ page=%s GET %s", page, url)

        r = session.get(
            url,
            headers=headers,
            params=params if page == 1 else None,  # only pass params on first page
            timeout=REQUEST_TIMEOUT_S,
        )
        if r.status_code != 200:
            raise HTTPException(status_code=r.status_code, detail=f"Error fetching giving data: {r.text}")

        payload = r.json() or {}
        items = payload.get("data") or []
        included = payload.get("included") or []
        inc = {(i.get("type"), i.get("id")): i for i in included}

        for item in items:
            dbg["seen"] += 1
            attrs = (item.get("attributes") or {})

            # safety: skip anything not marked succeeded or marked refunded
            if attrs.get("payment_status") and attrs.get("payment_status") != "succeeded":
                dbg["skipped_unsuccessful"] += 1
                continue
            if attrs.get("refunded"):
                dbg["skipped_refunded_now"] += 1
                continue

            # compute the General slice for this donation
            gen_cents = _extract_general_designation_cents(item, inc, fund_id)
            if gen_cents <= 0:
                dbg["skipped_no_general_designation"] += 1
                continue

            # accumulate gross General
            total_general_cents += gen_cents

            # compute donor "net for unit count" depending on mode
            donation_total = int(attrs.get("amount_cents") or 0)
            donation_fee = abs(int(attrs.get("fee_cents") or 0))
            fee_covered = bool(attrs.get("fee_covered"))
            net_cents = gen_cents
            if mode == "net":
                fee_share = _fee_share_for_general(donation_total, donation_fee, fee_covered, gen_cents)
                dbg["fee_share_cents"] += fee_share
                net_cents = max(gen_cents - fee_share, 0)

            # donor id
            person = ((item.get("relationships") or {}).get("person") or {}).get("data") or {}
            pid = person.get("id")
            if pid:
                donor_net[pid] = donor_net.get(pid, 0) + net_cents

            dbg["kept"] += 1

        url = (payload.get("links") or {}).get("next")
        page += 1

    dbg["pages"] = page - 1
    if debug:
        log.info(
            "[giving] fetched week donations in %.2fs over %s pages",
            time.perf_counter() - t0,
            dbg["pages"],
        )
        log.info(
            "[giving][debug] totals: %s",
            {
                "total_general_cents": total_general_cents,
                "fee_share_cents": dbg["fee_share_cents"],
                "donors_with_activity": len(donor_net),
            },
        )

    # Unique donors with positive activity for the week
    giving_units = sum(1 for v in donor_net.values() if v > 0)

    # Return cents according to mode: gross (default) or net
    if mode == "net":
        # We already subtracted fees into donor_net; recompute the matching total from those
        total_cents = sum(donor_net.values())
    else:
        total_cents = total_general_cents

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
    # Pydantic v2 uses "pattern" instead of "regex"
    mode: str = Query(DEFAULT_TOTAL_MODE, pattern="^(gross|net)$", description='Total mode: "gross" or "net"'),
    start: str | None = Query(None, description="Override week_start (YYYY-MM-DD)"),
    end: str | None = Query(None, description="Override week_end (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
):
    """
    Returns weekly giving totals for the *General* fund.
    - Default is the previous full Mon→Sun week derived by `get_previous_week_dates()`.
    - `mode=gross` (default) sums General-designated amounts.
    - `mode=net` subtracts a proportional fee share from the General slice.
    """
    if start and end:
        week_start = date.fromisoformat(start)
        week_end = date.fromisoformat(end)
    else:
        raw_start, raw_end = get_previous_week_dates()
        week_start = date.fromisoformat(raw_start)
        week_end = date.fromisoformat(raw_end)

    total_cents, units, dbg = fetch_giving_data(week_start, week_end, db, mode=mode, debug=debug)

    total_amount = Decimal(total_cents) / Decimal(100)

    # Save, but don’t break the API if persistence fails
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
