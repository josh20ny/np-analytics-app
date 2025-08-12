# app/planning_center/people.py
from __future__ import annotations

from datetime import datetime, date
from typing import Dict, Any, Iterable, List, Tuple, Optional

import logging
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_conn, get_db
from app.utils.common import paginate_next_links   # same helper used elsewhere
from app.planning_center.oauth_routes import get_pco_headers

log = logging.getLogger(__name__)
router = APIRouter(prefix="/planning-center/people", tags=["Planning Center"])

PEOPLE_URL = "https://api.planningcenteronline.com/people/v2/people"

def _as_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        return None

def _upsert_households(rows: Iterable[Tuple[str, Optional[str], Optional[str], Optional[str], Optional[str]]]) -> int:
    """
    rows: (household_id, name, campus_id, created_at_pco, updated_at_pco)
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.executemany(
            """
            INSERT INTO pco_households (household_id, name, campus_id, created_at_pco, updated_at_pco)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (household_id) DO UPDATE SET
              name = EXCLUDED.name,
              campus_id = EXCLUDED.campus_id,
              created_at_pco = COALESCE(pco_households.created_at_pco, EXCLUDED.created_at_pco),
              updated_at_pco = EXCLUDED.updated_at_pco
            """,
            rows,
        )
        n = cur.rowcount
        conn.commit()
        return n
    finally:
        cur.close()
        conn.close()

def _upsert_people(rows: Iterable[Tuple[str, Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]]) -> int:
    """
    rows: (person_id, household_id, first_name, last_name, birthdate, grade, gender,
           email, phone, campus_id, created_at_pco, updated_at_pco)
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.executemany(
            """
            INSERT INTO pco_people
              (person_id, household_id, first_name, last_name, birthdate, grade, gender,
               email, phone, campus_id, created_at_pco, updated_at_pco)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (person_id) DO UPDATE SET
              household_id   = EXCLUDED.household_id,
              first_name     = EXCLUDED.first_name,
              last_name      = EXCLUDED.last_name,
              birthdate      = COALESCE(pco_people.birthdate, EXCLUDED.birthdate),
              grade          = EXCLUDED.grade,
              gender         = EXCLUDED.gender,
              email          = COALESCE(pco_people.email, EXCLUDED.email),
              phone          = COALESCE(pco_people.phone, EXCLUDED.phone),
              campus_id      = EXCLUDED.campus_id,
              created_at_pco = COALESCE(pco_people.created_at_pco, EXCLUDED.created_at_pco),
              updated_at_pco = EXCLUDED.updated_at_pco
            """,
            rows,
        )
        n = cur.rowcount
        conn.commit()
        return n
    finally:
        cur.close()
        conn.close()

@router.get("/sync", summary="Sync People & Households from PCO (with progress logs)")
def sync_people(
    since: Optional[str] = None,   # ISO date (YYYY-MM-DD) for updated_at >= since
    limit: int = 0,                # for testing: stop after N pages (0 = all)
    per_page: int = 200,           # tune page size; 100 or 200 are typical
    batch_pages: int = 5,          # commit every N pages so logs show progress
    db: Session = Depends(get_db),
):
    """
    Full backfill:  /planning-center/people/sync
    Incremental:    /planning-center/people/sync?since=2025-08-01
    Tune runtime:   /planning-center/people/sync?per_page=200&batch_pages=5
    """
    import time
    t0 = time.perf_counter()

    headers = get_pco_headers(db)
    params: Dict[str, Any] = {
        "per_page": per_page,
        "include": "households",
    }
    if since:
        params["where[updated_at][gte]"] = f"{since}T00:00:00Z"

    page_ct = 0
    total_people_upserts = 0
    total_hh_upserts = 0

    people_rows: List[Tuple] = []
    hh_rows: List[Tuple] = []

    # small helper so we can commit periodically
    def flush_batch() -> Tuple[int, int]:
        nonlocal people_rows, hh_rows
        if not people_rows and not hh_rows:
            return (0, 0)

        # Deduplicate households inside the batch
        if hh_rows:
            dedup_hh = {}
            for r in hh_rows:
                dedup_hh[r[0]] = r  # key: household_id
            hh_rows = list(dedup_hh.values())

        # (Optional) dedupe people by person_id inside the batch
        if people_rows:
            dedup_people = {}
            for r in people_rows:
                dedup_people[r[0]] = r  # key: person_id
            people_rows = list(dedup_people.values())

        hh_count = _upsert_households(hh_rows) if hh_rows else 0
        ppl_count = _upsert_people(people_rows) if people_rows else 0

        # reset batch buffers
        people_rows = []
        hh_rows = []
        return (hh_count, ppl_count)

    try:
        log.info(
            "[people] sync starting since=%s per_page=%s batch_pages=%s",
            since, per_page, batch_pages
        )

        for page in paginate_next_links(PEOPLE_URL, headers=headers, params=params):
            page_ct += 1
            data = page.get("data") or []
            included = page.get("included") or []
            inc = {(i.get("type"), i.get("id")): i for i in included}

            # Per-page heartbeat
            log.info("[people] page=%s items=%s included=%s", page_ct, len(data), len(included))

            for item in data:
                pid = item.get("id")
                attrs = (item.get("attributes") or {})
                rels = (item.get("relationships") or {})

                first_name = attrs.get("first_name")
                last_name  = attrs.get("last_name")
                birthdate  = attrs.get("birthdate") or None
                grade      = attrs.get("grade") or None
                gender     = attrs.get("gender") or None
                email      = attrs.get("primary_email_address") or None
                phone      = attrs.get("primary_phone_number") or None
                campus_id  = None
                created_at = attrs.get("created_at")
                updated_at = attrs.get("updated_at")

                hh_rel = (rels.get("households") or {}).get("data") or []
                household_id = hh_rel[0]["id"] if hh_rel else None

                # Queue included households for upsert
                for hh in hh_rel:
                    h = inc.get(("Household", hh["id"])) or {}
                    hattrs = (h.get("attributes") or {})
                    hh_rows.append((
                        hh["id"],
                        hattrs.get("name"),
                        None,  # campus_id
                        hattrs.get("created_at"),
                        hattrs.get("updated_at"),
                    ))

                # Queue person for upsert
                people_rows.append((
                    pid,
                    household_id,
                    first_name,
                    last_name,
                    birthdate,
                    grade,
                    gender,
                    email,
                    phone,
                    campus_id,
                    created_at,
                    updated_at,
                ))

            # Commit every N pages so we see progress in logs
            if batch_pages and (page_ct % batch_pages == 0):
                hh_c, ppl_c = flush_batch()
                total_hh_upserts += hh_c
                total_people_upserts += ppl_c
                elapsed = time.perf_counter() - t0
                log.info(
                    "[people] committed batch upserts hh=%s ppl=%s totals hh=%s ppl=%s pages=%s elapsed=%.1fs",
                    hh_c, ppl_c, total_hh_upserts, total_people_upserts, page_ct, elapsed
                )

            if limit and page_ct >= limit:
                log.info("[people] limit reached at page=%s", page_ct)
                break

        # final flush
        hh_c, ppl_c = flush_batch()
        total_hh_upserts += hh_c
        total_people_upserts += ppl_c

    except Exception as e:
        log.exception("PCO People sync failed")
        raise HTTPException(status_code=502, detail=f"PCO People sync failed: {e}")

    elapsed = time.perf_counter() - t0
    log.info(
        "[people] sync complete pages=%s households=%s people=%s elapsed=%.1fs",
        page_ct, total_hh_upserts, total_people_upserts, elapsed
    )
    return {
        "status": "ok",
        "pages": page_ct,
        "households_upserted": total_hh_upserts,
        "people_upserted": total_people_upserts,
        "elapsed_seconds": round(elapsed, 1),
    }

