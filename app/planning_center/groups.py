# app/planning_center/groups.py
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from app.config import settings
from app.db import get_conn, get_db
from app.utils.common import paginate_next_links
from app.planning_center.oauth_routes import get_pco_headers

router = APIRouter(prefix="/planning-center/groups", tags=["Planning Center"])
log = logging.getLogger(__name__)

PCO_BASE = f"{settings.PLANNING_CENTER_BASE_URL}"
MAX_PER_PAGE = 100  # PCO max per_page


# ─────────────────────────────────────────────────────────────────────────────
# Your existing summary helpers (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_groups_by_type(type_name: str, db: Session, name: Optional[str] = None) -> List[dict]:
    """
    Fetch all active groups of a given GroupType name. Optionally filter by exact group name.
    Uses shared pagination helper.
    """
    headers = get_pco_headers(db)
    url = f"{PCO_BASE}/groups/v2/groups"
    params: Dict[str, str | int] = {"include[]": "group_type", "per_page": 100}

    results: List[dict] = []
    group_types: Dict[str, str] = {}  # GroupType ID -> GroupType name

    for page in paginate_next_links(url, headers=headers, params=params):
        for inc in page.get("included", []) or []:
            if inc.get("type") == "GroupType":
                group_types[inc["id"]] = (inc.get("attributes") or {}).get("name", "") or ""

        for g in page.get("data", []) or []:
            attrs = g.get("attributes") or {}
            rel = (g.get("relationships") or {}).get("group_type", {}).get("data")
            if attrs.get("archived_at") is None and rel:
                if group_types.get(rel.get("id")) == type_name:
                    if not name or attrs.get("name") == name:
                        results.append(g)
                        if name:
                            return results

    return results


def summarize_groups(db: Session) -> Dict[str, int]:
    """
    Fetches group and membership data to compute metrics in a single pass.
      - number_of_groups = count of active 'Groups' type
      - total_groups_attendance = unique people in all 'Groups' memberships (active)
      - group_leaders = unique leaders in 'Groups'
      - coaches = unique people in "Coaching Team" (type 'Teams')
    """
    groups = fetch_groups_by_type("Groups", db, None)
    number_of_groups = len(groups)
    group_ids: Set[str] = {g.get("id") for g in groups if g.get("id")}

    coaching = fetch_groups_by_type("Teams", db=db, name="Coaching Team")
    coaching_id = coaching[0].get("id") if coaching else None

    unique_people: Set[str] = set()
    leaders: Set[str] = set()
    coaches: Set[str] = set()

    headers = get_pco_headers(db)

    for gid in group_ids:
        url = f"{PCO_BASE}/groups/v2/groups/{gid}/memberships"
        params = {"filter[status]": "active", "per_page": 100}
        for page in paginate_next_links(url, headers=headers, params=params):
            for m in page.get("data", []) or []:
                pid = (
                    (m.get("relationships") or {})
                    .get("person", {})
                    .get("data", {})
                    .get("id")
                )
                role = ((m.get("attributes") or {}).get("role") or "").lower()
                if pid:
                    unique_people.add(pid)
                    if role == "leader":
                        leaders.add(pid)

    if coaching_id:
        url = f"{PCO_BASE}/groups/v2/groups/{coaching_id}/memberships"
        params = {"filter[status]": "active", "per_page": 100}
        for page in paginate_next_links(url, headers=headers, params=params):
            for m in page.get("data", []) or []:
                pid = (
                    (m.get("relationships") or {})
                    .get("person", {})
                    .get("data", {})
                    .get("id")
                )
                if pid:
                    coaches.add(pid)

    return {
        "number_of_groups":        number_of_groups,
        "total_groups_attendance": len(unique_people),
        "group_leaders":           len(leaders),
        "coaches":                 len(coaches),
    }


def insert_groups_summary_to_db(summary: dict, as_of_date):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO groups_summary
              (date, number_of_groups, total_groups_attendance, group_leaders, coaches)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (date) DO UPDATE SET
              number_of_groups        = EXCLUDED.number_of_groups,
              total_groups_attendance = EXCLUDED.total_groups_attendance,
              group_leaders           = EXCLUDED.group_leaders,
              coaches                 = EXCLUDED.coaches;
            """,
            (
                as_of_date,
                summary["number_of_groups"],
                summary["total_groups_attendance"],
                summary["group_leaders"],
                summary["coaches"],
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


@router.get("", response_model=dict)
def generate_and_store_groups_summary(db: Session = Depends(get_db)):
    summary = summarize_groups(db)
    today = datetime.now().date()
    insert_groups_summary_to_db(summary, today)
    return {
        "status":                  "success",
        "date":                    str(today),
        "metrics":                 summary,
        "distinct_group_count":    summary["number_of_groups"],
        "total_groups_attendance": summary["total_groups_attendance"],
        "group_leaders":           summary["group_leaders"],
        "coaches":                 summary["coaches"],
    }


# ─────────────────────────────────────────────────────────────────────────────
# NEW: Full Groups + Memberships sync that matches your actual schema
# ─────────────────────────────────────────────────────────────────────────────

def _parse_iso_ts_naive(val: Optional[str]) -> Optional[datetime]:
    """Parse ISO string to naive datetime (timestamp without time zone)."""
    if not val:
        return None
    try:
        dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
        # store UTC as naive to match your column type
        return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt
    except Exception:
        return None

def _existing_person_ids(pids: set[str]) -> set[str]:
    """Return the subset of pids that exist in pco_people (FK-safe)."""
    if not pids:
        return set()
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT person_id FROM pco_people WHERE person_id = ANY(%s);", (list(pids),))
        return {row[0] for row in cur.fetchall()}
    finally:
        cur.close()
        conn.close()


def upsert_pco_groups(rows: List[Tuple[str, str, Optional[str], Optional[str], Optional[datetime], Optional[datetime], bool]]) -> int:
    """
    rows: (group_id, name, group_type, campus_id, created_at_pco, updated_at_pco, is_serving_team)
    Matches table: pco_groups(group_id, name, group_type, campus_id, created_at_pco, updated_at_pco, is_serving_team)
    """
    if not rows:
        return 0
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.executemany(
            """
            INSERT INTO pco_groups
              (group_id, name, group_type, campus_id, created_at_pco, updated_at_pco, is_serving_team)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (group_id) DO UPDATE SET
              name            = EXCLUDED.name,
              group_type      = EXCLUDED.group_type,
              campus_id       = COALESCE(pco_groups.campus_id, EXCLUDED.campus_id),
              created_at_pco  = COALESCE(pco_groups.created_at_pco, EXCLUDED.created_at_pco),
              updated_at_pco  = EXCLUDED.updated_at_pco,
              is_serving_team = EXCLUDED.is_serving_team;
            """,
            rows,
        )
        affected = cur.rowcount
        conn.commit()
        return affected
    finally:
        cur.close()
        conn.close()


def upsert_f_groups_memberships(rows: List[Tuple[str, str, str, Optional[datetime], Optional[datetime], Optional[str]]]) -> int:
    """
    rows: (person_id, group_id, status, first_joined_at, archived_at, campus_id)
    Matches table: f_groups_memberships(person_id, group_id, status, first_joined_at, archived_at, campus_id)
    Conflict key: (person_id, group_id)
    """
    if not rows:
        return 0
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.executemany(
            """
            INSERT INTO f_groups_memberships
              (person_id, group_id, status, first_joined_at, archived_at, campus_id)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (person_id, group_id) DO UPDATE SET
              status          = EXCLUDED.status,
              first_joined_at = COALESCE(f_groups_memberships.first_joined_at, EXCLUDED.first_joined_at),
              archived_at     = COALESCE(EXCLUDED.archived_at, f_groups_memberships.archived_at),
              campus_id       = COALESCE(f_groups_memberships.campus_id, EXCLUDED.campus_id);
            """,
            rows,
        )
        affected = cur.rowcount
        conn.commit()
        return affected
    finally:
        cur.close()
        conn.close()


def _membership_status(attrs: dict) -> str:
    status = (attrs.get("status") or "").lower()
    if status == "active" and not attrs.get("archived_at") and not attrs.get("ended_at"):
        return "active"
    return "inactive"


@router.get("/sync", response_model=dict)  # ← add this back
def sync_groups_and_memberships(
    since: Optional[str] = Query(None, description="Optional updated-since filter (YYYY-MM-DD)"),
    per_page: int = Query(MAX_PER_PAGE, ge=1, le=MAX_PER_PAGE),
    limit_pages: Optional[int] = Query(None, ge=1, description="Stop after N group pages (testing)"),
    limit_groups: Optional[int] = Query(None, ge=1, description="Process only the first N groups from the page (testing)"),
    db: Session = Depends(get_db),
):
    headers = get_pco_headers(db)
    params: Dict[str, str | int] = {"include[]": "group_type", "per_page": per_page, "sort": "-updated_at"}
    if since:
        params[f"where[updated_at][gte]"] = f"{since}T00:00:00Z"

    groups_upserted_total = 0
    memb_upserted_total = 0
    group_pages = 0

    log.info("[groups] sync starting since=%s per_page=%s", since, per_page)

    url = f"{PCO_BASE}/groups/v2/groups"
    group_type_lookup: Dict[str, str] = {}

    try:
        for page in paginate_next_links(url, headers=headers, params=params):
            page_t0 = time.perf_counter()
            
            group_pages += 1
            if limit_pages and group_pages > limit_pages:
                log.info("[groups] limit_pages reached at page=%s", limit_pages)
                break

            data = page.get("data") or []
            included = page.get("included") or []
            for inc in included:
                if inc.get("type") == "GroupType":
                    gid = inc.get("id")
                    gname = (inc.get("attributes") or {}).get("name") or ""
                    if gid:
                        group_type_lookup[gid] = gname

            # Build rows for pco_groups (match your schema)
            group_rows: List[Tuple[str, str, Optional[str], Optional[str], Optional[datetime], Optional[datetime], bool]] = []
            for g in data:
                gid = g.get("id")
                attrs = g.get("attributes") or {}
                rel = (g.get("relationships") or {}).get("group_type", {}).get("data") or {}
                gt_name = group_type_lookup.get(rel.get("id"))

                name = attrs.get("name") or ""
                group_type = gt_name
                campus_id = None
                created_at_pco = _parse_iso_ts_naive(attrs.get("created_at"))
                updated_at_pco = _parse_iso_ts_naive(attrs.get("updated_at"))
                is_serving_team = False

                if gid:
                    group_rows.append((gid, name, group_type, campus_id, created_at_pco, updated_at_pco, is_serving_team))

            # ← apply limit AFTER building rows
            if limit_groups:
                group_rows = group_rows[:limit_groups]

            if group_rows:
                affected = upsert_pco_groups(group_rows)
                groups_upserted_total += affected

            # Build rows for memberships
            memb_rows: List[Tuple[str, str, str, Optional[datetime], Optional[datetime], Optional[str]]] = []
            for (gid, _name, _gt, _campus, _c_at, _u_at, _is_team) in group_rows:
                murl = f"{PCO_BASE}/groups/v2/groups/{gid}/memberships"
                mparams = {"per_page": 100}
                for mpage in paginate_next_links(murl, headers=headers, params=mparams):
                    for m in (mpage.get("data") or []):
                        m_attrs = m.get("attributes") or {}
                        status = _membership_status(m_attrs)
                        first_joined_at = _parse_iso_ts_naive(m_attrs.get("created_at") or m_attrs.get("joined_at"))
                        archived_at = _parse_iso_ts_naive(m_attrs.get("ended_at") or m_attrs.get("archived_at"))
                        person_id = (((m.get("relationships") or {}).get("person") or {}).get("data") or {}).get("id")
                        if not person_id:
                            continue
                        campus_id = None
                        memb_rows.append((person_id, gid, status, first_joined_at, archived_at, campus_id))

            if memb_rows:
                all_pids = {r[0] for r in memb_rows}
                existing = _existing_person_ids(all_pids)
                if len(existing) != len(all_pids):
                    skipped = len(all_pids) - len(existing)
                    log.warning("[groups] memberships: skipping %s rows due to missing people (FK). Seen=%s, existing=%s",
                                skipped, len(all_pids), len(existing))
                    memb_rows = [r for r in memb_rows if r[0] in existing]

                if memb_rows:
                    affected = upsert_f_groups_memberships(memb_rows)
                    memb_upserted_total += affected

            log.info(
                "[groups] page=%s in %.2fs groups=%s memberships_rows=%s (totals: groups=%s memberships=%s)",
                group_pages, time.perf_counter() - page_t0,
                len(group_rows), len(memb_rows),
                groups_upserted_total, memb_upserted_total
            )

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Groups sync failed: {e}")

    log.info("[groups] sync complete pages=%s groups_upserted=%s memberships_upserted=%s",
             group_pages, groups_upserted_total, memb_upserted_total)

    return {
        "status": "ok",
        "pages": group_pages,
        "groups_upserted": groups_upserted_total,
        "memberships_upserted": memb_upserted_total,
    }

