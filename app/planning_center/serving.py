# app/planning_center/serving.py
from __future__ import annotations

import logging
import time
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.config import settings
from app.db import get_conn, get_db
from app.utils.common import paginate_next_links
from app.planning_center.oauth_routes import get_pco_headers

# Reuse the DB helpers defined in groups.py so we don't duplicate logic
from app.planning_center.groups import (
    _existing_person_ids,
    _membership_status,
    _parse_iso_ts_naive,
    upsert_f_groups_memberships,
    upsert_pco_groups,
)

router = APIRouter(prefix="/planning-center/serving", tags=["Planning Center"])
log = logging.getLogger(__name__)

PCO_BASE = f"{settings.PLANNING_CENTER_BASE_URL}"
MAX_PER_PAGE = 100  # PCO max per_page

# ────────────────────────────────────────────────────────────────────────────────
# Utilities
# ────────────────────────────────────────────────────────────────────────────────

# replace your _normalize_list_param with this
def _normalize_list_param(value: Optional[str], *, split_on_space: bool = False) -> Optional[List[str]]:
    """Split by '|' and ',' by default. Optionally split on spaces."""
    if not value:
        return None
    seps = ",|"
    if split_on_space:
        seps += " "
    tokens: List[str] = []
    buf = ""
    for ch in value:
        if ch in seps:
            if buf.strip():
                tokens.append(buf.strip())
            buf = ""
        else:
            buf += ch
    if buf.strip():
        tokens.append(buf.strip())
    return tokens or None



def _is_serving_team(
    group_name: str,
    group_type_name: Optional[str],
    include_types: Optional[Iterable[str]],
    include_name_substrings: Optional[Iterable[str]],
    exclude_exact_names: Optional[Iterable[str]],
) -> bool:
    if _norm(group_type_name) == _norm("Groups"):
        return False
    gt_norm = _norm(group_type_name)
    name_norm = _norm(group_name)
    for gt_label, teams in EXACT_TEAM_MAP_NORM.items():
        if _norm(gt_label) == gt_norm and name_norm in teams:
            return True
    return False



# ────────────────────────────────────────────────────────────────────────────────
# SYNC: Pull serving *teams* from PCO Groups API and upsert groups + memberships
# ────────────────────────────────────────────────────────────────────────────────

@router.get("/sync", response_model=dict)
def sync_serving_teams_and_memberships(
    since: Optional[str] = Query(
        None, description="Optional updated-since filter (YYYY-MM-DD)"
    ),
    per_page: int = Query(MAX_PER_PAGE, ge=1, le=MAX_PER_PAGE),
    limit_pages: Optional[int] = Query(None, ge=1, description="Stop after N group pages (testing)"),
    limit_groups: Optional[int] = Query(None, ge=1, description="Process only the first N serving groups from the page (testing)"),
    include_types: Optional[str] = Query(
        "Teams",
        description=(
            "GroupType names considered SERVING (comma/pipe/space separated).\n"
            "Example: 'Teams|Volunteer Teams|Serving'"
        ),
    ),
    include_name_substrings: Optional[str] = Query(
        None,
        description=(
            "If provided, any group whose *name* contains one of these substrings\n"
            "will be treated as a serving team. Example: 'Usher,Greeter,Parking'"
        ),
    ),
    exclude_exact_names: Optional[str] = Query(
        "Coaching Team",
        description=(
            "Exact group *names* to exclude (comma/pipe/space separated).\n"
            "Useful to drop coaching/leader groups from serving counts."
        ),
    ),
    db: Session = Depends(get_db),
):
    """
    Reads PCO Groups, identifies which groups represent serving teams, then:
      • Upserts those groups into pco_groups with is_serving_team=True.
      • Upserts their memberships into f_groups_memberships (status, joined/archived).

    Notes:
      - Identification uses GroupType name and/or name substrings; pass them here so
        we don't hardcode ministry-specific logic in code.
    """
    headers = get_pco_headers(db)

    params: Dict[str, str | int] = {"include[]": "group_type", "per_page": per_page, "sort": "-updated_at"}
    if since:
        params["where[updated_at][gte]"] = f"{since}T00:00:00Z"

    include_types_list = _normalize_list_param(include_types, split_on_space=False)
    include_subs_list = _normalize_list_param(include_name_substrings, split_on_space=True)
    exclude_names_list = _normalize_list_param(exclude_exact_names, split_on_space=True)


    groups_upserted_total = 0
    memb_upserted_total = 0
    group_pages = 0

    log.info(
        "[serving] sync starting since=%s per_page=%s include_types=%s include_subs=%s", 
        since, per_page, include_types_list, include_subs_list
    )

    url = f"{PCO_BASE}/groups/v2/groups"
    group_type_lookup: Dict[str, str] = {}

    try:
        for page in paginate_next_links(url, headers=headers, params=params):
            page_t0 = time.perf_counter()
            group_pages += 1
            if limit_pages and group_pages > limit_pages:
                log.info("[serving] limit_pages reached at page=%s", limit_pages)
                break

            data = page.get("data") or []
            included = page.get("included") or []
            for inc in included:
                if inc.get("type") == "GroupType":
                    gid = inc.get("id")
                    gname = (inc.get("attributes") or {}).get("name") or ""
                    if gid:
                        group_type_lookup[gid] = gname

            # Build rows for pco_groups (serving-only)
            group_rows: List[Tuple[str, str, Optional[str], Optional[str], Optional[datetime], Optional[datetime], bool]] = []
            serving_group_ids: List[str] = []
            for g in data:
                gid = g.get("id")
                attrs = g.get("attributes") or {}
                rel = (g.get("relationships") or {}).get("group_type", {}).get("data") or {}
                gt_name = group_type_lookup.get(rel.get("id"))

                name = attrs.get("name") or ""
                is_serving = _is_serving_team(
                    group_name=name,
                    group_type_name=gt_name,
                    include_types=include_types_list,
                    include_name_substrings=include_subs_list,
                    exclude_exact_names=exclude_names_list,
                )
                if not (gid and is_serving):
                    continue

                created_at_pco = _parse_iso_ts_naive(attrs.get("created_at"))
                updated_at_pco = _parse_iso_ts_naive(attrs.get("updated_at"))

                group_rows.append((gid, name, gt_name, None, created_at_pco, updated_at_pco, True))
                serving_group_ids.append(gid)

            # apply optional per-page limit *after* filtering to serving-only
            if limit_groups:
                serving_group_ids = serving_group_ids[:limit_groups]
                group_rows = [r for r in group_rows if r[0] in set(serving_group_ids)]

            if group_rows:
                affected = upsert_pco_groups(group_rows)
                groups_upserted_total += affected

            # Build rows for memberships for the serving groups on this page
            memb_rows: List[Tuple[str, str, str, Optional[datetime], Optional[datetime], Optional[str]]] = []
            for gid in serving_group_ids:
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
                        memb_rows.append((person_id, gid, status, first_joined_at, archived_at, None))

            if memb_rows:
                all_pids = {r[0] for r in memb_rows}
                existing = _existing_person_ids(all_pids)
                if len(existing) != len(all_pids):
                    skipped = len(all_pids) - len(existing)
                    log.warning(
                        "[serving] memberships: skipping %s rows due to missing people (FK). Seen=%s, existing=%s",
                        skipped, len(all_pids), len(existing)
                    )
                    memb_rows = [r for r in memb_rows if r[0] in existing]

                if memb_rows:
                    affected = upsert_f_groups_memberships(memb_rows)
                    memb_upserted_total += affected

            log.info(
                "[serving] page=%s in %.2fs groups=%s memberships_rows=%s (totals: groups=%s memberships=%s)",
                group_pages, time.perf_counter() - page_t0,
                len(group_rows), len(memb_rows),
                groups_upserted_total, memb_upserted_total,
            )

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Serving sync failed: {e}")

    log.info(
        "[serving] sync complete pages=%s groups_upserted=%s memberships_upserted=%s",
        group_pages, groups_upserted_total, memb_upserted_total
    )

    return {
        "status": "ok",
        "pages": group_pages,
        "groups_upserted": groups_upserted_total,
        "memberships_upserted": memb_upserted_total,
    }


# ────────────────────────────────────────────────────────────────────────────────
# WEEKLY SUMMARY: volunteers total + per-category (custom ministry map)
# ────────────────────────────────────────────────────────────────────────────────

# Canonical categories we will publish in the weekly table
CATEGORIES = ("Groups", "InsideOut", "Transit", "UpStreet", "Waumba Land", "Misc")

# Exact mapping built from Joshua's taxonomy (GroupType -> {Team Name -> Category or tuple of Categories})
# ───────── Mapping (strict) ─────────

def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower().replace("’", "'")

# Your curated list (GroupType -> Team Name -> Category(s))
EXACT_TEAM_MAP: Dict[str, Dict[str, Tuple[str, ...]]] = {
    "Teams": {
        "Connection Support Team": ("Groups",),
        "Group Leaders & Coaches": ("Groups",),
        "Exchange ATX Team": ("Misc",),
        "Facilities": ("Misc",),
        "For Austin Prayer Team": ("Misc",),
        "Mentors": ("Misc",),
        "Safety Team": ("Misc",),
        "What's Next Team": ("Misc",),
    },
    "InsideOut Leaders": {
        "InsideOut Leaders": ("InsideOut",),
    },
    "InsideOut General": {
        "InsideOut Guest Services": ("InsideOut",),
        "InsideOut Set Up & Tear Down": ("InsideOut",),
    },
    "Transit Leaders": {
        "Transit Leaders": ("Transit",),
    },
    "Transit General": {
        "Transit Guest Services 11:00": ("Transit",),
        "Transit Guest Services 9:30": ("Transit",),
    },
    "Unique Groups": {
        "UPST 11:00 2nd & 3rd Leaders": ("UpStreet",),
        "UPST 11:00 4th, 5th, Hangout Leaders": ("UpStreet",),
        "UPST 11:00 K, 1st, Buddy Leaders": ("UpStreet",),
        "UPST 9:30 2nd & 3rd Leaders": ("UpStreet",),
        "UPST 9:30 4th, 5th, Hangout Leaders": ("UpStreet",),
        "UPST 9:30 K & 1st Leaders": ("UpStreet",),
        "UPST Coaches": ("UpStreet",),
        "UPST Large Group": ("UpStreet",),
        "Waumba Land 11:00 Leaders": ("Waumba Land",),
        "Waumba Land 9:30 Leaders": ("Waumba Land",),
        "Waumba Land Coaches": ("Waumba Land",),
        "Waumba Land Large Group": ("Waumba Land",),
        "Kid Min Digital Team": ("UpStreet", "Waumba Land"),
        "Kids Check In": ("UpStreet", "Waumba Land"),
        "Wow Team": ("UpStreet", "Waumba Land"),
    },
}

# Build a normalized lookup for comparisons
EXACT_TEAM_MAP_NORM: Dict[str, Dict[str, Tuple[str, ...]]] = {
    gt: { _norm(name): cats for name, cats in teams.items() }
    for gt, teams in EXACT_TEAM_MAP.items()
}

def _classify_categories(group_type: Optional[str], team_name: Optional[str]) -> Tuple[str, ...]:
    # Ignore GroupType "Groups" entirely
    if _norm(group_type) == _norm("Groups"):
        return tuple()
    gt_norm = _norm(group_type)
    name_norm = _norm(team_name)
    for gt_label, teams in EXACT_TEAM_MAP_NORM.items():
        if _norm(gt_label) == gt_norm:
            cats = teams.get(name_norm)
            if cats:
                return cats
            break
    return tuple()  # not curated → ignore completely


def _serving_counts_by_category(as_of: date) -> Tuple[int, Dict[str, int]]:
    """
    Distinct volunteer counts per curated category, as of the given date.
    Only curated (GroupType, Team Name) are counted.
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT DISTINCT m.person_id, g.group_type, g.name
            FROM f_groups_memberships m
            JOIN pco_groups g ON g.group_id = m.group_id
            WHERE (m.first_joined_at IS NULL OR m.first_joined_at::date <= %s)
              AND (m.archived_at IS NULL OR m.archived_at::date > %s);
            """,
            (as_of, as_of),
        )
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    total_people: set[str] = set()
    per_cat: Dict[str, set[str]] = {c: set() for c in CATEGORIES}

    for pid, group_type, name in rows:
        cats = _classify_categories(group_type, name)
        if not cats:
            continue
        total_people.add(pid)
        for cat in cats:
            per_cat.setdefault(cat, set()).add(pid)

    by_cat_counts = {c: len(per_cat.get(c, set())) for c in CATEGORIES}
    return len(total_people), by_cat_counts




def _upsert_serving_weekly(
    week_end: date,
    total: int,
    by_cat: Dict[str, int],
) -> None:
    """Create/Update one row in serving_volunteers_weekly."""
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO serving_volunteers_weekly
              (week_end, total_volunteers, groups_volunteers, insideout_volunteers,
               transit_volunteers, upstreet_volunteers, waumba_land_volunteers, misc_volunteers)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (week_end) DO UPDATE SET
              total_volunteers        = EXCLUDED.total_volunteers,
              groups_volunteers       = EXCLUDED.groups_volunteers,
              insideout_volunteers    = EXCLUDED.insideout_volunteers,
              transit_volunteers      = EXCLUDED.transit_volunteers,
              upstreet_volunteers     = EXCLUDED.upstreet_volunteers,
              waumba_land_volunteers  = EXCLUDED.waumba_land_volunteers,
              misc_volunteers         = EXCLUDED.misc_volunteers;
            """,
            (
                week_end,
                int(total),
                int(by_cat.get("Groups", 0)),
                int(by_cat.get("InsideOut", 0)),
                int(by_cat.get("Transit", 0)),
                int(by_cat.get("UpStreet", 0)),
                int(by_cat.get("Waumba Land", 0)),
                int(by_cat.get("Misc", 0)),
            ),
        )
        conn.commit()
    finally:
        cur.close(); conn.close()


@router.get("/summary", response_model=dict)
def serving_weekly_summary(
    week_end: Optional[str] = Query(None, description="Sunday YYYY-MM-DD; defaults to last Sunday (CST)"),
    persist: bool = Query(True, description="If true, upsert into serving_volunteers_weekly"),
    db: Session = Depends(get_db),
):
    log.info("[serving] summary source file: %s", __file__)
    # Compute last-Sunday default (CST semantics to match the app)
    from datetime import timedelta
    def _last_sunday_cst(today: Optional[date] = None) -> date:
        d = (today or datetime.now().date())
        return d - timedelta(days=((d.weekday() + 1) % 7))

    week_end_dt = date.fromisoformat(week_end) if week_end else _last_sunday_cst()

    total, by_cat = _serving_counts_by_category(week_end_dt)

    if persist:
        _upsert_serving_weekly(week_end_dt, total, by_cat)

    return {
        "status": "ok",
        "week_end": str(week_end_dt),
        "total_active_volunteers": total,
        "volunteers_by_category": by_cat,
        "persisted": bool(persist),
    }
