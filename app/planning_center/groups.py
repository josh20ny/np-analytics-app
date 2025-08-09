# app/planning_center/groups.py
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Dict, List, Optional, Set

from app.config import settings
from app.db import get_conn, get_db
from app.utils.common import paginate_next_links
from app.planning_center.oauth_routes import get_pco_headers

router = APIRouter(prefix="/planning-center/groups", tags=["Planning Center"])

PCO_BASE = f"{settings.PLANNING_CENTER_BASE_URL}"


def fetch_groups_by_type(type_name: str, db: Session, name: Optional[str] = None) -> List[dict]:
    """
    Fetch all active groups of a given GroupType name. Optionally filter by exact group name.
    Uses shared pagination helper.
    """
    headers = get_pco_headers(db)
    url = f"{PCO_BASE}/groups/v2/groups"

    # Keep the same include semantics you had before
    params: Dict[str, str | int] = {"include[]": "group_type", "per_page": 100}

    results: List[dict] = []
    group_types: Dict[str, str] = {}  # GroupType ID -> GroupType name

    for page in paginate_next_links(url, headers=headers, params=params):
        # Build/extend type lookup
        for inc in page.get("included", []) or []:
            if inc.get("type") == "GroupType":
                group_types[inc["id"]] = (inc.get("attributes") or {}).get("name", "") or ""

        # Filter by type (and exact name, if provided)
        for g in page.get("data", []) or []:
            attrs = g.get("attributes") or {}
            rel = (g.get("relationships") or {}).get("group_type", {}).get("data")
            if attrs.get("archived_at") is None and rel:
                if group_types.get(rel.get("id")) == type_name:
                    if not name or attrs.get("name") == name:
                        results.append(g)
                        if name:
                            return results  # early exit if we asked for a single, exact group

        # After first iteration, paginate_next_links follows the "next" link itself (no need to change params)

    return results


def summarize_groups(db: Session) -> Dict[str, int]:
    """
    Fetches group and membership data to compute metrics in a single pass.
    Logic preserved:
      - number_of_groups = count of active 'Groups' type
      - total_groups_attendance = unique people in all 'Groups' memberships (active)
      - group_leaders = unique leaders in 'Groups'
      - coaches = unique people in "Coaching Team" (type 'Teams')
    """
    # 1) Fetch all active "Groups" groups
    groups = fetch_groups_by_type("Groups", db, None)
    number_of_groups = len(groups)
    group_ids: Set[str] = {g.get("id") for g in groups if g.get("id")}

    # 2) Find Coaching Team ID (type "Teams")
    coaching = fetch_groups_by_type("Teams", db=db, name="Coaching Team")
    coaching_id = coaching[0].get("id") if coaching else None

    unique_people: Set[str] = set()
    leaders: Set[str] = set()
    coaches: Set[str] = set()

    headers = get_pco_headers(db)

    # 3) Loop through "Groups" memberships for people & leaders (active only)
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

    # 4) Separately loop Coaching Team memberships for coaches (active only)
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
