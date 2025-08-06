from fastapi import APIRouter, Depends
import requests, base64
from app.config import settings
from app.db import get_conn, get_db
from datetime import datetime
from sqlalchemy.orm import Session
from app.planning_center.oauth_routes import get_pco_headers

router = APIRouter(prefix="/planning-center/groups", tags=["Planning Center"])


def get_planning_center_headers():
    """
    Build headers for Basic auth using App ID & Secret.
    """
    auth = f"{settings.PLANNING_CENTER_APP_ID}:{settings.PLANNING_CENTER_SECRET}"
    token = base64.b64encode(auth.encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Accept":        "application/vnd.api+json",
    }


def fetch_groups_by_type(type_name: str, db: Session, name: str = None) -> list:
    """
    Fetch all active groups of a given GroupType name. Optionally filter by group name.
    """
    headers = get_pco_headers(db)
    url = "https://api.planningcenteronline.com/groups/v2/groups"
    params = {"include[]": "group_type", "per_page": 100}
    results = []
    group_types = {}

    while url:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        # build type lookup
        for inc in data.get("included", []):
            if inc.get("type") == "GroupType":
                group_types[inc["id"]] = inc.get("attributes", {}).get("name", "")

        # filter by type (and name if provided)
        for g in data.get("data", []):
            attrs = g.get("attributes", {})
            rel = g.get("relationships", {}).get("group_type", {}).get("data")
            if attrs.get("archived_at") is None and rel:
                if group_types.get(rel.get("id")) == type_name:
                    if not name or attrs.get("name") == name:
                        results.append(g)
                        if name:
                            return results

        url = data.get("links", {}).get("next")
        params = {"include[]": "group_type"}

    return results


def summarize_groups(db: Session):
    """
    Fetches group and membership data to compute metrics in a single pass.
    """
    # fetch all active "Groups" groups
    groups = fetch_groups_by_type("Groups", db, None)
    number_of_groups = len(groups)
    group_ids = {g.get("id") for g in groups}

    # find Coaching Team ID (type "Teams")
    coaching = fetch_groups_by_type("Teams", db=db, name="Coaching Team")
    coaching_id = coaching[0].get("id") if coaching else None

    unique_people = set()
    leaders = set()
    coaches = set()

    headers = get_pco_headers(db)

    # 1) loop through "Groups" memberships for people & leaders
    for gid in group_ids:
        url = f"https://api.planningcenteronline.com/groups/v2/groups/{gid}/memberships"
        params = {"filter[status]": "active", "per_page": 100}

        while url:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            for m in data.get("data", []):
                pid = m.get("relationships", {}).get("person", {}).get("data", {}).get("id")
                role = m.get("attributes", {}).get("role", "").lower()
                if pid:
                    unique_people.add(pid)
                    if role == "leader":
                        leaders.add(pid)

            url = data.get("links", {}).get("next")
            params = {}

    # 2) separately loop Coaching Team memberships for coaches
    if coaching_id:
        url = f"https://api.planningcenteronline.com/groups/v2/groups/{coaching_id}/memberships"
        params = {"filter[status]": "active", "per_page": 100}

        while url:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            for m in data.get("data", []):
                pid = m.get("relationships", {}).get("person", {}).get("data", {}).get("id")
                if pid:
                    coaches.add(pid)

            url = data.get("links", {}).get("next")
            params = {}

    return {
        "number_of_groups":        number_of_groups,
        "total_groups_attendance": len(unique_people),
        "group_leaders":           len(leaders),
        "coaches":                 len(coaches),
    }


def insert_groups_summary_to_db(summary: dict, as_of_date):
    conn = get_conn()
    cur = conn.cursor()
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
        )
    )
    conn.commit()
    cur.close()
    conn.close()


@router.get("", response_model=dict)
def generate_and_store_groups_summary(db: Session = Depends(get_db)):
    summary = summarize_groups(db)
    today = datetime.now().date()
    insert_groups_summary_to_db(summary, today)
    return {
        "status":                 "success",
        "date":                   str(today),
        "metrics":                summary,
        "distinct_group_count":   summary["number_of_groups"],
        "total_groups_attendance": summary["total_groups_attendance"],
        "group_leaders":           summary["group_leaders"],
        "coaches":                 summary["coaches"],
    }
