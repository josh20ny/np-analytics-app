from fastapi import APIRouter
import requests, base64
from app.config import settings
from app.db import get_conn
from datetime import datetime

router = APIRouter(prefix="/planning-center/groups", tags=["Planning Center"])


def get_planning_center_headers():
    """
    Build headers for Basic auth using OAuth App ID & Secret.
    """
    auth = f"{settings.PLANNING_CENTER_APP_ID}:{settings.PLANNING_CENTER_SECRET}"
    token = base64.b64encode(auth.encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Accept":        "application/vnd.api+json"
    }


def fetch_all_groups():
    """
    Returns a list of active Group objects of type "Groups" from /groups/v2/groups, paging as needed.
    """
    headers = get_planning_center_headers()
    url = "https://api.planningcenteronline.com/groups/v2/groups"
    params = {"include[]": "group_type", "per_page": 100}
    filtered = []
    group_types = {}

    # page through groups
    while url:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        body = resp.json()

        # build group_type lookup
        for inc in body.get("included", []):
            if inc.get("type") == "GroupType":
                group_types[inc["id"]] = inc.get("attributes", {}).get("name", "")

        # filter active Groups-type groups
        for g in body.get("data", []):
            attrs = g.get("attributes", {})
            rel = g.get("relationships", {}).get("group_type", {}).get("data")
            if attrs.get("archived_at") is None and rel:
                if group_types.get(rel.get("id")) == "Groups":
                    filtered.append(g)

        url = body.get("links", {}).get("next")
        params = {"include[]": "group_type"}

    return filtered


def fetch_unique_counts(group_ids: set):
    """
    Pages through each group's memberships endpoint to accumulate:
      - unique_people: set of unique person IDs
      - group_leaders: set of unique person IDs with role "leader"
    Returns (unique_people_count, group_leaders_count).
    """
    headers = get_planning_center_headers()
    unique_people = set()
    leaders = set()

    for gid in group_ids:
        url = f"https://api.planningcenteronline.com/groups/v2/groups/{gid}/memberships"
        params = {"filter[status]": "active", "per_page": 100}

        while url:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            body = resp.json()

            for m in body.get("data", []):
                pid = m.get("relationships", {}).get("person", {}).get("data", {}).get("id")
                role = m.get("attributes", {}).get("role")
                if pid:
                    unique_people.add(pid)
                    if role == "leader":
                        leaders.add(pid)

            url = body.get("links", {}).get("next")
            params = {}

    return len(unique_people), len(leaders)


def summarize_groups(group_list):
    """
    Compute summary metrics:
      - number_of_groups
      - total_groups_attendance (unique people)
      - group_leaders (unique leaders count)
      - coaches (set to 0 for now)
    """
    number_of_groups = len(group_list)
    group_ids = {g.get("id") for g in group_list}

    # unique people and leaders count
    unique_people_count, leaders_count = fetch_unique_counts(group_ids)

    return {
        "number_of_groups":          number_of_groups,
        "total_groups_attendance":   unique_people_count,
        "group_leaders":             leaders_count,
        "coaches":                   0,
    }


def insert_groups_summary_to_db(summary: dict, as_of_date):
    """
    Insert or update the groups_summary table for the given date.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO groups_summary
          (date, number_of_groups, total_groups_attendance, group_leaders, coaches)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE SET
          number_of_groups          = EXCLUDED.number_of_groups,
          total_groups_attendance   = EXCLUDED.total_groups_attendance,
          group_leaders             = EXCLUDED.group_leaders,
          coaches                   = EXCLUDED.coaches;
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
def generate_and_store_groups_summary():
    """
    Fetch filtered groups, summarize metrics, store snapshot, and return results.
    """
    groups = fetch_all_groups()
    summary = summarize_groups(groups)
    today = datetime.now().date()
    insert_groups_summary_to_db(summary, today)
    return {
        "status":               "success",
        "date":                 str(today),
        "metrics":              summary,
        "distinct_group_count": summary["number_of_groups"],
        "total_groups_attendance": summary["total_groups_attendance"],
        "group_leaders":         summary["group_leaders"],
        "coaches":               summary["coaches"]
    }
