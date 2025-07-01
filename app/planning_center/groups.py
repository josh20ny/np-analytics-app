from fastapi import APIRouter
import requests, base64
from ..config import settings
from ..db import get_conn
from datetime import datetime

router = APIRouter(prefix="/planning-center/groups", tags=["Planning Center"])

def get_planning_center_headers():
    auth = f"{settings.PLANNING_CENTER_APP_ID}:{settings.PLANNING_CENTER_SECRET}"
    token = base64.b64encode(auth.encode()).decode()
    return {"Authorization": f"Basic {token}"}


def fetch_total_groups_count(headers):
    url = "https://api.planningcenteronline.com/groups/v2/groups"
    params = {"per_page":100, "include[]":["group_type"]}
    total = 0
    while url:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        for g in data.get("data",[]):
            gid = g.get("relationships",{}).get("group_type",{}).get("data",{}).get("id")
            for inc in data.get("included",[]):
                if inc.get("type")=="GroupType" and inc.get("id")==gid:
                    if inc.get("attributes",{}).get("name","").lower()=="hangout":
                        break
            else:
                total += 1
        url = data.get("links",{}).get("next")
    return total


def insert_groups_summary_to_db(summary, date):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO groups_summary (
            date, number_of_groups, total_groups_attendance,
            group_leaders, coaches, total_connection_volunteers
        ) VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT(date) DO UPDATE SET
            number_of_groups=EXCLUDED.number_of_groups,
            total_groups_attendance=EXCLUDED.total_groups_attendance,
            group_leaders=EXCLUDED.group_leaders,
            coaches=EXCLUDED.coaches,
            total_connection_volunteers=EXCLUDED.total_connection_volunteers;
        """,
        (
            date,
            summary.get("total_groups",0),
            summary.get("total_attendance",0),
            summary.get("group_leaders",0),
            summary.get("coaches",0),
            summary.get("total_connection_volunteers",0)
        )
    )
    conn.commit()
    cur.close()
    conn.close()

@router.get("")
def generate_and_store_groups_summary():
    headers = get_planning_center_headers()
    total = fetch_total_groups_count(headers)
    summary = {
        "total_groups": total,
        "total_attendance": 0,
        "group_leaders": 0,
        "coaches": 0,
        "total_connection_volunteers": 0
    }
    insert_groups_summary_to_db(summary, datetime.now().date())
    return {"status":"success","date": str(datetime.now().date()), "summary": summary}