import requests
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from collections import defaultdict

from fastapi import APIRouter, HTTPException

from app.config import settings
from app.db import get_conn

router = APIRouter(prefix="/planning-center/checkins", tags=["Planning Center"])


SERVICE_KEY_MAP = {
    "9:30 AM":  "930",
    "11:00 AM": "1100",
    "4:30 PM":  "1630",
}

MINISTRY_COLUMNS = {
    "Waumba Land": [
        "attendance_930", "attendance_1100", "total_attendance",
        "total_new_kids",  "notes",
        "age_0_2_male",    "age_0_2_female",
        "age_3_5_male",    "age_3_5_female",
    ],
    "UpStreet": [
        "attendance_930", "attendance_1100", "total_attendance",
        "total_new_kids", "notes",
        "grade_k_1_male", "grade_k_1_female",
        "grade_2_3_male", "grade_2_3_female",
        "grade_4_5_male", "grade_4_5_female",
    ],
    "Transit": [
        "attendance_930", "attendance_1100", "total_attendance",
        "total_new_kids", "notes",
        "grade_6_male",   "grade_6_female",
        "grade_7_male",   "grade_7_female",
        "grade_8_male",   "grade_8_female",
    ],
    "InsideOut": [
        "total_attendance", "new_students", "notes",
    ],
}


def get_last_sunday() -> datetime.date:
    today = datetime.now(tz=ZoneInfo("America/Chicago"))
    # weekday(): Mon=0 … Sun=6 → (weekday+1)%7 days back to last Sunday
    last_sunday = today - timedelta(days=(today.weekday() + 1) % 7)
    return last_sunday.date()


def fetch_all_checkins(date: datetime.date):
    url = "https://api.planningcenteronline.com/check-ins/v2/check_ins"
    auth = (settings.PLANNING_CENTER_APP_ID, settings.PLANNING_CENTER_SECRET)
    params = {
        "include": "person,event,location",
        "where[created_at][gte]": f"{date}T00:00:00Z",
        "where[created_at][lte]": f"{date}T23:59:59Z",
    }

    checkins = []
    included = []
    try:
        while url:
            resp = requests.get(url, auth=auth, headers={"Accept": "application/json"}, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            checkins.extend(data.get("data", []))
            included.extend(data.get("included", []))
            url = data.get("links", {}).get("next")
            params = {}  # only send filters on first page
    except Exception as e:
        raise HTTPException(502, f"PCO fetch failed: {e}")

    return checkins, included


def parse_people_data(included: list[dict]) -> dict[str, dict]:
    people: dict[str, dict] = {}
    for item in included:
        if item.get("type") != "Person":
            continue

        pid   = item.get("id")
        attrs = item.get("attributes", {})

        # grade → int
        grade = None
        if attrs.get("grade") is not None:
            try:
                grade = int(attrs["grade"])
            except ValueError:
                pass

        # birthdate → age in years
        age = None
        bd = attrs.get("birthdate")
        if bd:
            try:
                born = datetime.fromisoformat(bd).date()
                age = (datetime.now().date() - born).days // 365
            except Exception:
                pass

        raw_gender = attrs.get("gender")
        if isinstance(raw_gender, str) and raw_gender.strip():
            gender = raw_gender.lower()
        else:
            gender = "other"

        people[pid] = {"grade": grade, "age": age, "gender": gender}

    return people


def determine_ministry(grade: int | None, age: int | None) -> str | None:
    # by grade first
    if grade is not None:
        if 0 <= grade <= 4:
            return "UpStreet"
        if 5 <= grade <= 8:
            return "Transit"
        if 9 <= grade <= 12:
            return "InsideOut"
    # fallback by age
    if age is not None:
        if age <= 5:
            return "Waumba Land"
        if 6 <= age <= 10:
            return "UpStreet"
    return None


def determine_service_time(iso_time: str, ministry: str) -> str | None:
    # from Zulu into Chicago
    dt = datetime.fromisoformat(iso_time.replace("Z", "+00:00")) \
                .astimezone(ZoneInfo("America/Chicago"))
    t = dt.time()

    if ministry == "InsideOut":
        if time(15, 45) <= t <= time(17, 0):
            return "4:30 PM"
    else:
        if time(9, 0) <= t <= time(10, 0):
            return "9:30 AM"
        if time(10, 30) <= t <= time(11, 30):
            return "11:00 AM"
    return None


def summarize_checkins_by_ministry(
    checkins: list[dict], people: dict[str, dict]
) -> dict[str, defaultdict[str, int]]:
    # prepare counters
    summary = {m: defaultdict(int) for m in MINISTRY_COLUMNS}

    for c in checkins:
        # guard against missing person relationship
        person_data = (
            c.get("relationships", {})
             .get("person", {})
             .get("data")
        )
        if not isinstance(person_data, dict) or "id" not in person_data:
            continue

        pid = person_data["id"]
        pd = people.get(pid)
        if not pd:
            continue

        ministry = determine_ministry(pd["grade"], pd["age"])
        svc = determine_service_time(c["attributes"]["created_at"], ministry)
        if not ministry or not svc:
            continue

        key = SERVICE_KEY_MAP[svc]
        summary[ministry][f"attendance_{key}"] += 1

        if c["attributes"].get("one_time_guest", False):
            summary[ministry][f"new_kids_{key}"] += 1

        # breakdown demographics
        gender = pd["gender"]
        if ministry == "Waumba Land":
            age = pd["age"]
            if age is not None:
                bracket = "0_2" if age <= 2 else "3_5" if age <= 5 else None
                if bracket:
                    summary[ministry][f"age_{bracket}_{gender}"] += 1

        elif ministry in ("UpStreet", "Transit"):
            grade = pd["grade"]
            if grade is not None:
                if ministry == "UpStreet":
                    if grade in (0, 1):
                        grp = "k_1"
                    elif grade in (2, 3):
                        grp = "2_3"
                    elif grade in (4, 5):
                        grp = "4_5"
                    else:
                        grp = None
                else:  # Transit
                    if grade in (6,):
                        grp = "6"
                    elif grade in (7,):
                        grp = "7"
                    elif grade in (8,):
                        grp = "8"
                    else:
                        grp = None

                if grp:
                    summary[ministry][f"grade_{grp}_{gender}"] += 1

    return summary


def insert_summary_into_db(ministry: str, data: dict):
    table_map = {
        "Waumba Land": "waumbaland_attendance",
        "UpStreet":     "upstreet_attendance",
        "Transit":      "transit_attendance",
        "InsideOut":    "insideout_attendance",
    }
    table = table_map[ministry]
    cols = ["date"] + MINISTRY_COLUMNS[ministry]
    vals = [data[col] for col in cols]

    col_sql = ", ".join(cols)
    placeholders = ", ".join("%s" for _ in cols)
    updates = ", ".join(f"{col}=EXCLUDED.{col}" for col in cols if col != "date")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"""
        INSERT INTO {table} ({col_sql})
        VALUES ({placeholders})
        ON CONFLICT (date) DO UPDATE SET {updates}
        """,
        vals,
    )
    conn.commit()
    cur.close()
    conn.close()


def fetch_and_process_checkins():
    date = get_last_sunday()
    checkins, included = fetch_all_checkins(date)
    people = parse_people_data(included)
    raw_sum = summarize_checkins_by_ministry(checkins, people)

    # build debug info
    debug = {
        "checkins_count": len(checkins),
        "included_count": len(included),
        "summaries": {m: dict(cnts) for m, cnts in raw_sum.items()},
    }

    # upsert into each ministry table
    for m, data in raw_sum.items():
        data["date"] = date
        if m == "InsideOut":
            data["total_attendance"] = data.get("attendance_1630", 0)
            data["new_students"]     = data.get("new_kids_1630", 0)
        else:
            data["total_attendance"] = (
                data.get("attendance_930", 0) + data.get("attendance_1100", 0)
            )
            data["total_new_kids"] = (
                data.get("new_kids_930", 0) + data.get("new_kids_1100", 0)
            )

        if data["total_attendance"] == 0:
            continue

        data["notes"] = None
        # ensure all keys exist
        for col in MINISTRY_COLUMNS[m]:
            data.setdefault(col, 0 if col != "notes" else None)

        insert_summary_into_db(m, data)

    return {"status": "success", "date": str(date), **debug}


@router.get("")
def run_checkin_summary():
    return fetch_and_process_checkins()