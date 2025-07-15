import requests
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from collections import defaultdict

from fastapi import APIRouter, HTTPException

from app.config import settings
from app.db import get_conn

router = APIRouter(prefix="/planning-center/checkins", tags=["Planning Center"])

# Map service labels to summary keys
defaultdict
SERVICE_KEY_MAP = {
    "9:30 AM":  "930",
    "11:00 AM": "1100",
    "4:30 PM":  "1630",
}

# Columns to persist for each ministry
MINISTRY_COLUMNS = {
    "Waumba Land": [
        "attendance_930", "attendance_1100", "total_attendance",
        "new_kids_930",   "new_kids_1100",   "total_new_kids",  "notes",
        "age_0_2_male",    "age_0_2_female",
        "age_3_5_male",    "age_3_5_female",
    ],
    "UpStreet": [
        "attendance_930", "attendance_1100", "total_attendance",
        "new_kids_930",   "new_kids_1100",   "total_new_kids",  "notes",
        "grade_k_1_male", "grade_k_1_female",
        "grade_2_3_male", "grade_2_3_female",
        "grade_4_5_male", "grade_4_5_female",
    ],
    "Transit": [
        "attendance_930", "attendance_1100", "total_attendance",
        "new_kids_930",   "new_kids_1100",   "total_new_kids",  "notes",
        "grade_6_male",   "grade_6_female",
        "grade_7_male",   "grade_7_female",
        "grade_8_male",   "grade_8_female",
    ],
    "InsideOut": [
        "total_attendance", "new_students", "notes",
    ],
}


def get_last_sunday() -> datetime.date:
    """Return the most recent Sunday (America/Chicago)."""
    today = datetime.now(tz=ZoneInfo("America/Chicago"))
    last_sunday = today - timedelta(days=(today.weekday() + 1) % 7)
    return last_sunday.date()


def fetch_all_checkins(date: datetime.date):
    """
    Fetch all check-ins for the given date, including person and event.
    Handles pagination and returns (checkins, included).
    """
    base_url = "https://api.planningcenteronline.com/check-ins/v2/check_ins"
    auth = (settings.PLANNING_CENTER_APP_ID, settings.PLANNING_CENTER_SECRET)
    params = {
        "include": "person,event",
        "where[created_at][gte]": f"{date}T00:00:00Z",
        "where[created_at][lte]": f"{date}T23:59:59Z",
    }

    checkins = []
    included = []
    url = base_url
    first = True

    try:
        while url:
            if first:
                resp = requests.get(
                    url,
                    auth=auth,
                    headers={"Accept": "application/json"},
                    params=params,
                    timeout=15
                )
                first = False
            else:
                resp = requests.get(
                    url,
                    auth=auth,
                    headers={"Accept": "application/json"},
                    timeout=15
                )
            resp.raise_for_status()
            data = resp.json()
            checkins.extend(data.get("data", []))
            included.extend(data.get("included", []))
            url = data.get("links", {}).get("next")
    except Exception as e:
        raise HTTPException(502, f"PCO fetch failed: {e}")

    return checkins, included


def parse_people_data(included: list[dict]) -> dict[str, dict]:
    """
    Extract Person attributes: grade, age, gender.
    Returns mapping person_id -> info dict.
    """
    people = {}
    for item in included:
        if item.get("type") != "Person":
            continue
        pid = item["id"]
        attrs = item.get("attributes", {})
        # Grade
        grade = None
        if attrs.get("grade") is not None:
            try:
                grade = int(attrs["grade"])
            except ValueError:
                pass
        # Age
        age = None
        bd = attrs.get("birthdate")
        if bd:
            try:
                born = datetime.fromisoformat(bd).date()
                age = (datetime.now().date() - born).days // 365
            except Exception:
                pass
        # Gender
        raw_gender = attrs.get("gender")
        gender = raw_gender.lower() if isinstance(raw_gender, str) and raw_gender.strip() else "other"

        people[pid] = {"grade": grade, "age": age, "gender": gender}
    return people


def parse_person_created_dates(included: list[dict]) -> dict[str, datetime.date]:
    """
    Map each person_id to the date their PCO profile was created.
    """
    created_map = {}
    for item in included:
        if item.get("type") == "Person":
            pid = item["id"]
            iso = item.get("attributes", {}).get("created_at")
            if iso:
                created_map[pid] = datetime.fromisoformat(iso.replace("Z", "+00:00")).date()
    return created_map


def parse_event_data(included: list[dict]) -> dict[str, dict]:
    """
    Map event_id -> {dt: start_datetime, name: event_name}
    """
    events = {}
    for item in included:
        if item.get("type") == "Event":
            eid = item["id"]
            attrs = item.get("attributes", {})
            starts = attrs.get("starts_at")
            name = attrs.get("name", "")
            dt = None
            if starts:
                dt = datetime.fromisoformat(starts.replace("Z", "+00:00"))
                dt = dt.astimezone(ZoneInfo("America/Chicago"))
            events[eid] = {"dt": dt, "name": name}
    return events


def determine_ministry(grade: int | None, age: int | None) -> str | None:
    """
    Determine ministry by grade first, then age as fallback.
    """
    if grade is not None:
        if 0 <= grade <= 4:
            return "UpStreet"
        if 5 <= grade <= 8:
            return "Transit"
        if 9 <= grade <= 12:
            return "InsideOut"
    if age is not None:
        if age <= 5:
            return "Waumba Land"
        if 6 <= age <= 10:
            return "UpStreet"
    return None


def determine_service_time(dt: datetime, ministry: str) -> str | None:
    """
    Given an event start or check-in datetime, return service slot label.
    """
    t = dt.time()
    if ministry == "InsideOut":
        if time(15, 15) <= t <= time(17, 30):
            return "4:30 PM"
    else:
        if time(8, 30) <= t <= time(10, 30):
            return "9:30 AM"
        if time(10, 0) <= t <= time(12, 0):
            return "11:00 AM"
    return None


def summarize_checkins_by_ministry(
    checkins: list[dict], people: dict[str, dict],
    person_created: dict[str, datetime.date], events: dict[str, dict]
) -> dict[str, defaultdict[str, int]]:
    """
    Build attendance summaries and first-time counts per ministry.
    """
    summary = {m: defaultdict(int) for m in MINISTRY_COLUMNS}
    for c in checkins:
        pdata = c.get("relationships").get("person", {}).get("data")
        if not pdata or not pdata.get("id"): continue
        pid = pdata["id"]
        pd = people.get(pid)
        if not pd: continue

        ministry = determine_ministry(pd["grade"], pd["age"])
        if ministry is None:
            evt_id = c.get("relationships").get("event", {}).get("data", {}).get("id")
            raw_name = events.get(evt_id, {}).get("name", "")
            for candidate in MINISTRY_COLUMNS:
                if raw_name.startswith(candidate):
                    ministry = candidate
                    break
            if ministry is None:
                print(f"⏩ Skipping {pid}: cannot determine ministry for event name {raw_name!r}")
                continue

        evt_id = c.get("relationships").get("event", {}).get("data", {}).get("id")
        svc_dt = events.get(evt_id, {}).get("dt")
        if not svc_dt:
            svc_dt = datetime.fromisoformat(c["attributes"]["created_at"].replace("Z", "+00:00"))
            svc_dt = svc_dt.astimezone(ZoneInfo("America/Chicago"))

        svc = determine_service_time(svc_dt, ministry)
        if not svc:
            print(f"⏩ Skipping {pid}: service slot not detected (time={svc_dt.time()})")
            continue
        key = SERVICE_KEY_MAP[svc]

        summary[ministry][f"attendance_{key}"] += 1

        svc_date = svc_dt.date()
        if person_created.get(pid) == svc_date:
            summary[ministry][f"new_kids_{key}"] += 1

        gender = pd["gender"]
        if ministry == "Waumba Land":
            age = pd["age"]
            if age is not None:
                bracket = "0_2" if age <= 2 else "3_5" if age <= 5 else None
                if bracket:
                    summary[ministry][f"age_{bracket}_{gender}"] += 1
        else:
            grade = pd["grade"]
            if grade is not None:
                grp = None
                if ministry == "UpStreet":
                    if grade in (0, 1): grp = "k_1"
                    elif grade in (2, 3): grp = "2_3"
                    elif grade in (4, 5): grp = "4_5"
                elif ministry == "Transit":
                    if grade == 6: grp = "6"
                    elif grade == 7: grp = "7"
                    elif grade == 8: grp = "8"
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
    placeholders = ", ".join(["%s"] * len(cols))
    updates = ", ".join([f"{col}=EXCLUDED.{col}" for col in cols if col != "date"])

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"""
        INSERT INTO {table} ({col_sql})
        VALUES ({placeholders})
        ON CONFLICT (date) DO UPDATE SET {updates}
        """, vals
    )
    conn.commit()
    cur.close()
    conn.close()


@router.get("")
async def run_checkin_summary():
    date = get_last_sunday()
    checkins, included = fetch_all_checkins(date)
    people = parse_people_data(included)
    person_created = parse_person_created_dates(included)
    events = parse_event_data(included)
    raw_sum = summarize_checkins_by_ministry(checkins, people, person_created, events)

    debug = {
        "checkins_count": len(checkins),
        "included_count": len(included),
        "summaries": {m: dict(cnts) for m, cnts in raw_sum.items()},
    }

    for m, data in raw_sum.items():
        data["date"] = date
        if m == "InsideOut":
            data["total_attendance"] = data.get("attendance_1630", 0)
            data["new_students"]     = data.get("new_kids_1630", 0)
        else:
            data["total_attendance"] = data.get("attendance_930", 0) + data.get("attendance_1100", 0)
            data["total_new_kids"] = data.get("new_kids_930", 0) + data.get("new_kids_1100", 0)
        if data.get("total_attendance", 0) == 0:
            continue
        data["notes"] = None
        for col in MINISTRY_COLUMNS[m]:
            data.setdefault(col, 0 if col != "notes" else None)
        insert_summary_into_db(m, data)

    return {"status": "success", "date": str(date), **debug}
