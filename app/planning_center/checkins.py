import requests
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from collections import defaultdict
import io
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
    people = {}
    for item in included:
        if item.get("type") == "Person":
            pid = item["id"]
            people[pid] = item.get("attributes", {})
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
    Determine ministry primarily from grade, fallback to age.
    Final fallback: if age is high school range and grade is missing, assume InsideOut.
    """
    if grade is not None:
        if 0 <= grade <= 5:
            return "UpStreet"
        if grade == 'kinder':
            return "UpStreet"
        if 6 <= grade <= 8:
            return "Transit"
        if 9 <= grade <= 12:
            return "InsideOut"

    if age is not None:
        if age <= 5:
            return "Waumba Land"
        if 6 <= age <= 10:
            return "UpStreet"
        if 11 <= age <= 13:
            return "Transit"
        if 14 <= age <= 19:
            return "InsideOut"  # ← final fallback for high-school-age students

    return None



def determine_service_time(dt: datetime, ministry: str) -> str | None:
    """
    Given event start/check-in time, return service slot ONLY if it's valid for that ministry.
    """
    t = dt.time()

    if ministry == "InsideOut": 
        return "4:30 PM"
        
    if time(15, 15) <= t <= time(17, 30):
        return "4:30 PM"
    else:  # Waumba, UpStreet, Transit
        if time(8, 30) <= t <= time(10, 30):
            return "9:30 AM"
        if time(10, 30) <= t <= time(12, 0):
            return "11:00 AM"

    return None  # Invalid time for this ministry



def summarize_checkins_by_ministry(
    checkins: list[dict], included_map: dict[str, dict],
    person_created: dict[str, datetime.date], events: dict[str, dict]
) -> dict[str, dict]:
    from collections import defaultdict

    summary = {}
    skipped = {
        "no_person": 0,
        "no_person_data": 0,
        "no_event": 0,
        "no_event_time": 0,
        "no_ministry": 0,
        "no_service_time": 0,
        "duplicate_checkin": 0,
    }

    already_counted = set()  # (pid, ministry, key)
    seen_people_keys = {}    # (first, last, birthdate) → pid
    possible_duplicates = defaultdict(set)  # ministry → set of dedup keys

    for c in checkins:
        try:
            pdata = c.get("relationships", {}).get("person", {}).get("data")
            if not pdata or not pdata.get("id"):
                skipped["no_person"] += 1
                continue
            pid = pdata["id"]
            pinfo = included_map.get(pid)
            if not pinfo:
                skipped["no_person_data"] += 1
                continue

            evt_id = c.get("relationships", {}).get("event", {}).get("data", {}).get("id")
            if not evt_id:
                skipped["no_event"] += 1
                continue
            svc_dt = events.get(evt_id, {}).get("dt")
            if not svc_dt:
                try:
                    svc_dt = datetime.fromisoformat(c["attributes"]["created_at"].replace("Z", "+00:00"))
                    svc_dt = svc_dt.astimezone(ZoneInfo("America/Chicago"))
                except:
                    skipped["no_event_time"] += 1
                    continue
            svc_date = svc_dt.date()

            grade = None
            if pinfo.get("grade") is not None:
                raw_grade = pinfo["grade"]
                try:
                    if raw_grade == "kinder":
                        grade = 0
                    else:
                        grade = int(raw_grade)
                except (ValueError, TypeError):
                    pass
            age = None
            bd = pinfo.get("birthdate")
            if bd:
                try:
                    born = datetime.fromisoformat(bd).date()
                    age = (svc_date - born).days // 365
                except:
                    pass

            ministry = determine_ministry(grade, age)
            if ministry is None:
                raw_name = events.get(evt_id, {}).get("name", "")
                for candidate in MINISTRY_COLUMNS:
                    if candidate.lower() in raw_name.lower():
                        ministry = candidate
                        break
                if ministry is None:
                    skipped["no_ministry"] += 1
                    continue

            svc = determine_service_time(svc_dt, ministry)
            if not svc:
                skipped["no_service_time"] += 1
                continue
            key = SERVICE_KEY_MAP[svc]

            checkin_key = (pid, ministry, key)
            if checkin_key in already_counted:
                skipped["duplicate_checkin"] += 1
                continue
            already_counted.add(checkin_key)

            dedup_key = (
                pinfo.get("first_name", "").strip().lower(),
                pinfo.get("last_name", "").strip().lower(),
                pinfo.get("birthdate")
            )
            if dedup_key in seen_people_keys and seen_people_keys[dedup_key] != pid:
                possible_duplicates[ministry].add(dedup_key)
            else:
                seen_people_keys[dedup_key] = pid

            if ministry not in summary:
                summary[ministry] = {
                    "breakdown": defaultdict(int),
                    "counted_ids": set(),
                }

            summary[ministry]["counted_ids"].add(pid)
            summary[ministry]["breakdown"][f"attendance_{key}"] += 1

            if person_created.get(pid) == svc_date:
                summary[ministry]["breakdown"][f"new_kids_{key}"] += 1

            raw_gender = pinfo.get("gender")
            gender = raw_gender.lower() if isinstance(raw_gender, str) and raw_gender.strip() else "other"

            if ministry == "UpStreet":
                grp = None
                if grade in (0, 1): grp = "k_1"
                elif grade in (2, 3): grp = "2_3"
                elif grade in (4, 5): grp = "4_5"
                if grp:
                    demo_col = f"grade_{grp}_{gender}"
                    summary[ministry]["breakdown"][demo_col] += 1

            elif ministry == "Waumba Land":
                bracket = None
                if grade == -1:  # Pre-K from grade
                    bracket = "3_5"
                elif age is not None:
                    if age <= 2:
                        bracket = "0_2"
                    elif age <= 5:
                        bracket = "3_5"
                if bracket:
                    demo_col = f"age_{bracket}_{gender}"
                    summary[ministry]["breakdown"][demo_col] += 1

        except Exception:
            continue

    # print("\n🔍 Skip Reasons Summary:")
    # for reason, count in skipped.items():
    #     print(f" - {reason}: {count}")

    # print("\n🧹 Possible Duplicate Profiles Detected:")
    # for ministry, dups in possible_duplicates.items():
    #     print(f" - {ministry}: {len(dups)} potential duplicates")

    # print("\n📋 WRAP-UP SUMMARY")
    # print("--------------------------")
    # for ministry, data in summary.items():
    #     total_count = sum(
    #         count for key, count in data["breakdown"].items() if key.startswith("attendance_")
    #     )
    #     unique_ids = len(data["counted_ids"])
    #     possible_dupes = len(possible_duplicates.get(ministry, set()))

    #     print(f"📍 {ministry}")
    #     print(f"   ✅ Total Counted: {total_count}")
    #     print(f"   👤 Unique People IDs Counted: {unique_ids}")
    #     print(f"   🧹 Possible Duplicates: {possible_dupes}\n")

    output = io.StringIO()
    output.write("\n🔍 Uncounted Planning Center Checkins:\n")
    output.write(f"\n\n📦 Raw check-ins received from API: {len(checkins)}\n\n")
    for reason, count in skipped.items():
        output.write(f"- {reason}: {count}\n")

    output.write("\n\n🧹 Possible Duplicate Profiles Detected:\n")
    for ministry, dups in possible_duplicates.items():
        output.write(f"- {ministry}: {len(dups)} potential duplicates\n")

    output.write("\n📋 WRAP-UP SUMMARY\n--------------------------\n")
    for ministry, data in summary.items():
        total_count = sum(
            count for key, count in data["breakdown"].items() if key.startswith("attendance_")
        )
        unique_ids = len(data["counted_ids"])
        possible_dupes = len(possible_duplicates.get(ministry, set()))
        output.write(f"📍 {ministry}\n")
        output.write(f"   ✅ Total Counted: {total_count}\n")
        output.write(f"   👤 Unique People IDs Counted: {unique_ids}\n")
        output.write(f"   🧹 Possible Duplicates: {possible_dupes}\n\n")
    return (
        {k: v["breakdown"] for k, v in summary.items()},
        output.getvalue().strip()
    )


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
async def run_checkin_summary(date: str | None = None):
    if date:
        date = datetime.fromisoformat(date).date()
    else:
        date = get_last_sunday()

    checkins, included = fetch_all_checkins(date)

    people = parse_people_data(included)
    person_created = parse_person_created_dates(included)
    events = parse_event_data(included)

    # 🧠 Always generate this
    raw_sum, debug_text = summarize_checkins_by_ministry(checkins, people, person_created, events)

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

    return {
        "status": "success",
        "date": str(date),
        "checkins_count": len(checkins),
        "included_count": len(included),
        "summaries": {m: dict(cnts) for m, cnts in raw_sum.items()},
        "debug_text": debug_text  # ✅ always included now
    }


