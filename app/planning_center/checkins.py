# app/planning_center/checkins.py
from __future__ import annotations

from datetime import datetime, timedelta, time, date as Date
from zoneinfo import ZoneInfo
from collections import defaultdict
from typing import Dict, List, Set

from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session

from app.config import settings
from app.db import get_conn, get_db
from app.utils.common import paginate_next_links, get_last_sunday_cst
from app.planning_center.oauth_routes import get_pco_headers

router = APIRouter(prefix="/planning-center/checkins", tags=["Planning Center"])

# Map service labels to summary keys
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
        "grade_9_male", "grade_9_female",
        "grade_10_male", "grade_10_female",
        "grade_11_male", "grade_11_female",
        "grade_12_male", "grade_12_female",
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Fetch layer (uses shared paginator)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_all_checkins(target_date: Date, db: Session) -> tuple[list[dict], list[dict]]:
    """
    Fetch all check-ins for the given date, including person and event.
    Handles pagination and returns (checkins, included).
    """
    url = f"{settings.PLANNING_CENTER_BASE_URL}/check-ins/v2/check_ins"
    params = {
        "include": "person,event",
        "where[created_at][gte]": f"{target_date}T00:00:00Z",
        "where[created_at][lte]": f"{target_date}T23:59:59Z",
        "per_page": 100,
    }

    checkins: list[dict] = []
    included: list[dict] = []
    headers = get_pco_headers(db)

    try:
        for page in paginate_next_links(url, headers=headers, params=params):
            checkins.extend(page.get("data", []))
            included.extend(page.get("included", []))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"PCO fetch failed: {e}")

    return checkins, included


# ─────────────────────────────────────────────────────────────────────────────
# Included object parsers
# ─────────────────────────────────────────────────────────────────────────────
def parse_people_data(included: list[dict]) -> dict[str, dict]:
    people: dict[str, dict] = {}
    for item in included:
        if item.get("type") == "Person":
            pid = item["id"]
            people[pid] = item.get("attributes", {}) or {}
    return people


def parse_person_created_dates(included: list[dict]) -> dict[str, datetime.date]:
    """
    Map each person_id to the date their PCO profile was created.
    """
    created_map: dict[str, datetime.date] = {}
    for item in included:
        if item.get("type") == "Person":
            pid = item["id"]
            iso = (item.get("attributes") or {}).get("created_at")
            if iso:
                try:
                    created_map[pid] = datetime.fromisoformat(iso.replace("Z", "+00:00")).date()
                except Exception:
                    pass
    return created_map


def parse_event_data(included: list[dict]) -> dict[str, dict]:
    """
    Map event_id -> {dt: start_datetime_in_CST, name: event_name}
    """
    events: dict[str, dict] = {}
    for item in included:
        if item.get("type") == "Event":
            eid = item["id"]
            attrs = item.get("attributes", {}) or {}
            starts = attrs.get("starts_at")
            name = attrs.get("name", "") or ""
            dt = None
            if starts:
                try:
                    dt = datetime.fromisoformat(starts.replace("Z", "+00:00")).astimezone(ZoneInfo("America/Chicago"))
                except Exception:
                    dt = None
            events[eid] = {"dt": dt, "name": name}
    return events


# ─────────────────────────────────────────────────────────────────────────────
# Business rules
# ─────────────────────────────────────────────────────────────────────────────
def determine_ministry(grade: int | None, age: int | None) -> str | None:
    """
    Determine ministry primarily from grade, fallback to age.
    Final fallback: if age is high school range and grade is missing, assume InsideOut.
    """
    if grade is not None:
        if 0 <= grade <= 5:
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
            return "InsideOut"  # final fallback for high-school-age students

    return None


def determine_service_time(dt: datetime, ministry: str) -> str | None:
    """
    Given event start/check-in time, return service slot ONLY if it's valid for that ministry.
    """
    if dt is None:
        return None

    t = dt.time()

    # InsideOut always counted at 4:30 PM if present
    if ministry == "InsideOut":
        return "4:30 PM"

    # General windows
    if time(15, 15) <= t <= time(17, 30):
        return "4:30 PM"
    else:
        if time(8, 0) <= t <= time(10, 15):
            return "9:30 AM"
        if time(10, 15) <= t <= time(12, 30):
            return "11:00 AM"

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Summarization
# ─────────────────────────────────────────────────────────────────────────────
def summarize_checkins_by_ministry(
    checkins: list[dict],
    included_map: dict[str, dict],
    person_created: dict[str, datetime.date],
    events: dict[str, dict],
) -> dict[str, dict]:
    summary: dict[str, dict] = {}
    skipped = {
        "no_person": 0,
        "no_person_data": 0,
        "no_event": 0,
        "no_event_time": 0,
        "no_ministry": 0,
        "no_service_time": 0,
        "duplicate_checkin": 0,
    }
    skip_details: list[dict] = []  # (name, ID, reason)
    already_counted: Set[tuple[str, str, str]] = set()  # (pid, ministry, key)
    seen_people_keys: dict[tuple[str, str, str | None], str] = {}  # (first,last,birthdate) → pid
    possible_duplicates: dict[str, set] = defaultdict(set)  # ministry → set of dedup keys

    for c in checkins:
        try:
            pdata = (c.get("relationships") or {}).get("person", {}).get("data")
            if not pdata or not pdata.get("id"):
                skipped["no_person"] += 1
                continue
            pid = pdata["id"]
            pinfo = included_map.get(pid)
            if not pinfo:
                skipped["no_person_data"] += 1
                continue

            evt_id = (c.get("relationships") or {}).get("event", {}).get("data", {}).get("id")
            if not evt_id:
                skipped["no_event"] += 1
                continue

            svc_dt = (events.get(evt_id) or {}).get("dt")
            if not svc_dt:
                try:
                    created_iso = (c.get("attributes") or {}).get("created_at")
                    if created_iso:
                        svc_dt = datetime.fromisoformat(created_iso.replace("Z", "+00:00")).astimezone(ZoneInfo("America/Chicago"))
                except Exception:
                    svc_dt = None
            if not svc_dt:
                skipped["no_event_time"] += 1
                continue

            svc_date = svc_dt.date()

            # Grade parsing (accepts "kinder" as 0)
            grade = None
            raw_grade = pinfo.get("grade")
            if raw_grade is not None:
                try:
                    grade = 0 if raw_grade == "kinder" else int(raw_grade)
                except (ValueError, TypeError):
                    grade = None

            # Age from birthdate (approx years)
            age = None
            bd = pinfo.get("birthdate")
            if bd:
                try:
                    born = datetime.fromisoformat(bd).date()
                    age = (svc_date - born).days // 365
                except Exception:
                    age = None

            ministry = determine_ministry(grade, age)
            if ministry is None:
                raw_name = (events.get(evt_id) or {}).get("name", "")
                for candidate in MINISTRY_COLUMNS:
                    if candidate.lower() in (raw_name or "").lower():
                        ministry = candidate
                        break
                if ministry is None:
                    skipped["no_ministry"] += 1
                    skip_details.append({
                        "person_id": pid,
                        "reason": "no ministry",
                        "name": f"{pinfo.get('first_name','')} {pinfo.get('last_name','')}".strip(),
                        "email": pinfo.get("email_address") or pinfo.get("email"),
                        "phone": pinfo.get("phone_number") or pinfo.get("mobile_phone"),
                    })
                    continue

            svc = determine_service_time(svc_dt, ministry)
            if not svc:
                skipped["no_service_time"] += 1
                skip_details.append({
                    "person_id": pid,
                    "reason": "no service time",
                    "name": f"{pinfo.get('first_name','')} {pinfo.get('last_name','')}".strip(),
                    "email": pinfo.get("email_address") or pinfo.get("email"),
                    "phone": pinfo.get("phone_number") or pinfo.get("mobile_phone"),
                })
                continue

            key = SERVICE_KEY_MAP[svc]
            checkin_key = (pid, ministry, key)
            if checkin_key in already_counted:
                skipped["duplicate_checkin"] += 1
                skip_details.append({
                    "person_id": pid,
                    "reason": "duplicate checkin",
                    "name": f"{pinfo.get('first_name','')} {pinfo.get('last_name','')}".strip(),
                    "email": pinfo.get("email_address") or pinfo.get("email"),
                    "phone": pinfo.get("phone_number") or pinfo.get("mobile_phone"),
                })
                continue
            already_counted.add(checkin_key)

            # Cross-person dupe detection (same name + birthdate → different pid)
            dedup_key = (
                (pinfo.get("first_name") or "").strip().lower(),
                (pinfo.get("last_name") or "").strip().lower(),
                pinfo.get("birthdate"),
            )
            if dedup_key in seen_people_keys and seen_people_keys[dedup_key] != pid:
                possible_duplicates[ministry].add(dedup_key)
            else:
                seen_people_keys[dedup_key] = pid

            if ministry not in summary:
                summary[ministry] = {"breakdown": defaultdict(int), "counted_ids": set()}

            summary[ministry]["counted_ids"].add(pid)
            summary[ministry]["breakdown"][f"attendance_{key}"] += 1
            summary[ministry]["breakdown"]["total_attendance"] += 1

            # New kids/students logic
            if person_created.get(pid) == svc_date:
                summary[ministry]["breakdown"][f"new_kids_{key}"] += 1
                if ministry == "InsideOut":
                    summary[ministry]["breakdown"]["new_students"] += 1
                else:
                    summary[ministry]["breakdown"]["total_new_kids"] += 1

            # Gender tally by ministry-specific buckets
            raw_gender = pinfo.get("gender")
            gender = raw_gender.lower() if isinstance(raw_gender, str) and raw_gender.strip() else "other"

            if ministry == "UpStreet":
                grp = None
                if grade in (0, 1): grp = "k_1"
                elif grade in (2, 3): grp = "2_3"
                elif grade in (4, 5): grp = "4_5"
                if grp:
                    summary[ministry]["breakdown"][f"grade_{grp}_{gender}"] += 1

            elif ministry == "Waumba Land":
                bracket = None
                if grade == -1:      # Pre-K from grade (kept for parity)
                    bracket = "3_5"
                elif age is not None:
                    if age <= 2:
                        bracket = "0_2"
                    elif age <= 5:
                        bracket = "3_5"
                if bracket:
                    summary[ministry]["breakdown"][f"age_{bracket}_{gender}"] += 1

            elif ministry == "Transit":
                if grade in (6, 7, 8):
                    summary[ministry]["breakdown"][f"grade_{grade}_{gender}"] += 1

            elif ministry == "InsideOut":
                if grade is not None and 9 <= grade <= 12:
                    summary[ministry]["breakdown"][f"grade_{grade}_{gender}"] += 1

        except Exception:
            # Keep behavior: skip hard errors on a single row
            continue

    return {
        "breakdown": {k: v["breakdown"] for k, v in summary.items()},
        "uncounted_reasons": skipped,
        "skip_details": skip_details,
        "possible_duplicates": {m: len(dups) for m, dups in possible_duplicates.items()},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Persistence
# ─────────────────────────────────────────────────────────────────────────────
def insert_summary_into_db(ministry: str, data: dict):
    table_map = {
        "Waumba Land": "waumbaland_attendance",
        "UpStreet":    "upstreet_attendance",
        "Transit":     "transit_attendance",
        "InsideOut":   "insideout_attendance",
    }
    table = table_map[ministry]
    cols = ["date"] + MINISTRY_COLUMNS[ministry]
    vals = [data.get(col) for col in cols]
    col_sql = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    updates = ", ".join([f"{col}=EXCLUDED.{col}" for col in cols if col != "date"])

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            INSERT INTO {table} ({col_sql})
            VALUES ({placeholders})
            ON CONFLICT (date) DO UPDATE SET {updates}
            """,
            vals,
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Route (name intact)
# ─────────────────────────────────────────────────────────────────────────────
@router.get("", response_model=dict)
async def run_checkin_summary(
    date: str | None = None,
    db: Session = Depends(get_db),
):
    if date:
        as_date = datetime.fromisoformat(date).date()
    else:
        as_date = get_last_sunday_cst()

    checkins, included = fetch_all_checkins(as_date, db)
    people = parse_people_data(included)
    person_created = parse_person_created_dates(included)
    events = parse_event_data(included)

    result = summarize_checkins_by_ministry(checkins, people, person_created, events)
    processed_count = sum(
        breakdown_dict.get("total_attendance", 0)
        for breakdown_dict in result["breakdown"].values()
    )

    # Persist per-ministry summaries
    for ministry, data in result["breakdown"].items():
        data["date"] = as_date
        insert_summary_into_db(ministry, data)

    return {
        "status": "success",
        "date": str(as_date),
        "checkins_count": processed_count,
        **result,
    }
