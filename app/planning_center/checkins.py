# app/planning_center/checkins.py
from __future__ import annotations
import logging
from datetime import datetime, timezone, timedelta, time, date as Date
from zoneinfo import ZoneInfo
from collections import defaultdict
from typing import Dict, List, Set

from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session

from app.config import settings
from app.db import get_conn, get_db
from app.utils.common import paginate_next_links, get_last_sunday_cst
from app.planning_center.oauth_routes import get_pco_headers

log = logging.getLogger(__name__)
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

FACT_BATCH_SIZE = 1000  # tune as you like

def _parse_iso(ts: str | None):
    if not ts:
        return None
    try:
        # PCO timestamps are ISO8601 with Z
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def upsert_f_checkins_person(
    rows: list[tuple],
    *,
    chunk_size: int = 1000,
    log_chunks: bool = False,
) -> int:
    """
    rows: (person_id, svc_date, service_time, ministry, event_id, campus_id, created_at_utc)
    Conflict key: (person_id, svc_date, ministry, service_time)

    Behavior preserved:
      - chunked executemany with optional logging
      - event_id: prefer newly provided value if existing is NULL
      - campus_id: keep existing if present, otherwise take new
      - created_at_utc: keep the earliest (LEAST) non-null timestamp

    New:
      - Ensures all person_ids exist in pco_people before insert (FK-safe).
    """
    if not rows:
        return 0

    # ── NEW: seed any missing people to satisfy FK on f_checkins_person.person_id
    try:
        person_ids = {r[0] for r in rows if r and r[0]}
        if person_ids:
            seeded = _ensure_people_exist(person_ids)  # assumes helper exists in this module
            if seeded and log_chunks:
                log.info("[checkins] pco_people seeded=%d (pre-flight for person_facts batch)", seeded)
    except Exception as e:
        # Don't fail the whole request if seeding hiccups; we'll try the upsert and let FK surface if real
        log.warning("[checkins] warning while seeding pco_people: %s", e)

    conn = get_conn()
    cur = conn.cursor()
    total_affected = 0

    try:
        if log_chunks:
            log.info("[checkins] upserting %d f_checkins_person rows (chunk_size=%d)", len(rows), chunk_size)

        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]

            cur.executemany(
                """
                INSERT INTO f_checkins_person
                  (person_id, svc_date, service_time, ministry, event_id, campus_id, created_at_utc)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (person_id, svc_date, ministry, service_time) DO UPDATE SET
                  event_id = COALESCE(EXCLUDED.event_id, f_checkins_person.event_id),
                  campus_id = COALESCE(f_checkins_person.campus_id, EXCLUDED.campus_id),
                  created_at_utc = LEAST(
                    COALESCE(f_checkins_person.created_at_utc, EXCLUDED.created_at_utc),
                    COALESCE(EXCLUDED.created_at_utc, f_checkins_person.created_at_utc)
                  )
                """,
                chunk,
            )
            affected = cur.rowcount
            total_affected += affected

            if log_chunks:
                log.info(
                    "[checkins] upsert chunk %d-%d size=%d affected=%d",
                    i, i + len(chunk) - 1, len(chunk), affected
                )

        conn.commit()
        return total_affected

    finally:
        cur.close()
        conn.close()


    return total_affected

def _ensure_people_exist(person_ids: set[str]) -> int:
    """
    Make sure every id in person_ids exists in pco_people.
    Inserts minimal rows (person_id only) for any missing ids.
    Returns the number of rows inserted.
    """
    if not person_ids:
        return 0
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT person_id FROM pco_people WHERE person_id = ANY(%s);", (list(person_ids),))
        existing = {r[0] for r in cur.fetchall()}
        missing = [pid for pid in person_ids if pid not in existing]
        if missing:
            cur.executemany(
                "INSERT INTO pco_people (person_id) VALUES (%s) ON CONFLICT DO NOTHING;",
                [(pid,) for pid in missing],
            )
        conn.commit()
        return len(missing)
    finally:
        cur.close()
        conn.close()


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
    collect_person_facts: bool = False,  # NEW: when True, returns "person_fact_rows"
) -> dict[str, dict]:
    """
    Build ministry/service-time summaries (existing behavior) and, optionally,
    collect person-level fact rows for f_checkins_person using the *same* de-dupe
    rules you already trust.

    Returns:
      {
        "breakdown": { ministry: {metric: count, ...}, ... },
        "uncounted_reasons": {...},
        "skip_details": [...],
        "possible_duplicates": { ministry: N, ... },
        # when collect_person_facts=True:
        "person_fact_rows": [
           (person_id, svc_date, service_time_code, ministry, event_id, campus_id, created_at_utc), ...
        ]
      }
    """
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
    skip_details: list[dict] = []
    already_counted: Set[tuple[str, str, str]] = set()  # (pid, ministry, key)
    seen_people_keys: dict[tuple[str, str, str | None], str] = {}  # (first,last,birthdate) → pid
    possible_duplicates: dict[str, set] = defaultdict(set)  # ministry → set of dedup keys
    person_fact_rows: list[tuple] = [] if collect_person_facts else []

    for c in checkins:
        try:
            # ── Person presence ────────────────────────────────────────────────
            pdata = (c.get("relationships") or {}).get("person", {}).get("data")
            if not pdata or not pdata.get("id"):
                skipped["no_person"] += 1
                continue
            pid = pdata["id"]
            pinfo = included_map.get(pid)
            if not pinfo:
                skipped["no_person_data"] += 1
                continue

            # ── Event presence ─────────────────────────────────────────────────
            evt_id = (c.get("relationships") or {}).get("event", {}).get("data", {}).get("id")
            if not evt_id:
                skipped["no_event"] += 1
                continue

            svc_dt = (events.get(evt_id) or {}).get("dt")
            if not svc_dt:
                # Fallback to check-in created time (convert to America/Chicago)
                try:
                    created_iso = (c.get("attributes") or {}).get("created_at")
                    if created_iso:
                        svc_dt = datetime.fromisoformat(created_iso.replace("Z", "+00:00")).astimezone(
                            ZoneInfo("America/Chicago")
                        )
                except Exception:
                    svc_dt = None
            if not svc_dt:
                skipped["no_event_time"] += 1
                continue
            svc_date = svc_dt.date()

            # ── Grade & Age parsing ────────────────────────────────────────────
            grade = None
            raw_grade = pinfo.get("grade")
            if raw_grade is not None:
                try:
                    grade = 0 if raw_grade == "kinder" else int(raw_grade)
                except (ValueError, TypeError):
                    grade = None

            age = None
            bd = pinfo.get("birthdate")
            if bd:
                try:
                    born = datetime.fromisoformat(bd).date()
                    age = (svc_date - born).days // 365
                except Exception:
                    age = None

            # ── Ministry inference (your existing rules + fallback by event name) ─
            ministry = determine_ministry(grade, age)
            if ministry is None:
                raw_name = (events.get(evt_id) or {}).get("name", "") or ""
                for candidate in MINISTRY_COLUMNS:
                    if candidate.lower() in raw_name.lower():
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

            # ── Service time mapping ──────────────────────────────────────────
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
            key = SERVICE_KEY_MAP[svc]  # e.g., "930" | "1100" | "1630"

            # ── Per-person/ministry/service de-dupe for the day ───────────────
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

            # ── Cross-person dupe detection (FYI only) ────────────────────────
            dedup_key = (
                (pinfo.get("first_name") or "").strip().lower(),
                (pinfo.get("last_name") or "").strip().lower(),
                pinfo.get("birthdate"),
            )
            if dedup_key in seen_people_keys and seen_people_keys[dedup_key] != pid:
                possible_duplicates[ministry].add(dedup_key)
            else:
                seen_people_keys[dedup_key] = pid

            # ── Initialize ministry bucket ────────────────────────────────────
            if ministry not in summary:
                summary[ministry] = {"breakdown": defaultdict(int), "counted_ids": set()}

            # ── Counted attendance (your core behavior) ───────────────────────
            summary[ministry]["counted_ids"].add(pid)
            summary[ministry]["breakdown"][f"attendance_{key}"] += 1
            summary[ministry]["breakdown"]["total_attendance"] += 1

            # ── New kids/students logic ───────────────────────────────────────
            if person_created.get(pid) == svc_date:
                summary[ministry]["breakdown"][f"new_kids_{key}"] += 1
                if ministry == "InsideOut":
                    summary[ministry]["breakdown"]["new_students"] += 1
                else:
                    summary[ministry]["breakdown"]["total_new_kids"] += 1

            # ── Gender tallies by ministry buckets ────────────────────────────
            raw_gender = pinfo.get("gender")
            gender = raw_gender.lower() if isinstance(raw_gender, str) and raw_gender.strip() else "other"

            if ministry == "UpStreet":
                grp = None
                if grade in (0, 1):
                    grp = "k_1"
                elif grade in (2, 3):
                    grp = "2_3"
                elif grade in (4, 5):
                    grp = "4_5"
                if grp:
                    summary[ministry]["breakdown"][f"grade_{grp}_{gender}"] += 1

            elif ministry == "Waumba Land":
                bracket = None
                if grade == -1:  # Pre-K from grade (kept for parity)
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

            # ── OPTIONAL: collect person-fact rows for f_checkins_person ──────
            if collect_person_facts:
                created_iso = (c.get("attributes") or {}).get("created_at") or (c.get("attributes") or {}).get("checked_in_at")
                created_at_utc = None
                if created_iso:
                    try:
                        created_at_utc = datetime.fromisoformat(created_iso.replace("Z", "+00:00")).astimezone(
                            ZoneInfo("UTC")
                        )
                    except Exception:
                        created_at_utc = None

                person_fact_rows.append((
                    pid,
                    svc_date,
                    key,        # service_time code stored as your canonical key (e.g., "930")
                    ministry,
                    evt_id,
                    None,       # campus_id (future-ready)
                    created_at_utc,
                ))

        except Exception:
            # Keep behavior: skip hard errors on a single row
            continue

    result = {
        "breakdown": {k: v["breakdown"] for k, v in summary.items()},
        "uncounted_reasons": skipped,
        "skip_details": skip_details,
        "possible_duplicates": {m: len(dups) for m, dups in possible_duplicates.items()},
    }
    if collect_person_facts:
        result["person_fact_rows"] = person_fact_rows
    return result


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
    write_person_facts: bool = True,    
    log_person_facts: bool = False,   # NEW: toggle writing f_checkins_person
    db: Session = Depends(get_db),
):
    # Resolve target date (your existing logic)
    if date:
        as_date = datetime.fromisoformat(date).date()
    else:
        as_date = get_last_sunday_cst()

    # Fetch & parse (your existing helpers)
    checkins, included = fetch_all_checkins(as_date, db)
    people = parse_people_data(included)
    person_created = parse_person_created_dates(included)
    events = parse_event_data(included)

    # Summarize (now collecting person facts when enabled)
    result = summarize_checkins_by_ministry(
        checkins=checkins,
        included_map=people,
        person_created=person_created,
        events=events,
        collect_person_facts=write_person_facts,   # NEW
    )

    # Compute processed count (unchanged)
    processed_count = sum(
        breakdown_dict.get("total_attendance", 0)
        for breakdown_dict in result["breakdown"].values()
    )

    # Persist per-ministry summaries (unchanged)
    for ministry, data in result["breakdown"].items():
        data["date"] = as_date
        insert_summary_into_db(ministry, data)

    # NEW: write person-level facts if requested
    person_facts_attempted = 0
    person_facts_inserted  = 0
    if write_person_facts:
        rows = result.pop("person_fact_rows", [])
        person_facts_attempted = len(rows)
        if rows:
            person_facts_inserted = upsert_f_checkins_person(rows, log_chunks=log_person_facts)
            log.info(
                "[checkins] f_checkins_person attempted=%s affected=%s for %s",
                person_facts_attempted, person_facts_inserted, as_date
            )

    return {
        "status": "success",
        "date": str(as_date),
        "checkins_count": processed_count,
        "person_facts_attempted": person_facts_attempted,   # NEW
        "person_facts_inserted": person_facts_inserted,     # existing
        **result,
    }

