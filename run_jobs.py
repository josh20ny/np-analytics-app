# run_jobs.py
import os, time, json, logging, requests, argparse
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, date
from sqlalchemy import text, bindparam
from sqlalchemy.types import DATE
from datetime import date
import math

from dotenv import load_dotenv
from app.db import SessionLocal
from clickup_app.assistant_client import run_assistant_with_tools
from clickup_app.clickup_client import post_message, send_dm

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL                 = os.getenv("API_BASE_URL")
CLICKUP_WORKSPACE_ID     = os.getenv("CLICKUP_WORKSPACE_ID", "")
CLICKUP_TEAM_CHANNEL_ID  = os.getenv("CLICKUP_TEAM_CHANNEL_ID", "")

# Support either a single user var or comma list (back-compat with your envs)
CLICKUP_JOSH_USER_ID     = os.getenv("CLICKUP_JOSH_USER_ID") or os.getenv("CLICKUP_JOSH_CHANNEL_ID", "")
CLICKUP_DM_USER_IDS      = [s.strip() for s in os.getenv("CLICKUP_DM_USER_IDS", "").split(",") if s.strip()]
if CLICKUP_JOSH_USER_ID and CLICKUP_JOSH_USER_ID not in CLICKUP_DM_USER_IDS:
    CLICKUP_DM_USER_IDS.append(CLICKUP_JOSH_USER_ID)

CENTRAL_TZ = ZoneInfo("America/Chicago")

# Keep your original working routes (used in the collection phase)
JOBS = [
    ("/attendance/process-sheet",               "Adult attendance processing"),
    ("/planning-center/checkins",               "Planning Center check-ins"),
    ("/planning-center/giving/weekly-summary",  "Planning Center Giving Summary"),
    ("/planning-center/groups",                 "Planning Center Groups"),
    ("/planning-center/serving/summary",        "Planning Center Volunteer Summary"),
    ("/youtube/weekly-summary",                 "YouTube weekly summary"),
    ("/youtube/livestreams",                    "YouTube livestream tracking"),
    ("/mailchimp/weekly-summary",               "Mailchimp weekly summary"),
    (f"/mailchimp/weekly-refresh",              "Mailchimp weekly refresh POST"),
]

# Endpoint-specific read timeouts (seconds)
TIMEOUTS = {
    "People sync": 7200,        # can be long on first run; weekly with since=Mon is fast
    "Groups/memberships sync": 3600,
    "Serving teams/memberships sync": 1800,
    "Check-ins ingest (last Sunday)": 1200,
    "Giving summary (last full week)": 900,
    "Cadence rebuild": 2400,
    "Cadence snapshot": 1800,
    "Cadence weekly report": 1800,
    "Mailchimp weekly refresh": 900,
}

# ── helpers ───────────────────────────────────────────────────────────────────
def week_bounds_for(week_end: date) -> tuple[date, date]:
    """Given a Sunday (CST), return its Mon–Sun window."""
    week_start = week_end - timedelta(days=6)
    return week_start, week_end

def last_monday_and_sunday_cst(override_week_end: date | None = None) -> tuple[date, date]:
    """Return the last full Mon–Sun window in America/Chicago, or the window for override_week_end."""
    if override_week_end:
        return week_bounds_for(override_week_end)
    now_cst = datetime.now(CENTRAL_TZ).date()
    last_sun = now_cst - timedelta(days=(now_cst.weekday() + 1))
    last_mon = last_sun - timedelta(days=6)
    return last_mon, last_sun

def resolve_week_window(week_end_str: str | None) -> tuple[date, date]:
    """
    If week_end_str (YYYY-MM-DD) is provided, use that Sunday as week_end.
    If it is not a Sunday, snap to the previous Sunday and log a warning.
    Otherwise, return the last full Mon–Sun window (default behavior).
    """
    if not week_end_str:
        return last_monday_and_sunday_cst()

    try:
        week_end = datetime.strptime(week_end_str, "%Y-%m-%d").date()
    except ValueError:
        raise SystemExit(f"Invalid --week-end date: {week_end_str!r}. Use YYYY-MM-DD.")

    if week_end.weekday() != 6:  # Sunday == 6
        snap = week_end - timedelta(days=(week_end.weekday() + 1))
        log.warning("Provided week_end %s is not a Sunday; using previous Sunday %s.", week_end, snap)
        week_end = snap

    week_start = week_end - timedelta(days=6)
    return week_start, week_end

def _json_default(o):
    from datetime import date, datetime, time
    if isinstance(o, (datetime, date, time)):
        return o.isoformat()
    return str(o)


# ── Logging ───────────────────────────────────────────────────────────────────
log = logging.getLogger("run_jobs")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s run_jobs: %(message)s"
)

# ── HTTP helpers ──────────────────────────────────────────────────────────────
def warmup_base_url():
    url = f"{BASE_URL.rstrip('/')}/docs"
    max_attempts = int(os.getenv("WARMUP_RETRIES", "12"))  # ~1 min total
    for i in range(max_attempts):
        try:
            r = requests.get(url, timeout=(5, 10))
            if r.status_code == 200:
                log.info("🌐 Warm-up OK (%s)", r.status_code)
                return True
            log.warning("🌐 Warm-up got %s (attempt %d/%d)", r.status_code, i+1, max_attempts)
        except Exception as e:
            log.warning("🌐 Warm-up error %s (attempt %d/%d)", e.__class__.__name__, i+1, max_attempts)
        time.sleep(5)
    log.error("🌐 Warm-up failed after %d attempts", max_attempts)
    return False

def call_job(endpoint: str, label: str, timeout_s: int | None = None) -> str:
    url = f"{BASE_URL.rstrip('/')}{endpoint}"
    to = timeout_s or TIMEOUTS.get(label, 600)
    timeout = (10, to)

    # Retry policy: up to 5 tries on 502/503/504 or connection errors
    max_tries = 5
    for attempt in range(1, max_tries + 1):
        t0 = time.perf_counter()
        log.info("📡 Calling: %s – %s (timeout=%ss try=%d/%d)", endpoint, label, to, attempt, max_tries)
        try:
            r = requests.get(url, timeout=timeout)
            elapsed = time.perf_counter() - t0
            if r.status_code == 200:
                log.info("✅ %s finished (%s) in %.1fs", label, r.status_code, elapsed)
                return r.text or ""
            if r.status_code in (502, 503, 504):
                log.warning("↩️ %s got %s in %.1fs, will retry", label, r.status_code, elapsed)
                # jittered backoff: 0.5, 1, 2, 4, 8s
                time.sleep(0.5 * (2 ** (attempt-1)))
                continue
            log.error("❌ %s failed (%s) in %.1fs: %s", label, r.status_code, elapsed, (r.text or "")[:300])
            return ""
        except (requests.ConnectionError, requests.ReadTimeout) as e:
            elapsed = time.perf_counter() - t0
            log.warning("↩️ %s connection error (%s) in %.1fs, will retry", label, e.__class__.__name__, elapsed)
            time.sleep(0.5 * (2 ** (attempt-1)))
        except Exception:
            elapsed = time.perf_counter() - t0
            log.exception("💥 %s unexpected error after %.1fs", label, elapsed)
            return ""
    log.error("❌ %s exhausted retries", label)
    return ""

def post_job(endpoint: str, label: str, timeout_s: int | None = None) -> str:
    url = f"{BASE_URL.rstrip('/')}{endpoint}"
    to = timeout_s or TIMEOUTS.get(label, 600)
    timeout = (10, to)

    max_tries = 5
    for attempt in range(1, max_tries + 1):
        t0 = time.perf_counter()
        log.info("📡 POSTing: %s – %s (timeout=%ss try=%d/%d)", endpoint, label, to, attempt, max_tries)
        try:
            r = requests.post(url, timeout=timeout)
            elapsed = time.perf_counter() - t0
            if r.status_code == 200:
                log.info("✅ %s finished (%s) in %.1fs", label, r.status_code, elapsed)
                return r.text or ""
            if r.status_code in (502, 503, 504):
                log.warning("↩️ %s got %s in %.1fs, will retry", label, r.status_code, elapsed)
                time.sleep(0.5 * (2 ** (attempt-1)))
                continue
            log.error("❌ %s failed (%s) in %.1fs: %s", label, r.status_code, elapsed, (r.text or "")[:300])
            return ""
        except (requests.ConnectionError, requests.ReadTimeout) as e:
            elapsed = time.perf_counter() - t0
            log.warning("↩️ %s connection error (%s) in %.1fs, will retry", label, e.__class__.__name__, elapsed)
            time.sleep(0.5 * (2 ** (attempt-1)))
        except Exception:
            elapsed = time.perf_counter() - t0
            log.exception("💥 %s unexpected error after %.1fs", label, elapsed)
            return ""
    log.error("❌ %s exhausted retries", label)
    return ""

def fetch_unplaced_for_date(target_sunday: str) -> list[dict]:
    """
    Return unplaced/invalid check-ins for the given CST date from pco_checkins_unplaced.
    """
    rows = []
    with SessionLocal() as db:
        d = date.fromisoformat(target_sunday)  # e.g., "2025-10-19" -> date(2025,10,19)
        sql = text("""
            SELECT
              checkin_id,
              person_id,
              (created_at_pco AT TIME ZONE 'America/Chicago') AS created_at_cst,
              reason_codes,
              details
            FROM pco_checkins_unplaced
            WHERE (created_at_pco AT TIME ZONE 'America/Chicago')::date = :d
            ORDER BY created_at_pco
        """).bindparams(bindparam("d", type_=DATE))
        for r in db.execute(sql, {"d": d}).mappings().all():
            rows.append(dict(r))
    return rows


def _json_or_empty(raw: str) -> dict:
    try:
        return json.loads(raw or "{}")
    except json.JSONDecodeError:
        return {"error": "invalid JSON", "raw": (raw or "")[:3000]}

# ── Cadence readiness polling (ensures DM has people lists) ───────────────────
def fetch_cadence_report(
    target_sunday: str,
    ensure_snapshot_first_attempt: bool = False,  # NEW
    tries: int = 8,
    wait_s: int = 10
) -> dict:
    """
    Poll for the cadence weekly-report once the JSON includes the expected keys.
    Note: an empty 'lapses' list is valid and should be considered ready.
    """
    last_raw = ""
    for attempt in range(1, tries + 1):
        ensure = "true" if (attempt == 1 and ensure_snapshot_first_attempt) else "false"  # CHANGED
        persist = "true" if attempt == 1 else "false"
        endpoint = (
            f"/analytics/cadence/weekly-report?"
            f"week_end={target_sunday}&ensure_snapshot={ensure}&persist_front_door={persist}"
        )
        raw = call_job(endpoint, "Cadence weekly report", TIMEOUTS.get("Cadence weekly report"))
        last_raw = raw or last_raw
        data = _json_or_empty(raw)

        if isinstance(data, dict):
            has_keys = all(k in data for k in ("engaged", "front_door", "lapses"))
            right_week = (data.get("week_end") == target_sunday)
            if has_keys and right_week:
                log.info("✅ Cadence report ready on attempt %d", attempt)
                return data

        log.info("⏳ Cadence report not ready (attempt %d/%d) – waiting %ss", attempt, tries, wait_s)
        time.sleep(wait_s)

    log.warning("⚠️ Cadence report missing sections after %d attempts; proceeding with best effort.", tries)
    return _json_or_empty(last_raw)



# ── Pipeline: facts first → cadence last (blocking) ───────────────────────────
def run_weekly_pipeline(selected_week_end: date | None = None) -> dict:
    """Sequential weekly pipeline with strict 1-week window (Mon–Sun, CST)."""
    last_mon, last_sun = last_monday_and_sunday_cst(selected_week_end)

    log.info("🗓️ Weekly window: %s → %s (CST)", last_mon, last_sun)

    calls = [
        (f"/planning-center/people/sync?since={last_mon}", "People sync"),
        (f"/planning-center/groups/sync?since={last_mon}", "Groups/memberships sync"),
        (f"/planning-center/serving/sync?since={last_mon}", "Serving teams/memberships sync"),
        (f"/planning-center/checkins-location/sync-locations",                                                "PCO locations sync POST"),
        (f"/planning-center/checkins-location/ingest-day?svc_date={last_sun}&write_person_facts=true",       "Check-ins ingest (last Sunday) POST"),
        (f"/planning-center/checkins-location/rollup-day?svc_date={last_sun}&write_legacy=true", "Check-ins rollup (last Sunday) POST"),
        (f"/planning-center/giving/weekly-summary?start={last_mon}&end={last_sun}", "Giving summary (last full week)"),
        (f"/analytics/cadence/snap-week?week_end={last_sun}", "Cadence snapshot"),
    ]

    for endpoint, label in calls:
        if "POST" in label:
            post_job(endpoint, label, TIMEOUTS.get(label))
        else:
            call_job(endpoint, label, TIMEOUTS.get(label))
        time.sleep(1.5)

    # We already snapped and persisted; just read the weekly report.
    cadence = fetch_cadence_report(str(last_sun))
    return cadence

# ── Team prompt (strict order) ────────────────────────────────────────────────
def build_team_prompt(outputs: dict, cadence: dict) -> str:
    engaged     = cadence.get("engaged") or {}
    front_door  = cadence.get("front_door") or {}

    sections = [
        ("Adult Attendance Summary",                           outputs.get("Adult attendance processing")),
        ("Check-ins (Kids & Students Attendance Summaries)",   outputs.get("Planning Center check-ins")),
        ("Giving Summary",                                     outputs.get("Planning Center Giving Summary")),
        ("Volunteering Summary",                               outputs.get("Planning Center Volunteer Summary")),
        ("Groups Summary",                                     outputs.get("Planning Center Groups")),
        ("Engaged Summary",                                    engaged or {"note": "no engaged data"}),
        ("Front Door Summary",                                 front_door or {"note": "no front door data"}),
    ]
    parts = [
        "You are NP Analytics’ reporting assistant.",
        "Compose a clear, concise weekly update for the team. Use the sections below in THIS EXACT ORDER.",
        "Rules:",
        "- Use short headings and provide all relevant data.",
        "- Keep numbers accurate; do not invent fields.",
        "- If a section is missing, write “No data this week.”",
        "- Keep it readable for non-technical staff.",
        "- Provide a short summary 1-2 sentence summary at the end of each section based on the data you see."
        "",
    ]
    for title, blob in sections:
        j = json.dumps(blob, ensure_ascii=False, indent=2, default=_json_default) if blob is not None else "null"
        parts.append(f"### {title}\n```json\n{j}\n```")
    return "\n".join(parts)

# ── DM payloads (chunk long people lists) ─────────────────────────────────────
def _codeblock(obj) -> str:
    return "```json\n" + json.dumps(obj, ensure_ascii=False, indent=2, default=_json_default) + "\n```"

def build_dm_messages(outputs: dict, cadence: dict) -> list[str]:
    msgs = []

    # Skipped Check-ins summary
    chk = outputs.get("Planning Center check-ins") or {}
    skipped = chk.get("skipped") or (chk.get("debug") or {}).get("skipped")
    if skipped:
        msgs.append("**Skipped Check-ins (raw)**\n" + _codeblock(skipped))

    # Cadence buckets
    buckets = cadence.get("cadence_buckets")
    if buckets:
        msgs.append("**Cadence Buckets**\n" + _codeblock(buckets))

    # Lapses summary
    lapses = cadence.get("lapses")
    if lapses:
        msgs.append("**Lapses Summary**\n" + _codeblock(lapses))

    # People printouts (chunked for readability)
    newly = lapses.get("items_attend", []) + lapses.get("items_give", []) if lapses else []
    all_lapsed = (lapses or {}).get("all_lapsed_people") or []
    nlas = (cadence.get("no_longer_attends") or {}).get("items") or []

    def chunk(label: str, people: list, size: int = 100):
        if not people:
            return
        for i in range(0, len(people), size):
            part = people[i:i+size]
            msgs.append(f"**{label} (rows {i+1}–{i+len(part)})**\n" + _codeblock(part))

    chunk("People: NEW lapses this week", newly)
    chunk("People: ALL lapsed", all_lapsed)
    chunk("People: NEW No Longer Attends", nlas)

    return msgs

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Run weekly NP Analytics jobs")
    parser.add_argument("--week-end", dest="week_end", help="Sunday YYYY-MM-DD to run FOR (Mon–Sun window)")
    args = parser.parse_args()

    selected_week_end_date: date | None = None
    if args.week_end:
        try:
            selected_week_end_date = date.fromisoformat(args.week_end)
            if selected_week_end_date.weekday() != 6:  # Monday=0..Sunday=6
                log.warning("⚠️ --week-end %s is not a Sunday; proceeding anyway.", selected_week_end_date)
        except ValueError:
            log.error("❌ --week-end must be YYYY-MM-DD (e.g., 2025-09-21)")
            return

    # Warm the app
    ok = warmup_base_url()
    if not ok:
        log.warning("Proceeding despite warm-up failure…")

    # DB ping
    try:
        with SessionLocal() as db:
            db.execute(text("SELECT 1"))
        log.info("✅ DB connectivity OK")
    except Exception as e:
        log.error("❌ DB test query failed: %s", e)

    # Resolve the target window once and reuse
    last_mon, last_sun = last_monday_and_sunday_cst(selected_week_end_date)

    # 1) Run the pipeline for that week
    cadence = run_weekly_pipeline(selected_week_end_date)

    # 2) Collect JSON for assistant + DM (read-only where possible)
    collection_jobs = [
        ("/attendance/process-sheet", "Adult attendance processing"),
        (f"/planning-center/checkins-location/day/{last_sun}", "Planning Center check-ins"),
        (f"/planning-center/giving/weekly-summary?start={last_mon}&end={last_sun}", "Planning Center Giving Summary"),
        ("/planning-center/groups", "Planning Center Groups"),
        ("/planning-center/serving/summary", "Planning Center Volunteer Summary"),
        ("/youtube/livestreams", "YouTube livestream tracking"),
        ("/youtube/weekly-summary", "YouTube weekly summary"),
        ("/mailchimp/weekly-summary", "Mailchimp weekly summary"),
    ]

    outputs: dict[str, object] = {}
    for endpoint, label in collection_jobs:
        # Prefer GET for reads. If your rollup route is POST-only, flip this back.
        raw = call_job(endpoint, label, 600)
        outputs[label] = _json_or_empty(raw)
        time.sleep(0.5)

    # Enrich with unplaced/skipped for DM
    try:
        skipped = fetch_unplaced_for_date(str(last_sun))
        if skipped:
            chk = outputs.get("Planning Center check-ins") or {}
            dbg = chk.get("debug") or {}
            dbg["skipped"] = skipped
            chk["debug"] = dbg
            outputs["Planning Center check-ins"] = chk
            log.info("🧾 Attached %d unplaced check-ins for DM", len(skipped))
    except Exception:
        log.exception("Could not attach unplaced check-ins")

    # 3) TEAM summary via Assistant
    team_prompt = build_team_prompt(outputs, cadence)
    log.info("🧠 Assistant input:\n%s", team_prompt[:1000] + ("…" if len(team_prompt) > 1000 else ""))
    summary = run_assistant_with_tools(team_prompt)
    log.info("🧠 Assistant finished")

    # 4) Post to ClickUp (team + DMs)
    with SessionLocal() as db:
        if CLICKUP_WORKSPACE_ID and CLICKUP_TEAM_CHANNEL_ID:
            post_message(db, CLICKUP_WORKSPACE_ID, CLICKUP_TEAM_CHANNEL_ID, summary)
            log.info("📤 Posted team summary to channel %s", CLICKUP_TEAM_CHANNEL_ID)
        else:
            log.warning("⚠️ Missing CLICKUP_WORKSPACE_ID or CLICKUP_TEAM_CHANNEL_ID; skipping team post.")

        try:
            if CLICKUP_WORKSPACE_ID and CLICKUP_DM_USER_IDS:
                dm_messages = build_dm_messages(outputs, cadence)
                for msg in dm_messages:
                    send_dm(db, CLICKUP_WORKSPACE_ID, CLICKUP_DM_USER_IDS, msg)
                    time.sleep(0.5)
                log.info("📤 Sent %d DM message(s)", len(dm_messages))
        except Exception as e:
            log.error("Skipping DMs due to DB/token error: %s", e)


if __name__ == "__main__":
    main()

