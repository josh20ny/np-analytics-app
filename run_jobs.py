# run_jobs.py
import os, time, json, logging, requests
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from sqlalchemy import text

from dotenv import load_dotenv
from app.db import SessionLocal
from clickup_app.assistant_client import run_assistant_with_tools
from clickup_app.clickup_client import post_message, send_dm

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL                 = os.getenv("API_BASE_URL")
WAKEUP_DELAY             = int(os.getenv("WAKEUP_DELAY", "10"))
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
}

# ── helpers ───────────────────────────────────────────────────────────────────
def last_monday_and_sunday_cst() -> tuple[datetime.date, datetime.date]:
    """Return the last full Mon–Sun window in America/Chicago."""
    now_cst = datetime.now(CENTRAL_TZ).date()
    # Monday=0..Sunday=6; last Sunday is yesterday if today is Monday
    last_sun = now_cst - timedelta(days=(now_cst.weekday() + 1))
    last_mon = last_sun - timedelta(days=6)
    return last_mon, last_sun

# ── Logging ───────────────────────────────────────────────────────────────────
log = logging.getLogger("run_jobs")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s run_jobs: %(message)s"
)

# ── HTTP helpers ──────────────────────────────────────────────────────────────
def call_job(endpoint: str, label: str, timeout_s: int | None = None) -> str:
    """Call an API route and return its raw text response, with better timeouts and logs."""
    url = f"{BASE_URL.rstrip('/')}{endpoint}"
    t0 = time.perf_counter()
    to = timeout_s or TIMEOUTS.get(label, 600)
    # requests allows (connect, read) tuple; keep connect short, read long
    timeout = (10, to)
    log.info("📡 Calling: %s – %s (timeout=%ss)", endpoint, label, to)
    try:
        r = requests.get(url, timeout=timeout)
        elapsed = time.perf_counter() - t0
        if r.status_code == 200:
            log.info("✅ %s finished (%s) in %.1fs", label, r.status_code, elapsed)
            return r.text or ""
        log.error("❌ %s failed (%s) in %.1fs: %s", label, r.status_code, elapsed, (r.text or "")[:300])
        return ""
    except requests.ReadTimeout:
        elapsed = time.perf_counter() - t0
        log.error("⏱️ %s timed out after %.1fs (server may still be working)", label, elapsed)
        return ""
    except Exception as e:
        elapsed = time.perf_counter() - t0
        log.exception("💥 %s error after %.1fs", label, elapsed)
        return ""

def _json_or_empty(raw: str) -> dict:
    try:
        return json.loads(raw or "{}")
    except json.JSONDecodeError:
        return {"error": "invalid JSON", "raw": (raw or "")[:3000]}

# ── Cadence readiness polling (ensures DM has people lists) ───────────────────
def fetch_cadence_report(last_sun: datetime.date, tries: int = 8, wait_s: int = 10) -> dict:
    """Poll weekly-report until engaged/front_door/lapses appear (bounded)."""
    qs = f"?week_end={last_sun.isoformat()}&ensure_snapshot=true&persist_front_door=true"
    endpoint = f"/analytics/cadence/weekly-report{qs}"
    raw = ""
    for attempt in range(1, tries + 1):
        raw = call_job(endpoint, "Cadence weekly report", TIMEOUTS.get("Cadence weekly report"))
        data = _json_or_empty(raw)
        if isinstance(data, dict) and data.get("engaged") and data.get("front_door") and data.get("lapses"):
            log.info("✅ Cadence report ready on attempt %d", attempt)
            return data
        log.info("⏳ Cadence report not ready (attempt %d/%d) – waiting %ss", attempt, tries, wait_s)
        time.sleep(wait_s)
    log.warning("⚠️ Cadence report missing sections after %d attempts; proceeding with best effort.", tries)
    return _json_or_empty(raw)

# ── Pipeline: facts first → cadence last (blocking) ───────────────────────────
def run_weekly_pipeline() -> dict:
    """Sequential weekly pipeline with strict 1-week window (Mon–Sun, CST)."""
    last_mon, last_sun = last_monday_and_sunday_cst()
    log.info("🗓️ Weekly window: %s → %s (CST)", last_mon, last_sun)

    calls = [
        (f"/planning-center/people/sync?since={last_mon}", "People sync"),
        (f"/planning-center/groups/sync?since={last_mon}", "Groups/memberships sync"),
        (f"/planning-center/serving/sync?since={last_mon}", "Serving teams/memberships sync"),
        (f"/planning-center/checkins?date={last_sun}", "Check-ins ingest (last Sunday)"),
        # giving endpoint already computes last full week; we pin week_end to be explicit
        (f"/planning-center/giving/weekly-summary?week_end={last_sun}", "Giving summary (last full week)"),
        (f"/analytics/cadence/rebuild?signals=attend,give,group,serve&since={last_mon}&rolling_days=180&week_end={last_sun}",
         "Cadence rebuild"),
        (f"/analytics/cadence/snap-week?week_end={last_sun}", "Cadence snapshot"),
    ]

    for endpoint, label in calls:
        call_job(endpoint, label, TIMEOUTS.get(label))
        # brief pacing to keep server comfy
        time.sleep(1.5)

    # Ensure we have a fully-populated weekly-report JSON before summaries/DMs
    cadence = fetch_cadence_report(last_sun)
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
        ("YouTube Livestreams Summary",                        outputs.get("YouTube weekly summary")),
    ]
    parts = [
        "You are NP Analytics’ reporting assistant.",
        "Compose a clear, concise weekly update for the team. Use the sections below in THIS EXACT ORDER.",
        "Rules:",
        "- Use short headings and 2–4 bullet points per section.",
        "- Keep numbers accurate; do not invent fields.",
        "- If a section is missing, write “No data this week.”",
        "- Keep it readable for non-technical staff.",
        "",
    ]
    for title, blob in sections:
        j = json.dumps(blob, ensure_ascii=False, indent=2) if blob is not None else "null"
        parts.append(f"### {title}\n```json\n{j}\n```")
    return "\n".join(parts)

# ── DM payloads (chunk long people lists) ─────────────────────────────────────
def _codeblock(obj) -> str:
    return "```json\n" + json.dumps(obj, ensure_ascii=False, indent=2) + "\n```"

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
    # Warm the app
    try:
        ping = requests.get(f"{BASE_URL.rstrip('/')}/docs", timeout=(5, 10))
        log.info("🌐 Warm-up ping %s", ping.status_code)
    except Exception:
        log.info("⏱️ Sleeping %ss while app spins up…", WAKEUP_DELAY)
        time.sleep(WAKEUP_DELAY)

    # 1) Run the pipeline (sequential, strict week window) → cadence dict
    cadence = run_weekly_pipeline()

    # 2) Collect JSON for assistant + DM (now that upstream is done)
    last_mon, last_sun = last_monday_and_sunday_cst()
    collection_jobs = [
        ("/attendance/process-sheet", "Adult attendance processing"),
        (f"/planning-center/checkins?date={last_sun}", "Planning Center check-ins"),
        (f"/planning-center/giving/weekly-summary?week_end={last_sun}", "Planning Center Giving Summary"),
        ("/planning-center/groups", "Planning Center Groups"),
        ("/planning-center/serving/summary", "Planning Center Volunteer Summary"),
        ("/youtube/weekly-summary", "YouTube weekly summary"),
        ("/youtube/livestreams", "YouTube livestream tracking"),
        (f"/analytics/cadence/weekly-report?week_end={last_sun}&ensure_snapshot=true", "Cadence weekly report"),
        # You can add Mailchimp here too if you want it captured:
        # ("/mailchimp/weekly-summary", "Mailchimp weekly summary"),
    ]

    outputs: dict[str, object] = {}
    for endpoint, label in collection_jobs:
        raw = call_job(endpoint, label, 600)
        outputs[label] = _json_or_empty(raw)
        time.sleep(0.5)

    # 3) TEAM summary via Assistant (strict order)
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

