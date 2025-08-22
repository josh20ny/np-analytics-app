# run_jobs.py
import os, time, json, logging, requests
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

from dotenv import load_dotenv
from app.db import SessionLocal
from app.utils.common import get_last_sunday_cst
from clickup_app.assistant_client import run_assistant_with_tools
from clickup_app.clickup_client import post_message, send_dm

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL                 = os.getenv("BASE_URL", "http://127.0.0.1:8000")
WAKEUP_DELAY             = int(os.getenv("WAKEUP_DELAY", "10"))
CLICKUP_WORKSPACE_ID     = os.getenv("CLICKUP_WORKSPACE_ID", "")
CLICKUP_TEAM_CHANNEL_ID  = os.getenv("CLICKUP_TEAM_CHANNEL_ID", "")

# Support either a single user var or comma list (back-compat with your envs)
CLICKUP_JOSH_USER_ID     = os.getenv("CLICKUP_JOSH_USER_ID") or os.getenv("CLICKUP_JOSH_CHANNEL_ID", "")
CLICKUP_DM_USER_IDS      = [s.strip() for s in os.getenv("CLICKUP_DM_USER_IDS", "").split(",") if s.strip()]
if CLICKUP_JOSH_USER_ID and CLICKUP_JOSH_USER_ID not in CLICKUP_DM_USER_IDS:
    CLICKUP_DM_USER_IDS.append(CLICKUP_JOSH_USER_ID)

CENTRAL_TZ = ZoneInfo("America/Chicago")

# Keep your original working routes
JOBS = [
    ("/attendance/process-sheet",          "Adult attendance processing"),
    ("/planning-center/checkins",          "Planning Center check-ins"),
    ("/planning-center/giving/weekly-summary", "Planning Center Giving Summary"),
    ("/planning-center/groups",            "Planning Center Groups"),
    ("/planning-center/serving/summary",   "Planning Center Volunteer Summary"),
    ("/youtube/weekly-summary",            "YouTube weekly summary"),
    ("/youtube/livestreams",               "YouTube livestream tracking"),
    ("/mailchimp/weekly-summary",          "Mailchimp weekly summary"),
]

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("run_jobs")

# ── HTTP helpers ──────────────────────────────────────────────────────────────
def call_job(endpoint: str, label: str, *, timeout: int = 300) -> str:
    url = f"{BASE_URL.rstrip('/')}{endpoint}"
    log.info("📡 Calling: %s – %s", url, label)
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200:
            log.info("✅ Finished: %s (200)", label)
            return r.text or ""
        log.error("❌ Failed: %s (%s) – %s", label, r.status_code, (r.text or "")[:300])
        return ""
    except Exception as e:
        log.exception("❌ Error calling %s: %s", label, e)
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
    for attempt in range(1, tries + 1):
        raw = call_job(endpoint, "Cadence weekly-report")
        data = _json_or_empty(raw)
        if isinstance(data, dict) and data.get("engaged") and data.get("front_door") and data.get("lapses"):
            log.info("✅ Cadence report ready on attempt %d", attempt)
            return data
        log.info("⏳ Cadence report not ready (attempt %d/%d) – waiting %ss", attempt, tries, wait_s)
        time.sleep(wait_s)
    log.warning("⚠️ Cadence report missing sections after %d attempts; proceeding with best effort.", tries)
    return _json_or_empty(raw)

# ── Pipeline: facts first → cadence last (blocking) ───────────────────────────
def run_weekly_pipeline():
    last_sun = get_last_sunday_cst()
    log.info("🗓️ Last Sunday (CST): %s", last_sun)

    calls = [
        ("/planning-center/people/sync",                 "People sync"),
        ("/planning-center/groups/sync",                 "Groups/memberships sync"),
        ("/planning-center/serving/sync",                "Serving teams/memberships sync"),
        (f"/planning-center/checkins?date={last_sun}",   "Check-ins ingest (last Sunday)"),
        ("/planning-center/giving/weekly-summary",       "Giving summary (last full week)"),
        # Rebuild cadence for current window and snap this week
        (f"/analytics/cadence/rebuild?signals=attend,give,group,serve&since={last_sun - timedelta(days=8)}&rolling_days=180&week_end={last_sun}",
            "Cadence rebuild"),
        (f"/analytics/cadence/snap-week?week_end={last_sun}", "Cadence snapshot"),
    ]

    for endpoint, label in calls:
        _ = call_job(endpoint, label)
        # small pacing to keep server comfy
        time.sleep(1.5)

    # Make sure we have a fully-populated weekly-report JSON before summaries/DMs
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

    # People printouts
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
    # Warm-up ping
    try:
        ping = requests.get(f"{BASE_URL.rstrip('/')}/docs", timeout=10)
        log.info("🌐 Warm-up ping returned %s", ping.status_code)
    except Exception:
        log.info("⏱️ Waiting %ss for app to spin up…", WAKEUP_DELAY)
        time.sleep(WAKEUP_DELAY)

    # 1) Pipeline (facts → cadence), returns fully-populated cadence weekly-report
    cadence = run_weekly_pipeline()

    # 2) Execute each job and collect raw JSON for the team & DM
    outputs: dict[str, dict] = {}
    for endpoint, label in JOBS:
        raw = call_job(endpoint, label)
        outputs[label] = _json_or_empty(raw)
        time.sleep(1.0)

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

        if CLICKUP_WORKSPACE_ID and CLICKUP_DM_USER_IDS:
            dm_messages = build_dm_messages(outputs, cadence)
            # Send as a small thread: first message creates the DM channel, rest follow
            thread_started = False
            for msg in dm_messages:
                send_dm(db, CLICKUP_WORKSPACE_ID, CLICKUP_DM_USER_IDS, msg)
                if not thread_started:
                    thread_started = True
                time.sleep(0.5)
            log.info("📤 Sent %d DM message(s) to %s", len(dm_messages), CLICKUP_DM_USER_IDS)
        else:
            log.info("ℹ️ No DM recipients configured; skipping DM.")

if __name__ == "__main__":
    main()
