# ─────────────────────────────────────────────────────────────────────────────
# run_jobs.py (PATCH)
# ─────────────────────────────────────────────────────────────────────────────
import os, json, time, requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from app.db import SessionLocal  # ADD: use real session, not Depends
from clickup_app.assistant_client import run_assistant_with_tools
from clickup_app.clickup_client import post_message
from clickup_app.clickup_client import send_dm  # ADD

load_dotenv()

API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000")
CENTRAL_TZ = ZoneInfo("America/Chicago")

CLICKUP_WORKSPACE_ID    = os.getenv("CLICKUP_WORKSPACE_ID", "")
CLICKUP_TEAM_CHANNEL_ID = os.getenv("CLICKUP_TEAM_CHANNEL_ID", "")
# Comma-separated list → list[str]
CLICKUP_DM_USER_IDS     = [s.strip() for s in os.getenv("CLICKUP_DM_USER_IDS", "").split(",") if s.strip()]

def _get(url: str, label: str):
    full = f"{API_BASE}{url}" if not url.startswith("http") else url
    r = requests.get(full, timeout=60)
    try:
        r.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"{label} failed: {e}\n↳ {r.text[:500]}")
    try:
        return r.json()
    except Exception:
        return {"status": "ok", "raw_text": r.text}

def _last_sunday_cst(now: datetime | None = None) -> str:
    if not now:
        now = datetime.now(CENTRAL_TZ)
    d = now.date()
    last_sun = d - timedelta(days=((d.weekday() + 1) % 7))
    return last_sun.isoformat()

# ── Weekly data collection (keep your current job calls; we also capture outputs) ──
def collect_weekly_outputs() -> dict:
    outputs = {}

    # 1) Adult Attendance (sheet → DB). If you have a read endpoint, use that instead.
    try:
        outputs["adult_attendance"] = _get("/attendance/process-sheet", "Adult Attendance")
    except Exception as e:
        outputs["adult_attendance_error"] = str(e)

    # 2) Check-ins (Kids/Students) – use last Sunday explicitly
    try:
        last_sun = _last_sunday_cst()
        outputs["checkins"] = _get(f"/planning-center/checkins?date={last_sun}", "Check-ins")
    except Exception as e:
        outputs["checkins_error"] = str(e)

    # 3) Giving weekly summary (last full Mon..Sun)
    try:
        outputs["giving"] = _get("/planning-center/giving/weekly-summary", "Giving summary")
    except Exception as e:
        outputs["giving_error"] = str(e)

    # 4) Volunteering / Serving
    # Prefer weekly-summary if present, else sync response
    try:
        try:
            outputs["serving"] = _get("/planning-center/serving/weekly-summary", "Serving weekly summary")
        except Exception:
            outputs["serving"] = _get("/planning-center/serving/sync", "Serving sync")
    except Exception as e:
        outputs["serving_error"] = str(e)

    # 5) Groups (same pattern as serving)
    try:
        try:
            outputs["groups"] = _get("/planning-center/groups/weekly-summary", "Groups weekly summary")
        except Exception:
            outputs["groups"] = _get("/planning-center/groups/sync", "Groups sync")
    except Exception as e:
        outputs["groups_error"] = str(e)

    # 6) Cadence weekly report (engaged, front_door, buckets, lapses…)
    try:
        outputs["cadence"] = _get("/analytics/cadence/weekly-report?ensure_snapshot=true&persist_front_door=true", "Cadence weekly-report")
    except Exception as e:
        outputs["cadence_error"] = str(e)

    # 7) YouTube weekly summary
    try:
        outputs["youtube"] = _get("/youtube/weekly-summary", "YouTube weekly summary")
    except Exception as e:
        outputs["youtube_error"] = str(e)

    return outputs

# ── TEAM: Build a strict-order Assistant prompt ───────────────────────────────
def build_team_prompt(outputs: dict) -> str:
    """
    Order required by the user:
      1) Adult Attendance Summary
      2) Checkins (Kids & Students)
      3) Giving Summary
      4) Volunteering Summary
      5) Groups Summary
      6) Engaged Summary (cadence.engaged)
      7) Front Door Summary (cadence.front_door)
      8) YouTube Livestreams Summary
    """
    # Carefully extract the two cadence subsections if present
    cadence = outputs.get("cadence") or {}
    engaged = cadence.get("engaged") or {}
    front_door = cadence.get("front_door") or {}

    # We pass JSON blobs as sections to keep the Assistant grounded
    sections = [
        ("Adult Attendance Summary", outputs.get("adult_attendance")),
        ("Check-ins (Kids & Students Attendance Summaries)", outputs.get("checkins")),
        ("Giving Summary", outputs.get("giving")),
        ("Volunteering Summary", outputs.get("serving")),
        ("Groups Summary", outputs.get("groups")),
        ("Engaged Summary", engaged or {"note": "no engaged data"}),
        ("Front Door Summary", front_door or {"note": "no front door data"}),
        ("YouTube Livestreams Summary", outputs.get("youtube")),
    ]

    # Assistant instruction keeps tone concise and sectioned
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

# ── DM: Build operational payloads (chunkable) ────────────────────────────────
def _codeblock(obj) -> str:
    return "```json\n" + json.dumps(obj, ensure_ascii=False, indent=2) + "\n```"

def build_dm_messages(outputs: dict) -> list[str]:
    msgs = []
    # 1) Skipped Check-ins (from checkins-route)
    chk = outputs.get("checkins") or {}
    skipped = chk.get("skipped") or (chk.get("debug") or {}).get("skipped")
    if skipped:
        msgs.append("**Skipped Check-ins (raw)**\n" + _codeblock(skipped))

    # 2) Cadence Buckets
    cad = outputs.get("cadence") or {}
    buckets = cad.get("cadence_buckets")
    if buckets:
        msgs.append("**Cadence Buckets**\n" + _codeblock(buckets))

    # 3) Lapses summary + people lists
    lapses = cad.get("lapses")
    if lapses:
        msgs.append("**Lapses Summary**\n" + _codeblock(lapses))

    # 4) People printouts
    # Try common keys that your cadence report tends to expose
    newly_people = cad.get("newly") or cad.get("new_lapses") or cad.get("new_lapses_people")
    all_lapsed = cad.get("all_lapsed_people") or cad.get("all_lapsed")
    no_longer_attends = cad.get("no_longer_attends") or cad.get("no_longer_attends_people")

    def chunk_people(label, data):
        if not data:
            return
        # break into 100/person chunks so the message stays readable
        batch = []
        for i, person in enumerate(data, 1):
            batch.append(person)
            if i % 100 == 0:
                msgs.append(f"**{label} (next 100)**\n" + _codeblock(batch))
                batch = []
        if batch:
            msgs.append(f"**{label}**\n" + _codeblock(batch))

    chunk_people("People: NEW lapses this week", newly_people)
    chunk_people("People: ALL lapsed", all_lapsed)
    chunk_people("People: NEW No Longer Attends", no_longer_attends)

    return msgs

# ── Main “weekly run” wrapper (minimal impact to your flow) ───────────────────
def run_weekly():
    outputs = collect_weekly_outputs()

    # TEAM summary via Assistant
    team_prompt = build_team_prompt(outputs)
    summary_text = run_assistant_with_tools(team_prompt)

    with SessionLocal() as db:
        if CLICKUP_WORKSPACE_ID and CLICKUP_TEAM_CHANNEL_ID:
            post_message(db, CLICKUP_WORKSPACE_ID, CLICKUP_TEAM_CHANNEL_ID, summary_text)

        # DM(s) – raw/operational details
        if CLICKUP_WORKSPACE_ID and CLICKUP_DM_USER_IDS:
            dm_messages = build_dm_messages(outputs)
            for msg in dm_messages:
                send_dm(db, CLICKUP_WORKSPACE_ID, CLICKUP_DM_USER_IDS, msg)

    return outputs  # keep the dict if you want to write it to disk/logs

if __name__ == "__main__":
    run_weekly()
