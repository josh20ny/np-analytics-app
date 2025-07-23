# weekly_summary/main.py

from datetime import date
from sqlalchemy.orm import Session
from app.db import get_db
from weekly_summary.data_access import fetch_all_with_yoy
from weekly_summary.formatter import format_summary
from weekly_summary.report_builder import build_full_report
from clickup_app.clickup_client import post_message
from clickup_app.crud import get_token
from weekly_summary.data_access import fetch_all_with_yoy, fetch_all_mailchimp_rows_for_latest_week
from app.config import settings
import requests
import os


def run(log_output=None, debug_text=""):
    # 1) Open DB session
    db: Session
    for db in get_db():
        break

    # 2) ClickUp workspace setup
    workspace_id = os.getenv("CLICKUP_WORKSPACE_ID")
    channel_id = os.getenv("CLICKUP_CHANNEL_ID")

    if not workspace_id or not channel_id:
        raise RuntimeError("CLICKUP_WORKSPACE_ID and CLICKUP_CHANNEL_ID must be set in the environment.")

    token_row = get_token(db, workspace_id)
    if not token_row:
        raise RuntimeError(f"No ClickUp OAuth token found for workspace {workspace_id}.")

    # 3) Fetch YoY summary data
    latest_data = fetch_all_with_yoy()
    mailchimp_rows = fetch_all_mailchimp_rows_for_latest_week()
    summary = format_summary(latest_data, mailchimp_rows)

    # 4) Fetch debug summary from checkins route
    debug_text = ""
    try:
        resp = requests.get(settings.API_BASE_URL.rstrip("/") + "/planning-center/checkins", timeout=30)
        if resp.ok:
            data = resp.json()
            debug_text = data.get("debug_text", "")
        else:
            print(f"⚠️ Failed to fetch checkins debug summary: {resp.status_code}")
    except Exception as e:
        print(f"❌ Error fetching checkins debug summary: {e}")

    # 5) Fetch debug summary from livestreams route
    livestreams_data = ""
    try:
        resp = requests.get(settings.API_BASE_URL.rstrip("/") + "/youtube/livestreams", timeout=30)
        if resp.ok:
            data = resp.json()
            tracked = data.get("livestreams_tracked", [])
            if tracked:
                lines = ["📺 Livestreams tracked:"]
                for item in tracked:
                    # only include non-null view counts
                    parts = []
                    for key in ("initial_views", "views_1w", "views_4w"):
                        v = item.get(key)
                        if v is not None:
                            parts.append(f"{key}={v}")
                    views_str = ", ".join(parts)
                    lines.append(
                        f"- {item['action']} – {item['video_id']} – "
                        f"{item['title']} ({item['pub_date']}): {views_str}"
                    )
                livestreams_debug = "\n".join(lines)
        else:
            print(f"⚠️ Failed to fetch livestreams debug summary: {resp.status_code}")
    except Exception as e:
        print(f"❌ Error fetching livestreams debug summary: {e}")

    # 6) Merge the two debug blocks
    combined_debug = "\n\n".join(b for b in (debug_text, livestreams_data) if b)

    # 5) Build full report and post to ClickUp
    full_report = build_full_report(summary, log_output or [], checkins_debug=combined_debug)

    try:
        post_message(db, workspace_id, channel_id, full_report)
        print(f"✅ Weekly summary posted to ClickUp workspace {workspace_id} channel {channel_id}.")
    except Exception as e:
        print(f"❌ Failed to post message to ClickUp: {e}")


if __name__ == "__main__":
    run()

