# weekly_summary/main.py

from datetime import date
from sqlalchemy.orm import Session
from app.db import get_db
from weekly_summary.data_access import fetch_all_with_yoy
from weekly_summary.formatter import format_summary
from weekly_summary.report_builder import build_full_report
from clickup_app.clickup_client import post_message
from clickup_app.crud import get_token
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
    summary = format_summary(latest_data)

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

    # 5) Build full report and post to ClickUp
    full_report = build_full_report(summary, log_output or [], checkins_debug=debug_text)

    try:
        post_message(db, workspace_id, channel_id, full_report)
        print(f"✅ Weekly summary posted to ClickUp workspace {workspace_id} channel {channel_id}.")
    except Exception as e:
        print(f"❌ Failed to post message to ClickUp: {e}")


if __name__ == "__main__":
    run()

