# weekly_summary/main.py

from datetime import date
from sqlalchemy.orm import Session
from app.db import get_db       # the SQLAlchemy session dependency
from weekly_summary.data_access import fetch_all_with_yoy  # or fetch_all_latest
from weekly_summary.formatter import format_summary
from clickup_app.clickup_client import post_message
from clickup_app.crud import get_token
import os

import os

def run():
    # 1) Open a DB session
    db: Session
    for db in get_db():  # get_db is a generator
        break

    # 2) Determine your workspace and channel
    workspace_id = os.getenv("CLICKUP_WORKSPACE_ID")
    if not workspace_id:
        raise RuntimeError("CLICKUP_WORKSPACE_ID must be set in your environment")

    token_row = get_token(db, workspace_id)
    if not token_row:
        raise RuntimeError(f"No ClickUp OAuth token found for workspace {workspace_id}.")


    workspace_id = token_row.workspace_id
    channel_id   = os.getenv("CLICKUP_CHANNEL_ID")

    # 3) Build the summary text
    latest_data = fetch_all_with_yoy()   # includes YoY; or fetch_all_latest()
    message     = format_summary(latest_data)

    # 4) Post via OAuth client
    post_message(db, workspace_id, channel_id, message)

    print(f"✅ Weekly summary posted to ClickUp workspace {workspace_id} channel {channel_id}.")

if __name__ == "__main__":
    run()

