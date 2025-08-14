# clickup_app/webhooks.py

from fastapi import APIRouter, Request, BackgroundTasks, Depends
from sqlalchemy.orm import Session
from app.db import get_db
from datetime import datetime
from pytz import timezone
import re

from clickup_app.assistant_client import run_assistant_with_tools
from clickup_app.clickup_client import post_message, get_channel_members_map, format_user_mention

import os

router = APIRouter()

@router.post("/webhooks/clickup/chat")
async def receive_clickup_automation(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    # 1) Parse payload safely
    body = await request.json()
    payload = body.get("payload", {}) or {}
    data = payload.get("data", {}) or {}

    content = (data.get("text_content") or data.get("content") or "").strip()
    channel_id = data.get("parent") or data.get("channel_id")
    user_id = str(data.get("userid") or (data.get("user") or {}).get("id") or "")
    workspace_id = body.get("team_id") or os.getenv("CLICKUP_WORKSPACE_ID")

    # 2) Only respond if mentioned
    if "@NP Analytics Bot" in content: 
        prompt = content.replace("@NP Analytics Bot", "").strip()

        # 3) Do the heavy lifting in the background
        def handle():
            try:
                reply = run_assistant_with_tools(prompt)

                # Try to resolve a nicer display name; fall back to id
                display_name = None
                try:
                    members_map = get_channel_members_map(db, workspace_id, channel_id)
                    display_name = members_map.get(user_id)
                except Exception as e:
                    print(f"[clickup] members lookup failed: {e}")

                mention = format_user_mention(user_id, display_name)
                message = f"{mention} {reply}".strip()

                post_message(db, workspace_id, channel_id, message)
                print("✅ Posted reply to ClickUp (OAuth)")
            except Exception as e:
                print(f"❌ Error handling webhook: {e}")

        background_tasks.add_task(handle)
        return {"status": "accepted"}
    elif re.search(r"\bOU\b", content, flags=re.IGNORECASE):
        now = datetime.now(timezone('America/Chicago'))
        reply = (
            "I have detected OU in your message.\n"
            f"The time is {now.strftime('%I:%M %p')} and OU still sucks! 🤘🐂"
        )

        display_name = None  # ✅ default before try
        try:
            members_map = get_channel_members_map(db, workspace_id, channel_id)
            display_name = members_map.get(user_id)
        except Exception as e:
            print(f"[clickup] members lookup failed: {e}")

        mention = format_user_mention(user_id, display_name)  # safe even if None
        message = f"{mention} {reply}".strip()
        post_message(db, workspace_id, channel_id, message)
        return {"status": "ok"}

    else:
        return {"status": "ignored"}
