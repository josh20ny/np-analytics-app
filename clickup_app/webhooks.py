# clickup_app/webhooks.py

from fastapi import APIRouter, Request, BackgroundTasks, Depends
from sqlalchemy.orm import Session
from app.db import get_db

from clickup_app.clickup_client import post_message
from clickup_app.assistant_client import run_assistant_with_tools

import os

router = APIRouter()

@router.post("/webhooks/clickup/chat")
async def receive_clickup_automation(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    body = await request.json()

    payload = body.get("payload", {}) or {}
    data = payload.get("data", {}) or {}

    # Message content + routing info
    content    = (data.get("text_content") or data.get("content") or "").strip()
    channel_id = data.get("parent") or data.get("channel_id")
    user_id    = data.get("userid") or (data.get("user") or {}).get("id")
    workspace_id = body.get("team_id") or os.getenv("CLICKUP_WORKSPACE_ID")

    # Only respond if bot is mentioned
    if not content or not channel_id or "@NP Analytics Bot" not in content:
        return {"status": "ignored"}

    prompt = content.replace("@NP Analytics Bot", "").strip()

    def handle():
        # Run your assistant, then @-mention the author and post back in same channel
        reply   = run_assistant_with_tools(prompt)
        mention = f"<@{user_id}>" if user_id else ""
        message = f"{mention} {reply}".strip()
        post_message(db, workspace_id, channel_id, message)

    background_tasks.add_task(handle)
    return {"status": "accepted"}

