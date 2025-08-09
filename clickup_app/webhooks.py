# clickup_app/webhooks.py
from fastapi import APIRouter, Request, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from app.db import get_db
from clickup_app.clickup_client import ClickUpService
from clickup_app.assistant_client import run_assistant_with_tools

import os

router = APIRouter()
service = ClickUpService()

@router.post("/webhooks/clickup/chat")
async def receive_clickup_automation(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    payload = await request.json()
    data = payload.get("payload", {}).get("data", {})
    content = data.get("text_content", "")
    channel_id = data.get("parent") or data.get("channel_id")
    user_id = data.get("userid")
    ws_id = payload.get("team_id") or os.getenv("CLICKUP_WORKSPACE_ID")

    if not content or not channel_id or "@NP Analytics Bot" not in content:
        return {"status": "ignored"}

    prompt = content.replace("@NP Analytics Bot", "").strip()

    def handle_response():
        try:
            reply = run_assistant_with_tools(prompt)
            mention = f"<@{user_id}>" if user_id else ""
            full = f"{mention} {reply}".strip()
            service.send_message(full, channel_id)
            print("✅ Message posted to ClickUp")
        except Exception as e:
            print(f"❌ Error posting to ClickUp: {e}")

    background_tasks.add_task(handle_response)
    return {"status": "accepted"}
