# clickup_app/webhooks.py

from fastapi import APIRouter, Request, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from app.db import get_db
from clickup_app.assistant_client import run_assistant_with_tools
from clickup_app.clickup_client import post_message, get_token_by_workspace
import os

router = APIRouter()

@router.post("/webhooks/clickup/chat")
async def receive_clickup_automation(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    payload = await request.json()
    data = payload.get("payload", {}).get("data", {})
    content = data.get("text_content", "")
    channel_id = data.get("parent")
    user_id = data.get("userid")
    workspace_id = os.getenv("CLICKUP_TEAM_ID")

    if not content or not channel_id or "@NP Analytics Bot" not in content:
        return {"status": "ignored"}

    prompt = content.replace("@NP Analytics Bot", "").strip()

    def respond():
        try:
            reply = run_assistant_with_tools(prompt)
            mention = f"<@{user_id}>"
            message = f"{mention} {reply}"
            post_message(db, workspace_id, channel_id, message)
        except Exception as e:
            print(f"❌ Assistant error: {e}")

    background_tasks.add_task(respond)
    return {"status": "accepted"}
