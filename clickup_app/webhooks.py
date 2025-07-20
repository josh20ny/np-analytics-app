# clickup_app/webhooks.py

from fastapi import APIRouter, Request, Depends
from sqlalchemy.orm import Session
from app.db import get_db
from clickup_app.assistant_client import run_assistant_with_tools
from clickup_app.clickup_client import post_message, get_token_by_workspace
import requests
import os

router = APIRouter()

@router.post("/webhooks/clickup/chat")
async def receive_clickup_automation(request: Request, db: Session = Depends(get_db)):
    payload = await request.json()
    print(f"📦 Received automation payload: {payload}")

    data = payload.get("payload", {}).get("data", {})
    content = data.get("text_content", "")
    channel_id = data.get("parent")  # chat channel ID
    username = "someone"  # Could improve by mapping user ID to name
    workspace_id = os.getenv("CLICKUP_TEAM_ID")

    if not content or not channel_id:
        return {"status": "ignored", "reason": "missing content or channel_id"}

    if "@NP Analytics Bot" not in content:
        return {"status": "ignored", "reason": "bot not mentioned"}

    prompt = content.replace("@NP Analytics Bot", "").strip()
    print(f"🤖 Prompting assistant: {prompt}")

    try:
        reply = run_assistant_with_tools(prompt)
        formatted_reply = f"{reply}"
        post_message(db, workspace_id, channel_id, formatted_reply)
        return {"status": "replied", "message": formatted_reply}
    except Exception as e:
        print(f"❌ Assistant error: {e}")
        return {"status": "error", "message": str(e)}


@router.post("/clickup/register-webhook")
def register_clickup_webhook(db: Session = Depends(get_db)):
    workspace_id = os.getenv("CLICKUP_TEAM_ID", "45004558")
    token = get_token_by_workspace(db, workspace_id)
    if not token:
        return {"status": "error", "message": "No ClickUp token found"}

    headers = {
        "Authorization": token.access_token,
        "Content-Type": "application/json"
    }
    payload = {
        "endpoint": "https://np-analytics-app.onrender.com/webhooks/clickup/chat",
        "events": ["taskCommentPosted"],
        "secret": "optional-secret-string"
    }

    resp = requests.post(
        f"https://api.clickup.com/api/v2/team/{workspace_id}/webhook",
        headers=headers,
        json=payload
    )

    print("📡 Webhook registration response:", resp.status_code, resp.text)
    if resp.status_code == 200:
        return {"status": "success", "webhook_id": resp.json().get("id")}
    return {"status": "error", "code": resp.status_code, "message": resp.text}
