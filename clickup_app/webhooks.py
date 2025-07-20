# clickup_app/webhooks.py

from fastapi import APIRouter, Request, Depends
from sqlalchemy.orm import Session
from app.db import get_db
from clickup_app.assistant_client import get_reply_from_assistant
from clickup_app.clickup_client import post_message, get_token_by_workspace
import requests
import os

router = APIRouter()

@router.post("/webhooks/clickup/chat")
async def receive_clickup_automation(request: Request, db: Session = Depends(get_db)):
    payload = await request.json()
    print(f"📦 Received automation payload: {payload}")

    # Attempt to parse what we need
    content = payload.get("content", "") or payload.get("text", "")
    channel_id = payload.get("chat_id") or payload.get("channel_id")
    username = payload.get("username") or "someone"
    workspace_id = os.getenv("CLICKUP_TEAM_ID")

    if not content or not channel_id:
        return {"status": "ignored", "reason": "missing content or channel_id"}

    if "@NP Analytics Bot" not in content:
        return {"status": "ignored", "reason": "bot not mentioned"}

    prompt = content.replace("@NP Analytics Bot", "").strip()
    print(f"🤖 Prompting assistant: {prompt}")

    try:
        reply = get_reply_from_assistant(prompt)
        formatted_reply = f"@{username} {reply}"
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
        "events": ["messageCreated"],  # ✅ updated event name
        "secret": "optional-secret-string",
        "workspace_id": workspace_id   # ✅ required for chat-view webhook
    }

    resp = requests.post(
        "https://api.clickup.com/api/v2/webhook/chat-view",  # ✅ new endpoint
        headers=headers,
        json=payload
    )

    print("📡 Webhook registration response:", resp.status_code, resp.text)
    if resp.status_code == 200:
        return {"status": "success", "webhook_id": resp.json().get("id")}
    return {"status": "error", "code": resp.status_code, "message": resp.text}



@router.get("/clickup/debug-token")
def debug_clickup_token(db: Session = Depends(get_db)):
    workspace_id = os.getenv("CLICKUP_TEAM_ID", "45004558")
    token = get_token_by_workspace(db, workspace_id)
    if not token:
        return {"status": "error", "message": "No token found"}

    headers = {
        "Authorization": token.access_token
    }

    resp = requests.get("https://api.clickup.com/api/v2/team", headers=headers)
    return {
        "status": "ok" if resp.status_code == 200 else "error",
        "response_code": resp.status_code,
        "response": resp.text
    }
