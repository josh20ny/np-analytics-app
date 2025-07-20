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
async def receive_chat_webhook(request: Request, db: Session = Depends(get_db)):
    payload = await request.json()
    event_type = payload.get("event")

    if event_type != "messageCreated":
        return {"status": "ignored", "reason": f"Unhandled event type: {event_type}"}

    data = payload.get("data", {})
    content = data.get("content", "")
    channel_id = data.get("channel_id")
    user_info = data.get("user", {})
    username = user_info.get("username", "")
    workspace_id = payload.get("team_id")

    if not content or not channel_id or not workspace_id:
        return {"status": "ignored", "reason": "missing fields"}

    print(f"📦 Incoming ClickUp Payload: {content}")

    # Only respond if bot is tagged
    if "@NP Analytics Bot" not in content:
        return {"status": "ignored", "reason": "not mentioned"}

    # Strip bot mention for cleaner prompt
    prompt = content.replace("@NP Analytics Bot", "").strip()
    print(f"⏳ Sending to OpenAI Assistant: {prompt}")

    try:
        reply = get_reply_from_assistant(prompt)
        formatted_reply = f"@{username} {reply}"
        post_message(db, workspace_id, channel_id, formatted_reply)
        return {"status": "replied", "message": formatted_reply}
    except Exception as e:
        print(f"❌ Error during assistant reply: {e}")
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
        "events": ["chat.messageCreated"],
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
