# clickup_app/webhooks.py

from fastapi import APIRouter, Request, Depends
from sqlalchemy.orm import Session
from app.db import get_db
from clickup_app.assistant_client import get_reply_from_assistant
from clickup_app.clickup_client import post_message

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
