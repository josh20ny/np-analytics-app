from fastapi import APIRouter, Request, Depends
from app.db import get_db
from sqlalchemy.orm import Session
from clickup_app.assistant_client import get_reply_from_assistant
from clickup_app.clickup_client import post_message

import json  # 👈 for pretty-printing payloads

router = APIRouter()

@router.post("/webhooks/clickup/chat")
async def receive_chat_webhook(request: Request, db: Session = Depends(get_db)):
    payload = await request.json()

    print("📦 Incoming ClickUp Payload:")
    print(json.dumps(payload, indent=2))  # 🔍 show full JSON structure

    event_type = payload.get("event")
    print(f"🔔 Event Type: {event_type}")

    if event_type == "messageCreated":
        data = payload.get("data", {})
        message = data.get("content", "")
        channel_id = data.get("channel_id")
        workspace_id = payload.get("team_id")

        print(f"💬 Message Received: {message}")
        print(f"📺 Channel ID: {channel_id} | 🧭 Workspace ID: {workspace_id}")

        if not (message and channel_id and workspace_id):
            print("⚠️ Missing content, channel_id, or workspace_id")
            return {"status": "ignored", "reason": "Missing message or metadata"}

        if "@NP Analytics Bot" not in message:
            print("🙈 Bot not mentioned — ignoring")
            return {"status": "ignored", "reason": "Bot not mentioned"}

        try:
            # Clean up prompt
            cleaned = message.replace("@NP Analytics Bot", "").strip()
            print(f"🧼 Cleaned Prompt: {cleaned}")

            # Send to Assistant
            print("⏳ Sending to OpenAI Assistant…")
            reply = get_reply_from_assistant(cleaned)
            print(f"✅ Assistant Response: {reply}")

            # Tag user in reply
            user_info = data.get("user", {})
            username = user_info.get("username") or user_info.get("email") or "there"
            mention = f"@{username}"
            final_reply = f"{mention} {reply}"

            # Post to ClickUp
            print("📤 Posting reply to ClickUp…")
            post_message(db, workspace_id, channel_id, final_reply)

            print("✅ Message successfully posted.")
            return {"status": "replied", "message": final_reply}

        except Exception as e:
            print(f"❌ Error during assistant handling or post: {e}")
            return {"status": "error", "message": str(e)}

    print("⚠️ Unsupported event type")
    return {"status": "ignored", "reason": f"Unhandled event: {event_type}"}



