from fastapi import APIRouter, Request, Depends
from app.db import get_db
from sqlalchemy.orm import Session
from clickup_app.assistant_client import get_reply_from_assistant
from clickup_app.clickup_client import post_message

router = APIRouter()

@router.post("/webhooks/clickup/chat")
async def receive_chat_webhook(request: Request, db: Session = Depends(get_db)):
    payload = await request.json()
    event_type = payload.get("event")

    if event_type == "messageCreated":
        data = payload.get("data", {})
        message = data.get("content", "")
        channel_id = data.get("channel_id")
        workspace_id = payload.get("team_id")

        # ✅ Require message metadata
        if not (message and channel_id and workspace_id):
            return {"status": "ignored", "reason": "Missing message or metadata"}

        # ✅ Only respond when bot is mentioned
        if "@NP Analytics Bot" not in message:
            return {"status": "ignored", "reason": "Bot not mentioned"}

        print(f"💬 Mentioned: {message}")

        try:
            # 🧼 Clean the prompt
            cleaned = message.replace("@NP Analytics Bot", "").strip()

            # 🤖 Generate response
            reply = get_reply_from_assistant(cleaned)

            # 👤 Tag the original sender
            user_info = data.get("user", {})
            username = user_info.get("username") or user_info.get("email") or "there"
            mention = f"@{username}"

            # 💬 Post back with mention
            final_reply = f"{mention} {reply}"
            post_message(db, workspace_id, channel_id, final_reply)

            return {"status": "replied", "message": final_reply}

        except Exception as e:
            print(f"❌ Assistant error: {e}")
            return {"status": "error", "message": str(e)}

    return {"status": "ignored", "reason": f"Unhandled event: {event_type}"}


