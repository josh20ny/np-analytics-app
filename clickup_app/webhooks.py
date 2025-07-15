# clickup_app/webhooks.py

from fastapi import APIRouter, Request, Depends
from app.db import get_db
from sqlalchemy.orm import Session

router = APIRouter()

@router.post("/webhooks/clickup/chat")
async def receive_chat_webhook(request: Request, db: Session = Depends(get_db)):
    """
    Endpoint for ClickUp to POST Chat webhook events.
    You’ll register this URL in your ClickUp App Webhooks settings.
    """
    payload = await request.json()
    # TODO: parse payload, maybe call clickup_client.post_message to respond
    return {"status": "received", "payload_summary": payload.get("event")}
