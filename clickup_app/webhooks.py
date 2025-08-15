# clickup_app/webhooks.py
import re
import time
from fastapi import APIRouter, Request, BackgroundTasks, Depends
from sqlalchemy.orm import Session
from app.db import get_db
from datetime import datetime
from pytz import timezone
from collections import OrderedDict

from clickup_app.assistant_client import run_assistant_with_tools
from clickup_app.clickup_client import (
    post_message,
    get_channel_members_map,
    format_user_mention,
    get_bot_user_id,     # ✅ import this
)
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

    content      = (data.get("text_content") or data.get("content") or "").strip()
    channel_id   = data.get("parent") or data.get("channel_id")
    user_id      = str(data.get("userid") or (data.get("user") or {}).get("id") or "")
    workspace_id = body.get("team_id") or os.getenv("CLICKUP_WORKSPACE_ID")
    msg_id = str(data.get("id") or f"{workspace_id}:{channel_id}:{user_id}:{content}")
    if _seen(msg_id):
        return {"status": "ignored_duplicate"}

    # 🚫 Ignore if missing basics
    if not content or not channel_id:
        return {"status": "ignored"}

    # 🚫 Ignore the bot's own messages to prevent loops
    try:
        bot_user_id = get_bot_user_id(db, workspace_id)
        if user_id == bot_user_id:
            return {"status": "ignored_self"}
    except Exception as e:
        # If we can't resolve bot id, fail-safe to proceed; worst case OU guard below still helps.
        print(f"[clickup] could not resolve bot user id: {e}")

    # ── Branch 1: bot mention ─────────────────────────────────────────────────
    if "@NP Analytics Bot" in content:
        prompt = content.replace("@NP Analytics Bot", "").strip()

        def handle():
            try:
                reply = run_assistant_with_tools(prompt)

                display_name = None
                try:
                    members_map = get_channel_members_map(db, workspace_id, channel_id)
                    display_name = members_map.get(user_id)
                except Exception as e:
                    print(f"[clickup] members lookup failed: {e}")

                mention = format_user_mention(user_id, display_name) if display_name else format_user_mention(user_id)
                message = f"{mention} {reply}".strip()
                post_message(db, workspace_id, channel_id, message)
                print("✅ Posted reply to ClickUp (OAuth)")
            except Exception as e:
                print(f"❌ Error handling webhook: {e}")

        background_tasks.add_task(handle)
        return {"status": "accepted"}

    # ── Branch 2: fun OU message ──────────────────────────────────────────────
    elif "ou" in content.casefold():
        now = datetime.now(timezone('America/Chicago'))
        reply = (
            f"I have detected OU in your message. The time is {now.strftime('%I:%M %p')} and OU *still sucks*! 🤘🐂"
        )

        display_name = None
        try:
            members_map = get_channel_members_map(db, workspace_id, channel_id)
            display_name = members_map.get(user_id)
        except Exception as e:
            print(f"[clickup] members lookup failed: {e}")

        mention = format_user_mention(user_id, display_name) if display_name else format_user_mention(user_id)
        message = f"{mention} {reply}".strip()
        post_message(db, workspace_id, channel_id, message)
        return {"status": "ok"}

    return {"status": "ignored"}

_DEDUPE: "OrderedDict[str, float]" = OrderedDict()
_DEDUPE_TTL = 60.0  # seconds

def _seen(key: str) -> bool:
    now = time.time()
    # purge old
    while _DEDUPE and (now - next(iter(_DEDUPE.values()))) > _DEDUPE_TTL:
        _DEDUPE.popitem(last=False)
    if key in _DEDUPE:
        return True
    _DEDUPE[key] = now
    return False