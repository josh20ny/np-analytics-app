from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.db import get_db
from clickup_app.clickup_client import send_dm, ensure_dm_channel, get_access_token
import os, requests

API_BASE = "https://api.clickup.com/api/v3"

router = APIRouter(prefix="/debug", tags=["Debug"])

def _ids_from_env() -> list[str]:
    ids = [s.strip() for s in os.getenv("CLICKUP_DM_USER_IDS", "").split(",") if s.strip()]
    fallback = os.getenv("CLICKUP_JOSH_USER_ID") or os.getenv("CLICKUP_JOSH_CHANNEL_ID", "")
    if fallback and fallback not in ids:
        ids.append(fallback)
    return ids

# app/debug/routes.py (replace dm_test_get)
@router.get("/dm-test")
def dm_test_get(
    msg: str = Query("DM test via /debug ✅", description="Message to send"),
    ids: str = Query("", description="Comma-separated ClickUp user IDs to DM (optional; overrides env)"),
    db: Session = Depends(get_db),
):
    ws = os.getenv("CLICKUP_WORKSPACE_ID", "").strip()
    to_ids = [s.strip() for s in ids.split(",") if s.strip()] or _ids_from_env()
    if not (ws and to_ids):
        raise HTTPException(status_code=400, detail="Set CLICKUP_WORKSPACE_ID and CLICKUP_DM_USER_IDS or pass ?ids=...")

    # Resolve a channel but DO NOT post yet; we want to prove membership first
    from clickup_app.clickup_client import ensure_dm_channel, get_access_token
    channel_id = ensure_dm_channel(db, ws, to_ids)

    # audit members
    token = get_access_token(db, ws)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    mem = requests.get(f"{API_BASE}/workspaces/{ws}/chat/channels/{channel_id}/members", headers=headers, timeout=30)
    try:
        members_json = mem.json()
    except Exception:
        members_json = {"raw": mem.text[:800]}

    # now send the message
    from clickup_app.clickup_client import send_dm
    _, msg_json = send_dm(db, ws, to_ids, msg)

    return {
        "attempted_recipients": to_ids,
        "channel_id": channel_id,
        "members_response": members_json,
        "message": msg_json,
    }


@router.get("/dm-audit")
def dm_audit(
    channel_id: str = Query(..., description="Chat channel id like 1axdre-XXXXX"),
    db: Session = Depends(get_db),
):
    ws = os.getenv("CLICKUP_WORKSPACE_ID", "").strip()
    if not (ws and channel_id):
        raise HTTPException(400, "workspace_id and channel_id required")
    token = get_access_token(db, ws)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    mem = requests.get(f"{API_BASE}/workspaces/{ws}/chat/channels/{channel_id}/members",
                       headers=headers, timeout=30)
    msgs = requests.get(f"{API_BASE}/workspaces/{ws}/chat/channels/{channel_id}/messages",
                        headers=headers, timeout=30)

    try:
        mem_json = mem.json()
    except Exception:
        mem_json = {"raw": mem.text[:800]}

    try:
        msgs_json = msgs.json()
    except Exception:
        msgs_json = {"raw": msgs.text[:1200]}

    return {
        "members_status": mem.status_code,
        "members": mem_json,
        "messages_status": msgs.status_code,
        "recent_messages": msgs_json,
    }
