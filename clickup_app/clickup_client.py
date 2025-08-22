# clickup_app/clickup_client.py

import requests
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from typing import Optional, Iterable
from functools import lru_cache
import os

from clickup_app.config import CLIENT_ID, CLIENT_SECRET
from clickup_app.crud import get_token, create_or_update_token

TOKEN_URL = "https://api.clickup.com/api/v2/oauth/token"
API_BASE  = "https://api.clickup.com/api/v3"
API_V2 = "https://api.clickup.com/api/v2"

def get_access_token(db: Session, workspace_id: str) -> str:
    """
    ClickUp OAuth access tokens currently do not expire.
    Just return the stored token. If API calls 401, the user must re-auth via /auth/start.
    """
    token_row = get_token(db, workspace_id)
    if not token_row or not token_row.access_token:
        raise RuntimeError(f"No ClickUp OAuth token found for workspace {workspace_id}")
    return token_row.access_token

@lru_cache(maxsize=32)
def get_bot_user_id(db, workspace_id: str) -> str:
    env_id = os.getenv("CLICKUP_BOT_USER_ID")
    if env_id:
        return str(env_id)
    access_token = get_access_token(db, workspace_id)
    resp = requests.get(f"{API_V2}/user",
                        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
                        timeout=30)
    resp.raise_for_status()
    data = resp.json() or {}
    uid = data.get("user", {}).get("id")
    if not uid:
        raise RuntimeError("Could not determine bot user id from /v2/user response")
    return str(uid)

def post_message(db, workspace_id: str, channel_id: str, content: str,
                 *, msg_type="message", content_format="text/md"):
    access_token = get_access_token(db, workspace_id)
    url = f"{API_BASE}/workspaces/{workspace_id}/chat/channels/{channel_id}/messages"
    headers = {
        "Authorization": f"Bearer {access_token}",  # ✅ Bearer for OAuth
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }
    payload = {"type": msg_type, "content_format": content_format, "content": content}
    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def format_user_mention(user_id: str, display_name: Optional[str] = None) -> str:
    """
    Returns a mention that renders as the user's name in ClickUp Chat and notifies them.
    If display_name is provided, show it; otherwise the link still resolves to the user.
    """
    url = f"clickup://user/{user_id}"
    return f"[{display_name}]({url})" if display_name else url

def get_channel_members_map(db, workspace_id: str, channel_id: str) -> dict[str, str]:
    access_token = get_access_token(db, workspace_id)
    url = f"{API_BASE}/workspaces/{workspace_id}/chat/channels/{channel_id}/members"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json() or {}
    members = data.get("members") or data.get("data") or []  # schema guard
    out = {}
    for m in members:
        # try a few likely shapes
        uid = str(m.get("id") or (m.get("user") or {}).get("id") or "")
        name = (
            m.get("username")
            or (m.get("user") or {}).get("username")
            or (m.get("user") or {}).get("email")
            or (m.get("user") or {}).get("name")
            or ""
        )
        if uid:
            out[uid] = name
    return out

# --- Direct Messages (DM) helpers --------------------------------------------

API_BASE = "https://api.clickup.com/api/v3"

def ensure_dm_channel(db: Session, workspace_id: str, member_user_ids: Iterable[str]) -> str:
    """
    Create (or return) a Direct Message channel between the bot user and member_user_ids.
    Returns the channel_id usable with post_message(...).
    Accepts several possible response shapes from ClickUp's experimental v3 Chat API.
    """
    access_token = get_access_token(db, workspace_id)
    bot_uid = get_bot_user_id(db, workspace_id)

    # de-dupe & coerce to strings
    uniq_ids: list[str] = []
    for uid in [bot_uid, *member_user_ids]:
        sid = str(uid).strip()
        if sid and sid not in uniq_ids:
            uniq_ids.append(sid)

    if not uniq_ids or len(uniq_ids) < 2:
        # A DM must include at least two members (bot + someone)
        raise ValueError("ensure_dm_channel requires at least one recipient user_id.")

    url = f"{API_BASE}/workspaces/{workspace_id}/chat/channels/direct_message"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    payload = {"member_user_ids": uniq_ids}
    resp = requests.post(url, json=payload, headers=headers, timeout=30)

    # Fallback for older/alternate key if they ever change it again
    if resp.status_code == 400 and "member_user_ids" in (resp.text or "").lower():
        alt_payload = {"member_ids": uniq_ids}
        resp = requests.post(url, json=alt_payload, headers=headers, timeout=30)

    resp.raise_for_status()
    data = resp.json() or {}

    # Normalize several possible shapes:
    # { "data": { "id": "..." } }
    # { "channel": { "id": "..." } }
    # { "id": "..." }
    # { "channel_id": "..." }
    container = data.get("data") or data.get("channel") or data
    channel_id = str(
        (container or {}).get("id")
        or (container or {}).get("channel_id")
        or data.get("id")
        or data.get("channel_id")
        or ""
    ).strip()

    if not channel_id:
        # Helpful diagnostic without leaking tokens
        raise RuntimeError(f"Create DM returned unexpected body: {data}")

    return channel_id


def send_dm(
    db: Session,
    workspace_id: str,
    to_user_ids: Iterable[str] | str,
    content: str,
    *,
    content_format: str = "text/md",
):
    """
    Resolve/create the DM channel and post a message to it.
    to_user_ids may be a single user_id (str) or an iterable of user_ids (max supported by ClickUp DM).
    """
    if isinstance(to_user_ids, str):
        to_ids = [to_user_ids]
    else:
        to_ids = [str(x).strip() for x in to_user_ids if str(x).strip()]

    channel_id = ensure_dm_channel(db, workspace_id, to_ids)
    # Reuse your existing poster
    return post_message(db, workspace_id, channel_id, content, msg_type="message", content_format=content_format)

# Friendly alias
post_dm = send_dm
