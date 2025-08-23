# clickup_app/clickup_client.py

import requests
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from typing import Optional, Iterable, Tuple
from functools import lru_cache
import os, json

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

# clickup_app/clickup_client.py  (DM helpers)

def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json; charset=utf-8",
        "Accept":        "application/json",
    }

def _normalize_channel_id(data: dict) -> str:
    container = data.get("data") or data.get("channel") or data
    channel_id = str(
        (container or {}).get("id")
        or (container or {}).get("channel_id")
        or data.get("id")
        or data.get("channel_id")
        or ""
    ).strip()
    return channel_id

def _get_members(token: str, workspace_id: str, channel_id: str) -> list[str]:
    url = f"{API_BASE}/workspaces/{workspace_id}/chat/channels/{channel_id}/members"
    r = requests.get(url, headers=_headers(token), timeout=30)
    try:
        j = r.json()
    except Exception:
        return []
    members = j.get("members") or j.get("data") or []
    out: list[str] = []
    for m in members:
        uid = (
            m.get("id")
            or (m.get("user") or {}).get("id")
            or (m.get("member") or {}).get("id")
        )
        if uid is not None:
            out.append(str(uid))
    return out

def ensure_dm_channel(db: Session, workspace_id: str, member_user_ids: Iterable[str]) -> str:
    """
    Create (or resolve) a DM that includes the recipients you pass.
    IMPORTANT: Do NOT include the bot user id; ClickUp includes the caller automatically.
    Body must use 'user_ids' per v3 docs.
    """
    access_token = get_access_token(db, workspace_id)
    bot_uid = str(get_bot_user_id(db, workspace_id))

    # recipients only (exclude bot), de-dupe, limit ≤ 10
    recips: list[str] = []
    for uid in member_user_ids:
        sid = str(uid).strip()
        if sid and sid != bot_uid and sid not in recips:
            recips.append(sid)
    if not recips:
        raise ValueError("ensure_dm_channel requires at least one non-bot recipient user_id.")
    if len(recips) > 10:
        raise ValueError("ClickUp direct_message supports up to 10 recipients.")

    # Create/resolve the DM with the correct key: user_ids
    url = f"{API_BASE}/workspaces/{workspace_id}/chat/channels/direct_message"
    payload = {"user_ids": recips}
    resp = requests.post(url, json=payload, headers=_headers(access_token), timeout=30)
    resp.raise_for_status()

    data = resp.json() if resp.headers.get("content-type","").startswith("application/json") else {}
    channel_id = _normalize_channel_id(data)
    if not channel_id:
        raise RuntimeError(f"Create DM returned unexpected body: {resp.text}")

    # Optional safety: verify recipients are actually members
    have = set(_get_members(access_token, workspace_id, channel_id))
    missing = [u for u in recips if u not in have]
    if missing:
        # Fail fast rather than silently posting into a bot-only DM
        raise RuntimeError(
            f"DM channel {channel_id} does not include recipients {missing}. "
            f"Members present: {sorted(list(have))}. Payload sent: {payload}."
        )

    return channel_id

def send_dm(
    db: Session,
    workspace_id: str,
    to_user_ids: Iterable[str] | str,
    content: str,
    *,
    content_format: str = "text/md",
) -> Tuple[str, dict]:
    if isinstance(to_user_ids, str):
        to_ids = [to_user_ids]
    else:
        to_ids = [str(x).strip() for x in to_user_ids if str(x).strip()]

    channel_id = ensure_dm_channel(db, workspace_id, to_ids)

    token = get_access_token(db, workspace_id)
    post_url = f"{API_BASE}/workspaces/{workspace_id}/chat/channels/{channel_id}/messages"
    body = {"content": content, "content_format": content_format, "type": "message"}
    r = requests.post(post_url, json=body, headers=_headers(token), timeout=30)
    r.raise_for_status()
    try:
        msg = r.json()
    except Exception:
        msg = {"raw": r.text}
    return channel_id, msg

# alias
post_dm = send_dm