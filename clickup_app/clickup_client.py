# clickup_app/clickup_client.py

import requests
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from typing import Optional

from clickup_app.config import CLIENT_ID, CLIENT_SECRET
from clickup_app.crud import get_token, create_or_update_token

TOKEN_URL = "https://api.clickup.com/api/v2/oauth/token"
API_BASE  = "https://api.clickup.com/api/v3"

def get_access_token(db, workspace_id: str) -> str:
    token_row = get_token(db, workspace_id)
    if not token_row:
        raise RuntimeError(f"No ClickUp OAuth token found for workspace {workspace_id}")

    now = datetime.utcnow()
    # Refresh only if actually expired (or exactly at expiry)
    if token_row.expires_at is None or token_row.expires_at <= now:
        try:
            # ✅ Use form-encoded body (not JSON)
            form = {
                "client_id":     CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "grant_type":    "refresh_token",
                "refresh_token": token_row.refresh_token,
            }
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            resp = requests.post(TOKEN_URL, data=form, headers=headers, timeout=30)
            # If ClickUp returns an error JSON, raise for clearer logs
            if resp.status_code >= 400:
                try:
                    print(f"[clickup] refresh failed {resp.status_code}: {resp.text}")
                finally:
                    resp.raise_for_status()

            data = resp.json()
            token_row = create_or_update_token(
                db=db,
                workspace_id=workspace_id,
                access_token=data["access_token"],
                refresh_token=data.get("refresh_token", token_row.refresh_token),
                expires_in=int(data.get("expires_in", 3600)),
            )
        except Exception as e:
            # Bubble up with context so you can see it in Render logs
            raise RuntimeError(f"ClickUp token refresh failed: {e}") from e

    return token_row.access_token  # raw token for v3 Chat APIs

def post_message(
    db: Session,
    workspace_id: str,
    channel_id: str,
    content: str,
    *,
    msg_type: str = "message",
    content_format: str = "text/md",
):
    """
    Post a message to a ClickUp Chat v3 channel using the workspace OAuth token.
    """
    access_token = get_access_token(db, workspace_id)
    url = f"{API_BASE}/workspaces/{workspace_id}/chat/channels/{channel_id}/messages"
    headers = {
        "Authorization": access_token,        # raw token (per ClickUp docs)
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }
    payload = {
        "type":           msg_type,
        "content_format": content_format,
        "content":        content,
    }
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
    """
    Returns {user_id: display_name} for members of a channel.
    Falls back gracefully if fields are missing.
    """
    access_token = get_access_token(db, workspace_id)
    url = f"{API_BASE}/workspaces/{workspace_id}/chat/channels/{channel_id}/members"
    headers = {"Authorization": access_token, "Accept": "application/json"}
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

