# clickup_app/clickup_client.py

import requests
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from clickup_app.config import CLIENT_ID, CLIENT_SECRET
from clickup_app.crud import get_token, create_or_update_token

TOKEN_URL = "https://api.clickup.com/api/v2/oauth/token"
API_BASE  = "https://api.clickup.com/api/v3"

def get_access_token(db: Session, workspace_id: str) -> str:
    """
    Return a valid access_token for the given workspace.
    Refresh if expired or within 60 seconds of expiring.
    """
    token_row = get_token(db, workspace_id)
    if not token_row:
        raise RuntimeError(f"No ClickUp OAuth token found for workspace {workspace_id}")

    # Refresh if expired or will expire within 60s
    now = datetime.utcnow()
    if token_row.expires_at is None or token_row.expires_at <= now + timedelta(seconds=60):
        resp = requests.post(
            TOKEN_URL,
            json={
                "client_id":     CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "grant_type":    "refresh_token",
                "refresh_token": token_row.refresh_token,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        # Upsert with new values and recomputed expires_at
        token_row = create_or_update_token(
            db=db,
            workspace_id=workspace_id,
            access_token=data["access_token"],
            refresh_token=data.get("refresh_token", token_row.refresh_token),
            expires_in=int(data.get("expires_in", 3600)),
        )

    # ClickUp expects the raw token in the Authorization header (no "Bearer ")
    return token_row.access_token

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
