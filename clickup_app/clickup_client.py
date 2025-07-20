# clickup_app/clickup_client.py

import requests
from datetime import datetime
from clickup_app.config import CLIENT_ID, CLIENT_SECRET
from clickup_app.crud import get_token, create_or_update_token
from app.db import get_db
from sqlalchemy.orm import Session
from clickup_app.models import ClickUpToken

TOKEN_URL = "https://api.clickup.com/api/v2/oauth/token"
API_BASE  = "https://api.clickup.com/api/v3"

def get_access_token(db: Session, workspace_id: str) -> str:
    """
    Returns the stored access_token for the given workspace.
    (Refresh logic removed since no refresh_token is available.)
    """
    token = get_token(db, workspace_id)
    if not token:
        raise Exception(f"No ClickUp OAuth token found for workspace {workspace_id}")
    return token.access_token

    # If it’s expired or about to, refresh it
    if token.expires_at <= datetime.utcnow():
        resp = requests.post(TOKEN_URL, json={
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type":    "refresh_token",
            "refresh_token": token.refresh_token
        })
        resp.raise_for_status()
        data = resp.json()
        token = create_or_update_token(
            db,
            workspace_id,
            data["access_token"],
            data["refresh_token"],
            data.get("expires_in", 3600)
        )
    return token.access_token

def post_message(db: Session, workspace_id: str, channel_id: str, content: str, msg_type: str = "message"):
    access_token = get_access_token(db, workspace_id)
    url = f"{API_BASE}/workspaces/{workspace_id}/chat/channels/{channel_id}/messages"
    headers = {
        "Authorization": access_token,
        "Content-Type":  "application/json"
    }
    payload = {"content": content, "type": msg_type}
    resp = requests.post(url, json=payload, headers=headers)
    resp.raise_for_status()
    return resp.json()

def get_token_by_workspace(db, workspace_id: str):
    return db.query(ClickUpToken).filter_by(workspace_id=workspace_id).first()

