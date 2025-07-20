# clickup_app/clickup_client.py

import requests
from sqlalchemy.orm import Session
from datetime import datetime
from clickup_app.config import CLIENT_ID, CLIENT_SECRET
from clickup_app.crud import get_token
from clickup_app.models import ClickUpToken

TOKEN_URL = "https://api.clickup.com/api/v2/oauth/token"
API_BASE = "https://api.clickup.com/api/v3"


def get_access_token(db: Session, workspace_id: str) -> str:
    """
    Returns the stored access_token for the given workspace.
    """
    token = get_token_by_workspace(db, workspace_id)
    if not token:
        raise Exception(f"No ClickUp OAuth token found for workspace {workspace_id}")
    return token.access_token


def get_token_by_workspace(db: Session, workspace_id: str):
    return db.query(ClickUpToken).filter_by(workspace_id=workspace_id).first()


def post_message(db: Session, workspace_id: str, channel_id: str, message: str):
    token = get_token_by_workspace(db, workspace_id)
    if not token:
        raise Exception("No ClickUp token found")
    headers = {
        "Authorization": token.access_token,
        "Content-Type": "application/json"
    }
    payload = {
        "channel_id": channel_id,
        "content": message
    }
    resp = requests.post("https://api.clickup.com/api/v2/chat/message", headers=headers, json=payload)
    if resp.status_code != 200:
        print("❌ Error posting message:", resp.status_code, resp.text)
        raise Exception(f"ClickUp API error: {resp.text}")

