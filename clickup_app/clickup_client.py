# clickup_app/clickup_client.py

import requests
from sqlalchemy.orm import Session
from datetime import datetime

from clickup_app.config import CLIENT_ID, CLIENT_SECRET
from clickup_app.crud import get_token, create_or_update_token
from clickup_app.models import ClickUpToken

TOKEN_URL = "https://api.clickup.com/api/v2/oauth/token"
API_BASE = "https://api.clickup.com/api/v3"


def get_token_by_workspace(db: Session, workspace_id: str) -> ClickUpToken | None:
    return get_token(db, workspace_id)


def refresh_access_token(db: Session, token_row: ClickUpToken) -> ClickUpToken:
    print(f"🔁 Refreshing expired token for workspace {token_row.workspace_id}...")

    resp = requests.post(
        TOKEN_URL,
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": token_row.refresh_token,
            "grant_type": "refresh_token",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    if resp.status_code != 200:
        raise Exception(f"❌ Failed to refresh ClickUp token: {resp.text}")

    data = resp.json()
    return create_or_update_token(
        db=db,
        workspace_id=token_row.workspace_id,
        access_token=data["access_token"],
        refresh_token=data.get("refresh_token", token_row.refresh_token),
        expires_in=data.get("expires_in", 3600)
    )


def get_access_token(db: Session, workspace_id: str) -> str:
    token = get_token_by_workspace(db, workspace_id)
    if not token:
        raise Exception(f"❌ No ClickUp token found for workspace {workspace_id}")

    if token.expires_at <= datetime.utcnow():
        token = refresh_access_token(db, token)

    return token.access_token


def post_message(db: Session, workspace_id: str, channel_id: str, message: str):
    token = get_token_by_workspace(db, workspace_id)
    if not token:
        print(f"❌ No token found in DB for workspace_id: {workspace_id}")
        raise Exception("No ClickUp token found")

    token = (
        refresh_access_token(db, token)
        if token.expires_at <= datetime.utcnow()
        else token
    )

    headers = {
        "Authorization": token.access_token,
        "Content-Type": "application/json"
    }
    payload = {
        "channel_id": channel_id,
        "content": message
    }

    resp = requests.post(
        "https://api.clickup.com/api/v2/chat/message",
        headers=headers,
        json=payload
    )

    if resp.status_code != 200:
        print("❌ Error posting message:", resp.status_code, resp.text)
        raise Exception(f"ClickUp API error: {resp.text}")
