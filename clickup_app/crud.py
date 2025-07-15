# clickup_app/crud.py

from sqlalchemy.orm import Session
from clickup_app.models import ClickUpToken
from datetime import datetime, timedelta

def get_token(db: Session, workspace_id: str) -> ClickUpToken | None:
    return db.query(ClickUpToken).filter_by(workspace_id=workspace_id).first()

def create_or_update_token(
    db: Session,
    workspace_id: str,
    access_token: str,
    refresh_token: str,
    expires_in: int
) -> ClickUpToken:
    """
    Upsert the token row for a workspace, setting expires_at based on now + expires_in seconds.
    """
    expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
    token = get_token(db, workspace_id)
    if token:
        token.access_token  = access_token
        token.refresh_token = refresh_token
        token.expires_at    = expires_at
    else:
        token = ClickUpToken(
            workspace_id=workspace_id,
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=expires_at
        )
        db.add(token)
    db.commit()
    return token
