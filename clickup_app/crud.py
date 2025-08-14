# clickup_app/crud.py

from sqlalchemy.orm import Session
from clickup_app.models import ClickUpToken
from datetime import datetime, timedelta

def get_token(db: Session, workspace_id: str) -> ClickUpToken | None:
    """
    Get the stored token for a given workspace.
    (This should not contain refresh logic — let clickup_client handle that.)
    """
    return db.query(ClickUpToken).filter_by(workspace_id=workspace_id).first()


def create_or_update_token(db, workspace_id, access_token, refresh_token=None, expires_in=None):
    expires_at = None
    if expires_in is not None:
        expires_at = datetime.utcnow() + timedelta(seconds=int(expires_in))

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

