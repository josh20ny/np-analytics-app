# clickup_app/models.py

from sqlalchemy import Column, String, DateTime
from app.db import Base
from datetime import datetime

class ClickUpToken(Base):
    __tablename__ = "clickup_tokens"

    workspace_id  = Column(String,   primary_key=True, index=True)
    access_token  = Column(String,   nullable=False)
    refresh_token = Column(String,   nullable=False)
    expires_at    = Column(DateTime, nullable=False, default=datetime.utcnow)
