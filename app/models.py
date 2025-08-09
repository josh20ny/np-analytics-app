from pydantic import BaseModel
from datetime import date
from sqlalchemy import Column, String, DateTime
from app.db import Base
from dataclasses import dataclass

class AttendanceInput(BaseModel):
    date: date
    chair_count: int
    attendance_930: int
    attendance_1100: int

class PlanningCenterToken(Base):
    __tablename__ = "planning_center_tokens"
    workspace_id  = Column(String, primary_key=True, index=True)
    access_token  = Column(String, nullable=False)
    refresh_token = Column(String, nullable=False)
    expires_at    = Column(DateTime, nullable=False)

@dataclass
class AdultAttendanceMetrics:
    total: int
    pc_930: float
    pc_1100: float
    pd_930: float
    pd_1100: float