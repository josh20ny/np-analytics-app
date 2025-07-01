from pydantic import BaseModel
from datetime import date


class AttendanceInput(BaseModel):
    date: date
    chair_count: int
    attendance_930: int
    attendance_1100: int