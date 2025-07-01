from fastapi import APIRouter
from .models import AttendanceInput
from .db import get_conn
from .google_sheets import process_adult_attendance_from_sheet

router = APIRouter(prefix="/attendance", tags=["Attendance"])

@router.get("/process-sheet")
def process_sheet():
    return process_adult_attendance_from_sheet()

@router.post("/adults")
def submit_adults(data: AttendanceInput):
    total = data.attendance_930 + data.attendance_1100
    pc_930 = (data.attendance_930 / data.chair_count) * 100 if data.chair_count else 0
    pc_1100 = (data.attendance_1100 / data.chair_count) * 100 if data.chair_count else 0
    pd_930 = (data.attendance_930 / total) * 100 if total else 0
    pd_1100 = (data.attendance_1100 / total) * 100 if total else 0

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO adult_attendance (
            date, chair_count, attendance_930, attendance_1100,
            percent_capacity_930, percent_capacity_1100,
            percent_distribution_930, percent_distribution_1100,
            total_attendance
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(date) DO UPDATE SET
            chair_count = EXCLUDED.chair_count,
            attendance_930 = EXCLUDED.attendance_930,
            attendance_1100 = EXCLUDED.attendance_1100,
            percent_capacity_930 = EXCLUDED.percent_capacity_930,
            percent_capacity_1100 = EXCLUDED.percent_capacity_1100,
            percent_distribution_930 = EXCLUDED.percent_distribution_930,
            percent_distribution_1100 = EXCLUDED.percent_distribution_1100,
            total_attendance = EXCLUDED.total_attendance;
        """,
        (
            data.date,
            data.chair_count,
            data.attendance_930,
            data.attendance_1100,
            round(pc_930, 2),
            round(pc_1100, 2),
            round(pd_930, 2),
            round(pd_1100, 2),
            total
        )
    )
    conn.commit()
    cur.close()
    conn.close()

    return {"status": "success", "date": data.date, "total_attendance": total}