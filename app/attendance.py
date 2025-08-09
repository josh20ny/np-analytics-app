from fastapi import APIRouter
from .models import AttendanceInput
from .db import get_conn
from .utils.common import compute_adult_attendance_metrics

router = APIRouter(prefix="/attendance", tags=["Attendance"])

@router.get("/process-sheet")
def process_sheet():
    # left as-is; google_sheets handles it
    from .google_sheets import process_adult_attendance_from_sheet
    return process_adult_attendance_from_sheet()

@router.post("/adults")
def submit_adults(data: AttendanceInput):
    m = compute_adult_attendance_metrics(
        chair_count=data.chair_count,
        a930=data.attendance_930,
        a1100=data.attendance_1100,
    )

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
            m.pc_930,
            m.pc_1100,
            m.pd_930,
            m.pd_1100,
            m.total,
        )
    )
    conn.commit()
    cur.close()
    conn.close()

    return {"status": "success", "date": data.date, "total_attendance": m.total}
