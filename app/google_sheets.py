from fastapi import APIRouter, HTTPException
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta
from dateutil.parser import parse
from app.utils.common import parse_sheet_date, compute_adult_attendance_metrics

from .config import settings
from .db import get_conn
from .utils.common import get_previous_week_dates, compute_adult_attendance_metrics

router = APIRouter(prefix="/google-sheets", tags=["Google Sheets"])

def get_service(scopes: list[str]):
    creds = Credentials.from_service_account_file(
        settings.GOOGLE_SERVICE_ACCOUNT_FILE,
        scopes=scopes
    )
    return build("sheets", "v4", credentials=creds)

def process_adult_attendance_from_sheet():
    """
    Reads rows A2:F from the configured Google Sheet and inserts any rows
    not yet marked as processed (F column != ✅) into adult_attendance.
    After successful insert, marks column F with ✅ via a single batchUpdate.
    """
    service = get_service(["https://www.googleapis.com/auth/spreadsheets"])
    sheet_api = service.spreadsheets()

    result = sheet_api.values().get(
        spreadsheetId=settings.GOOGLE_SPREADSHEET_ID,
        range=f"{settings.GOOGLE_SHEET_NAME}!A2:F",
        valueRenderOption="UNFORMATTED_VALUE"  # <-- NEW: dates come back as serial numbers
    ).execute()
    rows = result.get("values", [])

    updates = []   # queued ✅ updates for column F
    summary = []   # optional response summary

    conn = get_conn()
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO adult_attendance (
            date, chair_count, attendance_930, attendance_1100,
            percent_capacity_930, percent_capacity_1100,
            percent_distribution_930, percent_distribution_1100,
            total_attendance
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (date) DO NOTHING;
    """

    try:
        for idx, row in enumerate(rows, start=2):
            # Skip incomplete rows or rows already marked as processed (F column)
            if len(row) < 5 or (len(row) >= 6 and str(row[5]).strip() == "✅"):
                continue

            dt = parse_sheet_date(row[1])
            if not dt:
                continue

            # Safe integer parsing; blank cells become 0
            try:
                chair_count = int(row[2] or 0)
                a930        = int(row[3] or 0)
                a1100       = int(row[4] or 0)
            except (ValueError, IndexError):
                continue

            m = compute_adult_attendance_metrics(chair_count, a930, a1100)

            cur.execute(
                insert_sql,
                (dt, chair_count, a930, a1100, m.pc_930, m.pc_1100, m.pd_930, m.pd_1100, m.total)
            )

            # Queue the ✅ mark for this row’s F column
            updates.append({
                "range": f"{settings.GOOGLE_SHEET_NAME}!F{idx}",
                "values": [["✅"]],
            })

            summary.append({
                "date": dt,
                "attendance_930": a930,
                "percent_capacity_930": m.pc_930,
                "percent_distribution_930": m.pd_930,
                "attendance_1100": a1100,
                "percent_capacity_1100": m.pc_1100,
                "percent_distribution_1100": m.pd_1100,
                "total_attendance": m.total,
            })

        conn.commit()
    finally:
        cur.close()
        conn.close()

    # Batch the checkmarks once
    if updates:
        sheet_api.values().batchUpdate(
            spreadsheetId=settings.GOOGLE_SPREADSHEET_ID,
            body={"valueInputOption": "RAW", "data": updates}
        ).execute()

    return {"status": "done", "processed_rows": len(updates), "summary": summary}


@router.get("/process")
def trigger_process():
    return process_adult_attendance_from_sheet()
