from fastapi import APIRouter, HTTPException
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta
from dateutil.parser import parse
import os

from .config import settings
from .db import get_conn

router = APIRouter(prefix="/google-sheets", tags=["Google Sheets"])

def get_service(scopes: list[str]):
    """
    Initialize Google Sheets API client using the service account file path
    configured via Render Secret Files (mounted at /etc/secrets/...).
    """
    creds = Credentials.from_service_account_file(
        settings.GOOGLE_SERVICE_ACCOUNT_FILE,
        scopes=scopes
    )
    return build("sheets", "v4", credentials=creds)

def get_previous_week_dates() -> tuple[str, str]:
    today = datetime.utcnow().date()
    last_sunday = today - timedelta(days=today.weekday() + 1)
    last_monday = last_sunday - timedelta(days=6)
    return last_monday.isoformat(), last_sunday.isoformat()

def process_adult_attendance_from_sheet():
    service = get_service(["https://www.googleapis.com/auth/spreadsheets"])
    sheet_api = service.spreadsheets()

    # Fetch rows from A2:F (skip headers)
    result = sheet_api.values().get(
        spreadsheetId=settings.GOOGLE_SPREADSHEET_ID,
        range=f"{settings.GOOGLE_SHEET_NAME}!A2:F"
    ).execute()
    rows = result.get("values", [])

    updates = []
    conn = get_conn()
    cur = conn.cursor()

    # Enumerate starting at row 2 to align the F-column checkmarks
    for idx, row in enumerate(rows, start=2):
        # skip if fewer than 5 columns or already processed
        if len(row) < 5 or (len(row) >= 6 and row[5].strip() == "✅"):
            continue

        raw_date = row[1]
        # Parse date in various formats
        try:
            dt = parse(raw_date, dayfirst=False).date()
        except (ValueError, TypeError):
            if isinstance(raw_date, (int, float)):
                # Excel’s “day zero” is 1899-12-30
                dt = datetime(1899, 12, 30) + timedelta(days=int(raw_date))
            else:
                continue

        try:
            chair_count = int(row[2])
            a930 = int(row[3])
            a1100 = int(row[4])
        except (ValueError, IndexError):
            continue

        total = a930 + a1100
        pc_930 = round((a930 / chair_count) * 100, 2) if chair_count else 0
        pc_1100 = round((a1100 / chair_count) * 100, 2) if chair_count else 0
        pd_930 = round((a930 / total) * 100, 2) if total else 0
        pd_1100 = round((a1100 / total) * 100, 2) if total else 0

        cur.execute(
            """
            INSERT INTO adult_attendance (
                date, chair_count, attendance_930, attendance_1100,
                percent_capacity_930, percent_capacity_1100,
                percent_distribution_930, percent_distribution_1100,
                total_attendance
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING;
            """,
            (dt, chair_count, a930, a1100, pc_930, pc_1100, pd_930, pd_1100, total)
        )

        updates.append({
            "range": f"{settings.GOOGLE_SHEET_NAME}!F{idx}",
            "values": [["✅"]]
        })

    conn.commit()
    cur.close()
    conn.close()

    # Batch-update the sheet with ✅ marks
    if updates:
        sheet_api.values().batchUpdate(
            spreadsheetId=settings.GOOGLE_SPREADSHEET_ID,
            body={"valueInputOption": "RAW", "data": updates}
        ).execute()

    return {"status": "done", "processed_rows": len(updates)}

@router.get("/process")
def trigger_process():
    """Trigger the attendance-processing routine."""
    return process_adult_attendance_from_sheet()

