from fastapi import APIRouter, HTTPException
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import os
from .config import settings
from .db import get_conn

router = APIRouter(prefix="/google-sheets", tags=["Google Sheets"])

# debug
@router.get("/debug/secrets")
def debug_list_secrets():
    try:
        files = os.listdir("/etc/secrets")
        return {"mounted_secrets": files}
    except FileNotFoundError:
        return {"error": "/etc/secrets does not exist"}



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

@router.get("/test-read")
def test_read():
    """
    Reads A1:D5 to verify credentials and sheet access.
    """
    try:
        service = get_service(["https://www.googleapis.com/auth/spreadsheets.readonly"])
        sheet_api = service.spreadsheets()
        result = sheet_api.values().get(
            spreadsheetId=settings.GOOGLE_SPREADSHEET_ID,
            range=f"{settings.GOOGLE_SHEET_NAME}!A1:D5"
        ).execute()
        return {"data": result.get("values", [])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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

    # Enumerate starting at row 2 to align the F‑column checkmarks
    for idx, row in enumerate(rows, start=2):
        if len(row) < 5 or (len(row) >= 6 and row[5].strip() == "✅"):
            continue
        try:
            date_val = datetime.strptime(row[1], "%Y-%m-%d").date()
            chair_count = int(row[2])
            a930 = int(row[3])
            a1100 = int(row[4])
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
                (
                    date_val, chair_count, a930, a1100,
                    pc_930, pc_1100, pd_930, pd_1100, total
                )
            )

            updates.append({
                "range": f"{settings.GOOGLE_SHEET_NAME}!F{idx}",
                "values": [["✅"]]
            })

        except Exception:
            continue

    conn.commit()
    cur.close()
    conn.close()

    # Batch‑update the sheet with ✅ marks
    if updates:
        sheet_api.values().batchUpdate(
            spreadsheetId=settings.GOOGLE_SPREADSHEET_ID,
            body={"valueInputOption": "RAW", "data": updates}
        ).execute()

    return {"status": "done", "processed_rows": len(updates)}

@router.get("/process")
def trigger_process():
    """Trigger the attendance‑processing routine."""
    return process_adult_attendance_from_sheet()
