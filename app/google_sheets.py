from fastapi import APIRouter, HTTPException
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from .config import settings
from .db import get_conn
from datetime import datetime, timedelta
import os, json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

router = APIRouter(prefix="/google-sheets", tags=["Google Sheets"])


def get_service(scopes):
    # this will read the file whose path is in the env var
    creds = service_account.Credentials.from_service_account_file(
        settings.GOOGLE_SERVICE_ACCOUNT_FILE,
        scopes=scopes
    )
    return build("sheets", "v4", credentials=creds)

@router.get("/test-read")
def test_read():
    try:
        service = get_service(["https://www.googleapis.com/auth/spreadsheets.readonly"] )
        sheet_api = service.spreadsheets()
        result = sheet_api.values().get(
            spreadsheetId=settings.SPREADSHEET_ID,
            range=f"{settings.SHEET_NAME}!A1:D5"
        ).execute()
        return {"data": result.get("values", [])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def get_previous_week_dates():
    today = datetime.utcnow().date()
    last_sunday = today - timedelta(days=today.weekday() + 1)
    last_monday = last_sunday - timedelta(days=6)
    return last_monday.isoformat(), last_sunday.isoformat()


def process_adult_attendance_from_sheet():
    service = get_service(["https://www.googleapis.com/auth/spreadsheets"])
    sheet_api = service.spreadsheets()
    
    result = sheet_api.values().get(
        spreadsheetId=settings.SPREADSHEET_ID,
        range=f"{settings.SHEET_NAME}!A2:F"
    ).execute()
    rows = result.get("values", [])

    updates = []
    conn = get_conn()
    cur = conn.cursor()

    for i, row in enumerate(rows):
        if len(row) < 5 or (len(row) >= 6 and row[5].strip() == '✅'):
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
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(date) DO NOTHING;
                """,
                (
                    date_val, chair_count, a930, a1100,
                    pc_930, pc_1100, pd_930, pd_1100, total
                )
            )

            updates.append({
                "range": f"{settings.SHEET_NAME}!F{i+2}",
                "values": [["✅"]]
            })
        except Exception:
            continue

    conn.commit()
    cur.close()
    conn.close()

    if updates:
        body = {"valueInputOption": "RAW", "data": updates}
        sheet_api.values().batchUpdate(
            spreadsheetId=settings.SPREADSHEET_ID,
            body=body
        ).execute()

    return {"status": "done", "processed_rows": len(updates)}

@router.get("/process")
def trigger_process():
    return process_adult_attendance_from_sheet()