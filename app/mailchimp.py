from fastapi import APIRouter
import requests
from .config import settings
from .db import get_conn
from .google_sheets import get_previous_week_dates
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

router = APIRouter(prefix="/mailchimp", tags=["Mailchimp"])

AUDIENCES = {
    "Northpoint Church": settings.MAILCHIMP_AUDIENCE_NORTHPOINT,
    "InsideOut Parents": settings.MAILCHIMP_AUDIENCE_INSIDEOUT,
    "Transit Parents": settings.MAILCHIMP_AUDIENCE_TRANSIT,
    "Upstreet Parents": settings.MAILCHIMP_AUDIENCE_UPSTREET,
    "Waumba Land Parents": settings.MAILCHIMP_AUDIENCE_WAUMBA
}

@router.get("/weekly-summary")
def weekly_summary():
    start, end = get_previous_week_dates()
    auth = ("anystring", settings.MAILCHIMP_API_KEY)
    results = []

    for name, list_id in AUDIENCES.items():
        # Step 1: Fetch all campaigns for this audience last week
        url = f"https://{settings.MAILCHIMP_SERVER_PREFIX}.api.mailchimp.com/3.0/campaigns"
        params = {
            "since_send_time": start,
            "before_send_time": end,
            "list_id": list_id,
            "status": "sent"
        }
        resp = requests.get(url, auth=auth, params=params)
        if resp.status_code != 200:
            print(f"⚠️ Failed to fetch campaigns for {name}: {resp.text}")
            continue

        camps = resp.json().get("campaigns", [])
        total_proxy_open = 0.0
        total_click = 0.0
        count = 0

        # Step 2: For each campaign, fetch the full report
        for c in camps:
            camp_id = c["id"]
            rep_url = f"https://{settings.MAILCHIMP_SERVER_PREFIX}.api.mailchimp.com/3.0/reports/{camp_id}"
            rep_resp = requests.get(rep_url, auth=auth)
            if rep_resp.status_code != 200:
                print(f"❌ Failed to fetch report for campaign {camp_id}")
                continue

            report = rep_resp.json()
            proxy_open = report.get("opens", {}).get("proxy_excluded_open_rate")
            click_rate = report.get("clicks", {}).get("click_rate", 0.0)

            if proxy_open is not None:
                total_proxy_open += proxy_open
                total_click += click_rate or 0.0
                count += 1

        if count:
            results.append({
                "audience": name,
                "num_emails": count,
                "avg_open_rate": round(total_proxy_open / count * 100, 2),
                "avg_click_rate": round(total_click / count * 100, 3)
            })
        else:
            results.append({
                "audience": name,
                "num_emails": 0,
                "avg_open_rate": 0.0,
                "avg_click_rate": 0.0
            })

    # Step 3: Store in DB
    conn = get_conn()
    cur = conn.cursor()
    for item in results:
        cur.execute(
            """
            INSERT INTO mailchimp_weekly_summary (
                week_start, week_end, audience_name, audience_id,
                email_count, avg_open_rate, avg_click_rate
            ) VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(week_start,audience_id) DO UPDATE SET
                email_count = EXCLUDED.email_count,
                avg_open_rate = EXCLUDED.avg_open_rate,
                avg_click_rate = EXCLUDED.avg_click_rate;
            """,
            (
                start,
                end,
                item["audience"],
                AUDIENCES[item["audience"]],
                item["num_emails"],
                item["avg_open_rate"],
                item["avg_click_rate"]
            )
        )
    conn.commit()
    cur.close()
    conn.close()

    return {"status": "saved", "summary": results}
