# app/mailchimp/weekly_summary.py
from fastapi import APIRouter
import requests
from typing import Dict, List
from app.config import settings
from app.db import get_conn
from app.utils.common import get_previous_week_dates, mailchimp_auth

router = APIRouter(prefix="/mailchimp", tags=["Mailchimp"])

AUDIENCES: Dict[str, str] = {
    "Northpoint Church": settings.MAILCHIMP_AUDIENCE_NORTHPOINT,
    "InsideOut Parents": settings.MAILCHIMP_AUDIENCE_INSIDEOUT,
    "Transit Parents": settings.MAILCHIMP_AUDIENCE_TRANSIT,
    "Upstreet Parents": settings.MAILCHIMP_AUDIENCE_UPSTREET,
    "Waumba Land Parents": settings.MAILCHIMP_AUDIENCE_WAUMBA,
}

MC_BASE = f"https://{settings.MAILCHIMP_SERVER_PREFIX}.api.mailchimp.com/3.0"

def _window_to_rfc3339(start_date_iso: str, end_date_iso: str) -> tuple[str, str]:
    return f"{start_date_iso}T00:00:00Z", f"{end_date_iso}T23:59:59Z"

@router.get("/weekly-summary")
def weekly_summary():
    week_start, week_end = get_previous_week_dates()
    since_send_time, before_send_time = _window_to_rfc3339(week_start, week_end)

    auth = mailchimp_auth("user", settings.MAILCHIMP_API_KEY)
    results: List[Dict] = []

    for audience_name, list_id in AUDIENCES.items():
        params = {
            "status": "sent",
            "list_id": list_id,
            "since_send_time": since_send_time,
            "before_send_time": before_send_time,
            "count": 1000,
        }
        try:
            resp = requests.get(f"{MC_BASE}/campaigns", auth=auth, params=params, timeout=30)
            resp.raise_for_status()
        except Exception:
            results.append({"audience": audience_name, "num_emails": 0, "avg_open_rate": 0.0, "avg_click_rate": 0.0})
            continue

        campaigns = (resp.json() or {}).get("campaigns", [])
        total_proxy_open = total_click = 0.0
        count = 0

        for c in campaigns:
            r = requests.get(f"{MC_BASE}/reports/{c['id']}", auth=auth, timeout=30)
            if r.status_code != 200:
                continue
            rep = r.json() or {}
            opens = rep.get("opens") or {}
            clicks = rep.get("clicks") or {}

            proxy_open_rate = opens.get("proxy_excluded_open_rate")
            click_rate = clicks.get("click_rate", 0.0)

            if proxy_open_rate is not None:
                total_proxy_open += float(proxy_open_rate)
                total_click += float(click_rate or 0.0)
                count += 1

        if count > 0:
            results.append({
                "audience": audience_name,
                "num_emails": count,
                "avg_open_rate": round((total_proxy_open / count) * 100.0, 2),
                "avg_click_rate": round((total_click / count) * 100.0, 3),
            })
        else:
            results.append({"audience": audience_name, "num_emails": 0, "avg_open_rate": 0.0, "avg_click_rate": 0.0})

    conn = get_conn()
    cur = conn.cursor()
    try:
        for item in results:
            cur.execute("""
                INSERT INTO mailchimp_weekly_summary
                (week_start, week_end, audience_name, audience_id, email_count, avg_open_rate, avg_click_rate)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (week_start, audience_id)
                DO UPDATE SET
                email_count    = EXCLUDED.email_count,
                avg_open_rate  = EXCLUDED.avg_open_rate,
                avg_click_rate = EXCLUDED.avg_click_rate
            """, (
                week_start, week_end,
                item["audience"],
                AUDIENCES[item["audience"]],
                item["num_emails"],
                item["avg_open_rate"],    # already computed a few lines above
                item["avg_click_rate"],   # already computed a few lines above
            ))
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return {"status": "saved", "summary": results}
