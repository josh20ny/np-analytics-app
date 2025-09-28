# app/mailchimp.py
from fastapi import APIRouter
import requests
from .config import settings
from .db import get_conn
from .utils.common import get_previous_week_dates, mailchimp_auth  # helpers
from typing import Dict, List

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
    """
    Mailchimp expects full timestamps. We use whole-day bounds in UTC.
    e.g., '2025-08-04' → '2025-08-04T00:00:00Z'
    """
    start_ts = f"{start_date_iso}T00:00:00Z"
    end_ts   = f"{end_date_iso}T23:59:59Z"
    return start_ts, end_ts

@router.get("/weekly-summary")
def weekly_summary():
    # Previous completed Mon..Sun window (UTC helper you already had)
    week_start, week_end = get_previous_week_dates()
    since_send_time, before_send_time = _window_to_rfc3339(week_start, week_end)

    auth = mailchimp_auth("user", settings.MAILCHIMP_API_KEY)  # new helper
    results: List[Dict] = []

    for audience_name, list_id in AUDIENCES.items():
        # ── Step 1: fetch campaigns for this audience last week ────────────────
        campaigns_url = f"{MC_BASE}/campaigns"
        params = {
            "status": "sent",
            "list_id": list_id,
            # Use a full timestamp window to avoid partial-day mismatches
            "since_send_time": since_send_time,
            "before_send_time": before_send_time,
            "count": 1000,  # be generous; MC defaults to small pages
        }

        try:
            resp = requests.get(campaigns_url, auth=auth, params=params, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            print(f"⚠️ Failed to fetch campaigns for {audience_name}: {e}")
            results.append({
                "audience": audience_name,
                "num_emails": 0,
                "avg_open_rate": 0.0,
                "avg_click_rate": 0.0,
            })
            continue

        campaigns = (resp.json() or {}).get("campaigns", [])
        total_proxy_open = 0.0
        total_click = 0.0
        count = 0

        # ── Step 2: for each campaign, fetch condensed report ────────────────
        # We request only the fields we need to keep payloads light.
        for c in campaigns:
            camp_id = c.get("id")
            if not camp_id:
                continue

            report_url = f"{MC_BASE}/reports/{camp_id}"
            report_params = {
                "fields": "id,opens.proxy_excluded_open_rate,clicks.click_rate"
            }
            try:
                rep = requests.get(report_url, auth=auth, params=report_params, timeout=30)
                rep.raise_for_status()
            except Exception as e:
                print(f"❌ Failed to fetch report for campaign {camp_id}: {e}")
                continue

            data = rep.json() or {}
            opens = data.get("opens", {}) or {}
            clicks = data.get("clicks", {}) or {}

            proxy_open_rate = opens.get("proxy_excluded_open_rate")
            click_rate = clicks.get("click_rate", 0.0)

            # Only include rows where Mailchimp provides the proxy-excluded open metric
            if proxy_open_rate is not None:
                total_proxy_open += float(proxy_open_rate)
                total_click += float(click_rate or 0.0)
                count += 1

        if count > 0:
            results.append({
                "audience": audience_name,
                "num_emails": count,
                "avg_open_rate": round((total_proxy_open / count) * 100.0, 2),  # %
                "avg_click_rate": round((total_click / count) * 100.0, 3),     # %
            })
        else:
            results.append({
                "audience": audience_name,
                "num_emails": 0,
                "avg_open_rate": 0.0,
                "avg_click_rate": 0.0,
            })

    # ── Step 3: persist to DB ────────────────────────────────────────────────
    conn = get_conn()
    cur = conn.cursor()
    try:
        for item in results:
            cur.execute(
                """
                INSERT INTO mailchimp_weekly_summary (
                    week_start, week_end, audience_name, audience_id,
                    email_count, avg_open_rate, avg_click_rate
                ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(week_start, audience_id) DO UPDATE SET
                    email_count = EXCLUDED.email_count,
                    avg_open_rate = EXCLUDED.avg_open_rate,
                    avg_click_rate = EXCLUDED.avg_click_rate;
                """,
                (
                    week_start,
                    week_end,
                    item["audience"],
                    AUDIENCES[item["audience"]],
                    item["num_emails"],
                    item["avg_open_rate"],
                    item["avg_click_rate"],
                )
            )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return {"status": "saved", "summary": results}
