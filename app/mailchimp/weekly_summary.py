# app/mailchimp/weekly_summary.py

from __future__ import annotations

from datetime import datetime, timedelta, date
from typing import Optional, List, Dict, Any

import requests
from fastapi import APIRouter, Query, HTTPException

from app.db import get_conn
from app.utils.common import get_previous_week_dates, mailchimp_auth
from app.config import settings

router = APIRouter(prefix="/mailchimp", tags=["Mailchimp"])

# ──────────────────────────────────────────────────────────────────────────────
# Config derived from your real settings
# ──────────────────────────────────────────────────────────────────────────────

MC_BASE = f"https://{settings.MAILCHIMP_SERVER_PREFIX}.api.mailchimp.com/3.0"

# Build from your five env vars (only include ones that are set)
AUDIENCES: Dict[str, str] = {}
for label, val in [
    ("Northpoint Church", settings.MAILCHIMP_AUDIENCE_NORTHPOINT),
    ("InsideOut Parents", settings.MAILCHIMP_AUDIENCE_INSIDEOUT),
    ("Transit Parents", settings.MAILCHIMP_AUDIENCE_TRANSIT),
    ("Upstreet Parents", settings.MAILCHIMP_AUDIENCE_UPSTREET),
    ("Waumba Land Parents", settings.MAILCHIMP_AUDIENCE_WAUMBA),
]:
    if (val or "").strip():
        AUDIENCES[label] = val

if not AUDIENCES:
    raise RuntimeError(
        "No Mailchimp audience IDs are set. Check .env (MAILCHIMP_AUDIENCE_*) and restart."
    )

# ──────────────────────────────────────────────────────────────────────────────
# Date helpers
# ──────────────────────────────────────────────────────────────────────────────

def _iso(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def _monday_sunday_from_week_end(week_end: date) -> tuple[str, str]:
    monday = week_end - timedelta(days=6)
    return _iso(monday), _iso(week_end)

def _monday_sunday_from_week_start(week_start: date) -> tuple[str, str]:
    sunday = week_start + timedelta(days=6)
    return _iso(week_start), _iso(sunday)

def _window_to_rfc3339(week_start_iso: str, week_end_iso: str) -> tuple[str, str]:
    # whole-day UTC bounds
    return f"{week_start_iso}T00:00:00Z", f"{week_end_iso}T23:59:59Z"

def _compute_windows(
    week_end_str: Optional[str],
    week_start_str: Optional[str],
    weeks: int,
) -> List[tuple[str, str]]:
    """
    Returns a list of (week_start_iso, week_end_iso) windows to process.
    If neither is provided, use last full Mon..Sun and go back `weeks` windows.
    """
    windows: List[tuple[str, str]] = []
    we = _parse_date(week_end_str)
    ws = _parse_date(week_start_str)

    if we and ws:
        raise HTTPException(status_code=400, detail="Provide only one of week_end or week_start.")

    if we:
        for i in range(weeks):
            end_d = we - timedelta(days=7 * i)
            windows.append(_monday_sunday_from_week_end(end_d))
        return windows

    if ws:
        for i in range(weeks):
            start_d = ws - timedelta(days=7 * i)
            windows.append(_monday_sunday_from_week_start(start_d))
        return windows

    # Default to last full completed Mon..Sun window
    last_start_iso, last_end_iso = get_previous_week_dates()
    last_end = _parse_date(last_end_iso)
    for i in range(weeks):
        end_d = last_end - timedelta(days=7 * i)
        windows.append(_monday_sunday_from_week_end(end_d))
    return windows

# ──────────────────────────────────────────────────────────────────────────────
# Mailchimp fetch
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_campaign_reports_for_audience(
    list_id: str, since_send_time: str, before_send_time: str, timeout: int = 30
) -> List[Dict[str, Any]]:
    """
    Fetch sent campaigns for a given audience and return a list of per-campaign report dicts
    with proxy_excluded_open_rate and click_rate (if available).
    """
    auth = mailchimp_auth("user", settings.MAILCHIMP_API_KEY)

    # 1) Get campaigns for this list in the time window
    campaigns_url = f"{MC_BASE}/campaigns"
    params = {
        "status": "sent",
        "list_id": list_id,
        "since_send_time": since_send_time,
        "before_send_time": before_send_time,
        "count": 1000,
    }
    resp = requests.get(campaigns_url, auth=auth, params=params, timeout=timeout)
    resp.raise_for_status()
    campaigns = (resp.json() or {}).get("campaigns", []) or []

    reports: List[Dict[str, Any]] = []
    if not campaigns:
        return reports

    # 2) Pull minimal report fields per campaign
    for c in campaigns:
        camp_id = c.get("id")
        if not camp_id:
            continue
        report_url = f"{MC_BASE}/reports/{camp_id}"
        report_params = {
            "fields": "id,opens.proxy_excluded_open_rate,clicks.click_rate"
        }
        r = requests.get(report_url, auth=auth, params=report_params, timeout=timeout)
        if r.status_code >= 400:
            continue
        data = r.json() or {}
        opens = data.get("opens", {}) or {}
        clicks = data.get("clicks", {}) or {}
        reports.append(
            {
                "proxy_excluded_open_rate": float(opens.get("proxy_excluded_open_rate") or 0.0),
                "click_rate": float(clicks.get("click_rate") or 0.0),
            }
        )

    return reports

def _build_results_for_window(week_start: str, week_end: str) -> List[Dict[str, Any]]:
    """
    Aggregate per-audience weekly metrics.
    Returns items like:
      {
        "audience": "Northpoint Church",
        "num_emails": 6,
        "avg_open_rate": 36.2,   # percent
        "avg_click_rate": 5.9    # percent
      }
    """
    since_ts, before_ts = _window_to_rfc3339(week_start, week_end)
    results: List[Dict[str, Any]] = []

    for audience_name, list_id in AUDIENCES.items():
        try:
            reports = _fetch_campaign_reports_for_audience(list_id, since_ts, before_ts)
        except Exception as e:
            print(f"⚠️ Mailchimp fetch failed for {audience_name}: {e}")
            results.append(
                {"audience": audience_name, "num_emails": 0, "avg_open_rate": 0.0, "avg_click_rate": 0.0}
            )
            continue

        count = len(reports)
        if count == 0:
            results.append(
                {"audience": audience_name, "num_emails": 0, "avg_open_rate": 0.0, "avg_click_rate": 0.0}
            )
            continue

        # Mailchimp returns FRACTIONS (e.g., 0.362). Store as PERCENT.
        avg_open = round(sum(r["proxy_excluded_open_rate"] for r in reports) / count * 100.0, 2)
        avg_click = round(sum(r["click_rate"] for r in reports) / count * 100.0, 3)

        results.append(
            {
                "audience": audience_name,
                "num_emails": count,
                "avg_open_rate": avg_open,
                "avg_click_rate": avg_click,
            }
        )

    return results

# ──────────────────────────────────────────────────────────────────────────────
# DB upsert (matches your current schema)
# ──────────────────────────────────────────────────────────────────────────────

def _upsert_results(conn, week_start: str, week_end: str, items: List[Dict[str, Any]]) -> int:
    """
    Writes to mailchimp_weekly_summary (avg_open_rate, avg_click_rate).
    Returns number of upserted rows.
    """
    cur = conn.cursor()
    n = 0
    for item in items:
        cur.execute(
            """
            INSERT INTO mailchimp_weekly_summary (
                week_start, week_end, audience_name, audience_id,
                email_count, avg_open_rate, avg_click_rate
            ) VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(week_start, audience_id) DO UPDATE SET
                email_count    = EXCLUDED.email_count,
                avg_open_rate  = EXCLUDED.avg_open_rate,
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
            ),
        )
        n += 1
    return n

# ──────────────────────────────────────────────────────────────────────────────
# Route
# ──────────────────────────────────────────────────────────────────────────────

@router.get("/weekly-summary")
def weekly_summary(
    week_end: Optional[str] = Query(
        None, description="Sunday of the target week (YYYY-MM-DD), e.g. 2025-10-12"
    ),
    week_start: Optional[str] = Query(
        None, description="Monday of the target week (YYYY-MM-DD), e.g. 2025-10-06"
    ),
    weeks: int = Query(
        1, ge=1, le=104, description="Number of weeks to process (backwards from target)"
    ),
    dry_run: bool = Query(False, description="Compute but do not write to DB"),
):
    """
    Build/store Mailchimp weekly summary for a specific week (or backfill N weeks).
    If no week is provided, uses the last full Monday..Sunday window.
    """
    try:
        windows = _compute_windows(week_end, week_start, weeks)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    processed: List[Dict[str, Any]] = []
    conn = get_conn()

    try:
        for ws, we in windows:
            items = _build_results_for_window(ws, we)
            upserts = 0
            if not dry_run:
                upserts = _upsert_results(conn, ws, we, items)

            processed.append(
                {
                    "week_start": ws,
                    "week_end": we,
                    "audiences": len(items),
                    "rows_upserted": upserts,
                    "dry_run": dry_run,
                }
            )

        if not dry_run:
            conn.commit()

        return {"ok": True, "processed": processed}

    finally:
        conn.close()
