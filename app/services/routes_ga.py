# app/routes_ga.py
from fastapi import APIRouter
from typing import List, Dict
import asyncpg

from app.db import get_conn  # <-- use psycopg2 connection (matches attendance code)
from app.utils.common import get_previous_week_dates_cst
from app.config import GA4_PROPERTY_ID, GA4_GIVING_DOMAINS
from app.services.ga4 import run_report

router = APIRouter(prefix="/ga4", tags=["Google Analytics"])

def _num(val, to_int=False):
    try:
        f = float(val)
        return int(f) if to_int else f
    except Exception:
        return 0 if to_int else 0.0

# -------------------- UPSERT HELPERS --------------------

async def upsert_summary(conn: asyncpg.Connection, week_end: str, week_start: str, users: int, page_views: int, avg_eng: float):
    await conn.execute("""
        INSERT INTO website_weekly_summary (week_end, week_start, users, page_views, avg_engagement_time_sec)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (week_end)
        DO UPDATE SET users = EXCLUDED.users,
                      page_views = EXCLUDED.page_views,
                      avg_engagement_time_sec = EXCLUDED.avg_engagement_time_sec
    """, week_end, week_start, users, page_views, avg_eng)

async def upsert_page_views(conn: asyncpg.Connection, week_end: str, week_start: str, rows: List[Dict]):
    for r in rows:
        title = (r.get("pageTitle") or "(untitled)").strip()
        views = _num(r.get("screenPageViews"), to_int=True)
        await conn.execute("""
            INSERT INTO website_page_views_weekly (week_end, week_start, page_key, page_title, views)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (week_end, page_key)
            DO UPDATE SET page_title = EXCLUDED.page_title,
                          views = EXCLUDED.views
        """, week_end, week_start, title, title, views)

async def upsert_channel_group(conn: asyncpg.Connection, week_end: str, week_start: str, rows: List[Dict]):
    for r in rows:
        # GA4 uses sessionDefaultChannelGroup (newer API also exposes defaultChannelGroup; session* is safest)
        cg = (r.get("sessionDefaultChannelGroup") or r.get("defaultChannelGroup") or "(other)").strip()
        users = _num(r.get("activeUsers"), to_int=True)
        views = _num(r.get("screenPageViews"), to_int=True)
        await conn.execute("""
            INSERT INTO website_channel_group_weekly (week_end, week_start, channel_group, users, page_views)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (week_end, channel_group)
            DO UPDATE SET users = EXCLUDED.users,
                          page_views = EXCLUDED.page_views
        """, week_end, week_start, cg, users, views)

async def upsert_devices(conn: asyncpg.Connection, week_end: str, week_start: str, rows: List[Dict]):
    for r in rows:
        dev = (r.get("deviceCategory") or "(unknown)").strip()
        sessions = _num(r.get("sessions"), to_int=True)
        users = _num(r.get("activeUsers"), to_int=True)
        views = _num(r.get("screenPageViews"), to_int=True)
        await conn.execute("""
            INSERT INTO website_device_weekly (week_end, week_start, device_category, sessions, users, page_views)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (week_end, device_category)
            DO UPDATE SET sessions = EXCLUDED.sessions,
                          users = EXCLUDED.users,
                          page_views = EXCLUDED.page_views
        """, week_end, week_start, dev, sessions, users, views)

async def upsert_conversions(conn: asyncpg.Connection, week_end: str, week_start: str, give_count: int, next_step_count: int):
    for ctype, cnt in (("give", give_count), ("next_step", next_step_count)):
        await conn.execute("""
            INSERT INTO website_conversions_weekly (week_end, week_start, conversion_type, event_count)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (week_end, conversion_type)
            DO UPDATE SET event_count = EXCLUDED.event_count
        """, week_end, week_start, ctype, cnt)


# -------------------- MAIN ROUTE --------------------

@router.post("/sync-week")
def sync_last_full_week():
    if not GA4_PROPERTY_ID:
        return {"ok": False, "error": "GA4_PROPERTY_ID not set"}

    week_start, week_end = get_previous_week_dates_cst()

    # 1) Summary
    srows = run_report(
        dimensions=[],
        metrics=["activeUsers", "screenPageViews", "userEngagementDuration"],
        start_date=week_start, end_date=week_end,
    )
    s = srows[0] if srows else {}
    users = _num(s.get("activeUsers"), to_int=True)
    page_views = _num(s.get("screenPageViews"), to_int=True)
    total_eng = _num(s.get("userEngagementDuration"))
    avg_eng = (total_eng / users) if users > 0 else 0.0 

    # 2) Per-page (title)
    page_rows = run_report(
        dimensions=["pageTitle"],
        metrics=["screenPageViews"],
        start_date=week_start, end_date=week_end,
    )

    # 3) Channel group
    chan_rows = run_report(
        dimensions=["sessionDefaultChannelGroup"],
        metrics=["activeUsers", "screenPageViews"],
        start_date=week_start, end_date=week_end,
    )

    # 4) Devices
    dev_rows = run_report(
        dimensions=["deviceCategory"],
        metrics=["sessions", "activeUsers", "screenPageViews"],
        start_date=week_start, end_date=week_end,
    )

    # 5) Conversions (outbound click bucketing)
    outbound_rows = run_report(
        dimensions=["linkDomain"],
        metrics=["eventCount"],
        start_date=week_start, end_date=week_end,
        dimension_filters={"eventName": ["click"], "outbound": ["true"]},
    )
    give_count = 0
    next_step_count = 0
    for r in outbound_rows:
        domain = (r.get("linkDomain") or "").lower()
        cnt = _num(r.get("eventCount"), to_int=True)
        if any(domain.endswith(d) or domain == d for d in GA4_GIVING_DOMAINS):
            give_count += cnt
        else:
            next_step_count += cnt

    # ---- write to DB with psycopg2 (one transaction) ----
    conn = get_conn()
    cur = conn.cursor()
    try:
        # summary
        cur.execute("""
            INSERT INTO website_weekly_summary
              (week_end, week_start, users, page_views, avg_engagement_time_sec)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (week_end) DO UPDATE
              SET users=EXCLUDED.users,
                  page_views=EXCLUDED.page_views,
                  avg_engagement_time_sec=EXCLUDED.avg_engagement_time_sec
        """, (week_end, week_start, users, page_views, avg_eng))

        # per-page
        for r in page_rows:
            title = (r.get("pageTitle") or "(untitled)").strip()
            views = _num(r.get("screenPageViews"), to_int=True)
            cur.execute("""
                INSERT INTO website_page_views_weekly
                  (week_end, week_start, page_key, page_title, views)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (week_end, page_key) DO UPDATE
                  SET page_title=EXCLUDED.page_title,
                      views=EXCLUDED.views
            """, (week_end, week_start, title, title, views))

        # channel group
        for r in chan_rows:
            cg = (r.get("sessionDefaultChannelGroup") or r.get("defaultChannelGroup") or "(other)").strip()
            u  = _num(r.get("activeUsers"), to_int=True)
            pv = _num(r.get("screenPageViews"), to_int=True)
            cur.execute("""
                INSERT INTO website_channel_group_weekly
                  (week_end, week_start, channel_group, users, page_views)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (week_end, channel_group) DO UPDATE
                  SET users=EXCLUDED.users,
                      page_views=EXCLUDED.page_views
            """, (week_end, week_start, cg, u, pv))

        # devices
        for r in dev_rows:
            dev = (r.get("deviceCategory") or "(unknown)").strip()
            sess = _num(r.get("sessions"), to_int=True)
            u    = _num(r.get("activeUsers"), to_int=True)
            pv   = _num(r.get("screenPageViews"), to_int=True)
            cur.execute("""
                INSERT INTO website_device_weekly
                  (week_end, week_start, device_category, sessions, users, page_views)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (week_end, device_category) DO UPDATE
                  SET sessions=EXCLUDED.sessions,
                      users=EXCLUDED.users,
                      page_views=EXCLUDED.page_views
            """, (week_end, week_start, dev, sess, u, pv))

        # conversions
        for ctype, cnt in (("give", give_count), ("next_step", next_step_count)):
            cur.execute("""
                INSERT INTO website_conversions_weekly
                  (week_end, week_start, conversion_type, event_count)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (week_end, conversion_type) DO UPDATE
                  SET event_count=EXCLUDED.event_count
            """, (week_end, week_start, ctype, cnt))

        conn.commit()
    finally:
        cur.close()
        conn.close()

    return {
        "ok": True,
        "week_start": week_start,
        "week_end": week_end,
        "users": users,
        "page_views": page_views,
        "avg_engagement_time_sec": round(avg_eng, 2),
        "pages": len(page_rows),
        "channels": len(chan_rows),
        "devices": len(dev_rows),
        "give": give_count,
        "next_step": next_step_count
    }