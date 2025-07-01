import json
from datetime import datetime, timedelta
from youtube_auth import get_youtube_analytics_service
from ..google_sheets import get_previous_week_dates


def get_average_watch_time(video_id: str, start_date=None, end_date=None):
    analytics = get_youtube_analytics_service()
    if not end_date:
        end_date = datetime.utcnow().date()
    if not start_date:
        start_date = end_date - timedelta(days=30)

    resp = analytics.reports().query(
        ids="channel==MINE",
        startDate=start_date.isoformat(),
        endDate=end_date.isoformat(),
        metrics="averageViewDuration",
        dimensions="video",
        filters=f"video=={video_id}"
    ).execute()
    rows = resp.get("rows", [])
    if rows and rows[0][1]:
        return int(rows[0][1])
    print(json.dumps(resp, indent=2))
    return None


def get_weekly_youtube_metrics():
    analytics = get_youtube_analytics_service()
    start, end = get_previous_week_dates()
    resp = analytics.reports().query(
        ids="channel==MINE",
        startDate=start,
        endDate=end,
        metrics="averageViewDuration,views,subscribersGained,subscribersLost",
        dimensions="day"
    ).execute()

    total_views = total_avg = days = subs_g = subs_l = 0
    for _, avg, views, g, l in resp.get("rows", []):
        total_avg += int(avg)
        total_views += int(views)
        subs_g += int(g)
        subs_l += int(l)
        days += 1
    avg_watch = total_avg//days if days else 0
    net_subs = subs_g - subs_l
    return {
        "week_start": start,
        "week_end": end,
        "avg_watch_duration": avg_watch,
        "total_views": total_views,
        "subscribers_gained": subs_g,
        "subscribers_lost": subs_l,
        "net_subscribers": net_subs
    }


def store_weekly_summary_to_db(summary: dict):
    from ..db import get_conn
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO weekly_youtube_summary (
            week_start, week_end, avg_watch_duration,
            total_views, subscribers_gained, subscribers_lost, net_subscribers
        ) VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(week_start) DO UPDATE SET
            avg_watch_duration=EXCLUDED.avg_watch_duration,
            total_views=EXCLUDED.total_views,
            subscribers_gained=EXCLUDED.subscribers_gained,
            subscribers_lost=EXCLUDED.subscribers_lost,
            net_subscribers=EXCLUDED.net_subscribers;
        """,
        (
            summary["week_start"],
            summary["week_end"],
            summary["avg_watch_duration"],
            summary["total_views"],
            summary["subscribers_gained"],
            summary["subscribers_lost"],
            summary["net_subscribers"]
        )
    )
    conn.commit()
    cur.close()
    conn.close()