import requests
from datetime import datetime, timedelta
from isodate import parse_duration
from ..config import settings
from .analytics_api import get_average_watch_time


def get_recent_youtube_livestreams():
    thirty_days_ago = (datetime.utcnow() - timedelta(days=30)).isoformat("T") + "Z"
    search_url = (
        f"https://www.googleapis.com/youtube/v3/search"
        f"?key={settings.YOUTUBE_API_KEY}"
        f"&channelId={settings.CHANNEL_ID}"
        f"&part=snippet"
        f"&order=date"
        f"&maxResults=25"
        f"&type=video"
        f"&publishedAfter={thirty_days_ago}"
        f"&videoDuration=long"
    )
    items = requests.get(search_url).json().get("items", [])
    video_ids = [i["id"]["videoId"] for i in items if i["id"]["kind"]=="youtube#video"]
    if not video_ids:
        return []

    stats_url = (
        f"https://www.googleapis.com/youtube/v3/videos"
        f"?key={settings.YOUTUBE_API_KEY}"
        f"&part=snippet,statistics,contentDetails"
        f"&id={','.join(video_ids)}"
    )
    livestreams = []
    for i in requests.get(stats_url).json().get("items", []):
        vid = i["id"]
        title = i["snippet"]["title"]
        pub = i["snippet"]["publishedAt"]
        views = int(i["statistics"].get("viewCount",0))
        dur = parse_duration(i.get("contentDetails",{}).get("duration","")).total_seconds()
        bc = i["snippet"].get("liveBroadcastContent","none")
        if bc=="none" and dur<1800:
            continue
        livestreams.append({"video_id":vid,"title":title,"published_at":pub,"views":views})
    return livestreams


def insert_or_update_livestream(conn, video, today):
    video_id = video["video_id"]
    title = video["title"]
    pub_date = datetime.strptime(video["published_at"], "%Y-%m-%dT%H:%M:%SZ").date()
    views = video["views"]
    days_ago = (today - pub_date).days

    initial_views = views if 0 <= days_ago <= 6 else None
    views_1w = views if 7 <= days_ago <= 13 else None
    views_4w = views if 21 <= days_ago <= 27 else None
    avg_watch = None
    if initial_views is not None:
        avg_watch = get_average_watch_time(video_id)
    action = (
        "inserted" if initial_views is not None else
        "updated_1_week" if views_1w is not None else
        "updated_4_weeks" if views_4w is not None else None
    )
    if action is None:
        return None

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO livestreams (
            video_id, title, published_at, initial_views,
            views_1_week_later, views_4_weeks_later,
            avg_watch_duration, last_checked
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(video_id) DO UPDATE SET
            title=EXCLUDED.title,
            published_at=EXCLUDED.published_at,
            initial_views=COALESCE(livestreams.initial_views,EXCLUDED.initial_views),
            views_1_week_later=COALESCE(livestreams.views_1_week_later,EXCLUDED.views_1_week_later),
            views_4_weeks_later=COALESCE(livestreams.views_4_weeks_later,EXCLUDED.views_4_weeks_later),
            avg_watch_duration=COALESCE(livestreams.avg_watch_duration,EXCLUDED.avg_watch_duration),
            last_checked=EXCLUDED.last_checked;
        """,
        (
            video_id, title, pub_date,
            initial_views, views_1w, views_4w,
            avg_watch, today
        )
    )
    return {"action": action, "video_id": video_id}