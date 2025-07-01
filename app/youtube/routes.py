from fastapi import APIRouter
from .data_api import get_recent_youtube_livestreams, insert_or_update_livestream
from .analytics_api import get_weekly_youtube_metrics, store_weekly_summary_to_db
from ..db import get_conn
from datetime import datetime

router = APIRouter(prefix="/youtube", tags=["YouTube"])

@router.get("/livestreams")
def track_livestreams():
    conn = get_conn()
    today = datetime.utcnow().date()
    tracked = []
    for vid in get_recent_youtube_livestreams():
        res = insert_or_update_livestream(conn, vid, today)
        if res:
            tracked.append(res)
    conn.commit()
    conn.close()
    return {"status": "done", "livestreams_tracked": tracked}

@router.get("/weekly-summary")
def weekly_summary():
    summary = get_weekly_youtube_metrics()
    store_weekly_summary_to_db(summary)
    return {"status": "saved", "summary": summary}