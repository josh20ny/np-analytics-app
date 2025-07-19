import pandas as pd
from sqlalchemy import create_engine
from weekly_summary.config import DATABASE_URL
from dateutil.relativedelta import relativedelta
from datetime    import timedelta
from sqlalchemy.sql import text

# 1) same TABLES mapping as before
TABLES = {
    "AdultAttendance":     ("adult_attendance",         "date"),
    "GroupsSummary":       ("groups_summary",          "date"),
    "InsideOutAttendance": ("insideout_attendance",    "date"),
    "Livestreams":         ("livestreams",             "published_at"),
    "MailchimpSummary":    ("mailchimp_weekly_summary","week_end"),
    "TransitAttendance":   ("transit_attendance",      "date"),
    "UpStreetAttendance":  ("upstreet_attendance",     "date"),
    "WaumbaLandAttendance":("waumbaland_attendance",   "date"),
    "WeeklyYouTubeSummary":("weekly_youtube_summary",  "week_end"),
}

engine = create_engine(DATABASE_URL)

def fetch_all_with_yoy() -> dict[str, dict[str, dict]]:
    """
    Returns a dict mapping each table key to a dict with:
      - "current": the latest row as a dict
      - "prior":   the row from exactly 1 year ago (same date field)
    """
    result = {}
    for key, (tbl, date_col) in TABLES.items():
        # 1) load this week's row
        df_cur = pd.read_sql(
            f"SELECT * FROM {tbl} ORDER BY {date_col} DESC LIMIT 1",
            engine, parse_dates=[date_col]
        )
        row_cur = df_cur.iloc[0].to_dict() if not df_cur.empty else {}

        # 2) load prior-year row
        row_pri = {}
        if row_cur:
            dt_lastyear = row_cur[date_col] - timedelta(weeks=52)
            iso = dt_lastyear.isoformat()
            df_pri = pd.read_sql(
                f"SELECT * FROM {tbl} WHERE {date_col} = '{iso}' LIMIT 1",
                engine,
                parse_dates=[date_col]
            )
            if not df_pri.empty:
                row_pri = df_pri.iloc[0].to_dict()

        result[key] = {"current": row_cur, "prior": row_pri}
    return result

def fetch_all_mailchimp_rows_for_latest_week() -> list[dict]:
    with engine.connect() as conn:
        df = pd.read_sql("""
            SELECT * FROM mailchimp_weekly_summary
            WHERE week_end = (
                SELECT MAX(week_end) FROM mailchimp_weekly_summary
            )
            ORDER BY audience_name
        """, conn)
        return df.to_dict(orient="records")
