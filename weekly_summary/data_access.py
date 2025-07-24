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
    

def fetch_records_for_date(key: str, date_value: str) -> list[dict]:
    """
    key must be one of the keys in TABLES (e.g. "AdultAttendance", "GroupsSummary", etc.).
    date_value should be an ISO date string, e.g. "2025-04-01".
    """
    tbl, date_col = TABLES[key]
    with engine.connect() as conn:
        df = pd.read_sql(
            text(f"SELECT * FROM {tbl} WHERE {date_col} = :date"),
            conn,
            params={"date": date_value},
            parse_dates=[date_col]
        )
    return df.to_dict(orient="records")

def fetch_records_for_range(key: str, start_date: str, end_date: str) -> list[dict]:
    """
    Fetch rows where date_col BETWEEN start_date AND end_date (inclusive).
    Both dates should be ISO strings, e.g. "2025-04-01", "2025-04-30".
    """
    tbl, date_col = TABLES[key]
    with engine.connect() as conn:
        df = pd.read_sql(
            text(
                f"SELECT * FROM {tbl}"
                f" WHERE {date_col} BETWEEN :start AND :end"
                f" ORDER BY {date_col}"
            ),
            conn,
            params={"start": start_date, "end": end_date},
            parse_dates=[date_col]
        )
    return df.to_dict(orient="records")

def aggregate_total_attendance(table_key: str, start_date: str, end_date: str) -> int:
    """
    Return SUM(total_attendance) for a given table_key between start_date and end_date (inclusive).
    """
    tbl, date_col = TABLES[table_key]
    with engine.connect() as conn:
        df = pd.read_sql(
            text(
                f"SELECT COALESCE(SUM(total_attendance),0) AS total "
                f"FROM {tbl} "
                f"WHERE {date_col} BETWEEN :start AND :end"
            ),
            conn,
            params={"start": start_date, "end": end_date},
        )
    return int(df["total"].iloc[0])
