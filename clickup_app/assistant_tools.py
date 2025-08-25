# clickup_app/assistant_tools.py

import pandas as pd
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
from datetime import timedelta, date
from sqlalchemy.sql import text
import calendar
from app.config import settings

# ── 1) TABLES mapping ──────────────────────────────────────────────────────────
TABLES = {
    "AdultAttendance":      ("adult_attendance",         "date"),
    "GroupsSummary":        ("groups_summary",           "date"),
    "InsideOutAttendance":  ("insideout_attendance",     "date"),
    "Livestreams":          ("livestreams",              "published_at"),
    "MailchimpSummary":     ("mailchimp_weekly_summary", "week_end"),
    "TransitAttendance":    ("transit_attendance",       "date"),
    "UpStreetAttendance":   ("upstreet_attendance",      "date"),
    "WaumbaLandAttendance": ("waumbaland_attendance",    "date"),
    "WeeklyYouTubeSummary": ("weekly_youtube_summary",   "week_end"),
    "WeeklyGivingSummary":  ("weekly_giving_summary",    "week_end"),
    "ServingVolunteersWeekly": ("serving_volunteers_weekly", "week_end"),
}

DATABASE_URL = settings.DATABASE_URL

engine = create_engine(DATABASE_URL)

def fetch_all_with_yoy() -> dict[str, dict[str, dict]]:
    """
    Returns a dict mapping each table key to:
      - 'current': latest row as a dict
      - 'prior':   same-date row from one year ago
    """  # based on weekly_summary/data_access.py :contentReference[oaicite:0]{index=0}
    result = {}
    for key, (tbl, date_col) in TABLES.items():
        # current row
        df_cur = pd.read_sql(
            f"SELECT * FROM {tbl} ORDER BY {date_col} DESC LIMIT 1",
            engine, parse_dates=[date_col]
        )
        row_cur = df_cur.iloc[0].to_dict() if not df_cur.empty else {}

        # prior-year row
        row_pri = {}
        if row_cur:
            dt_lastyear = row_cur[date_col] - timedelta(weeks=52)
            iso = dt_lastyear.isoformat()
            df_pri = pd.read_sql(
                text(f"SELECT * FROM {tbl} WHERE {date_col} = :iso LIMIT 1"),
                engine,
                params={"iso": iso},
                parse_dates=[date_col]
            )
            if not df_pri.empty:
                row_pri = df_pri.iloc[0].to_dict()
        result[key] = {"current": row_cur, "prior": row_pri}
    return result

def fetch_all_mailchimp_rows_for_latest_week() -> list[dict]:
    """Pulls the most-recent Mailchimp weekly_summary block :contentReference[oaicite:1]{index=1}"""
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
    """SELECT * WHERE date_col = :date_value :contentReference[oaicite:2]{index=2}"""
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
    """SELECT * WHERE date_col BETWEEN :start AND :end :contentReference[oaicite:3]{index=3}"""
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
    tbl, date_col = TABLES[table_key]
    with engine.connect() as conn:
        df = pd.read_sql(
            text(  # <-- text() is required to bind :start and :end
                f"SELECT COALESCE(SUM(total_attendance),0) AS total "
                f"FROM {tbl} "
                f"WHERE {date_col} BETWEEN :start AND :end"
            ),
            conn,
            params={"start": start_date, "end": end_date},
        )
    return int(df["total"].iloc[0])

def compare_adult_attendance(year1: int, year2: int, month: int) -> dict[str, int]:
    results = {}
    for y in (year1, year2):
        # build YYYY-MM-DD range for that month
        start = date(y, month, 1).isoformat()
        last_day = calendar.monthrange(y, month)[1]
        end   = date(y, month, last_day).isoformat()

        # NOTE: the key must match TABLES["AdultAttendance"]
        total = aggregate_total_attendance("AdultAttendance", start, end)
        results[f"{y}"] = total
    return results

