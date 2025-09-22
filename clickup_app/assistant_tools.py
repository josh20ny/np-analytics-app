# clickup_app/assistant_tools.py

import pandas as pd
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
from datetime import timedelta, date
from sqlalchemy.sql import text
import calendar
from app.config import settings

from sqlalchemy.sql import text

# Canonical labels the new pipeline uses
_MINISTRY_WHITELIST = {"Waumba Land", "UpStreet", "Transit", "InsideOut"}
_SERVICE_WHITELIST  = {"9:30 AM", "11:00 AM", "4:30 PM"}

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

def _fetch_location_rows_for_day(conn, svc_date: str, ministry: str | None, service: str | None):
    """Read from attendance_by_location_daily (+ names from pco_locations)."""
    params = {"d": svc_date}
    filters = []
    if ministry:
        filters.append("a.ministry_key = :m")
        params["m"] = ministry
    if service:
        filters.append("a.service_bucket = :s")
        params["s"] = service
    where_extra = (" AND " + " AND ".join(filters)) if filters else ""
    sql = text(f"""
        SELECT
          a.date,
          a.ministry_key,
          a.service_bucket,
          a.location_id,
          COALESCE(l.name, CONCAT('Location ', a.location_id::text)) AS location_name,
          a.total_attendance,
          a.total_new
        FROM attendance_by_location_daily a
        LEFT JOIN pco_locations l ON l.location_id = a.location_id
        WHERE a.date = :d::date {where_extra}
        ORDER BY a.ministry_key, a.service_bucket, a.total_attendance DESC, a.location_id
    """)
    df = pd.read_sql(sql, conn, params=params)
    return df

def _fetch_person_facts_for_day(conn, svc_date: str, ministry: str | None):
    """Optional person-level facts for that day from f_checkins_person."""
    params = {"d": svc_date}
    filters = []
    if ministry:
        filters.append("p.ministry = :m")
        params["m"] = ministry
    where_extra = (" AND " + " AND ".join(filters)) if filters else ""
    sql = text(f"""
        SELECT
          p.person_id,
          p.ministry,
          CASE p.service_time
            WHEN '930'  THEN '9:30 AM'
            WHEN '1100' THEN '11:00 AM'
            WHEN '1630' THEN '4:30 PM'
            ELSE p.service_time
          END AS service_time,
          p.event_id,
          p.campus_id,
          p.created_at_utc
        FROM f_checkins_person p
        WHERE p.svc_date = :d::date {where_extra}
        ORDER BY p.ministry, service_time, p.person_id
    """)
    df = pd.read_sql(sql, conn, params=params, parse_dates=["created_at_utc"])
    return df

def get_checkins_attendance(
    date_value: str,
    view: str = "nested",                # "nested" | "rows"
    ministry: str | None = None,         # "Waumba Land" | "UpStreet" | "Transit" | "InsideOut"
    service: str | None = None,          # "9:30 AM" | "11:00 AM" | "4:30 PM"
    include_persons: bool = False,
) -> dict:
    """
    General read-only accessor that mirrors the new endpoints:
      - view="nested": ministries → services → locations (like /day/{date})
      - view="rows":   flat list (like /day/{date}/rows)
    Optional filters: ministry, service. Optional: include_persons on nested view.
    """
    # Light validation (don’t fail hard; just ignore bad filters)
    if ministry and ministry not in _MINISTRY_WHITELIST:
        ministry = None
    if service and service not in _SERVICE_WHITELIST:
        service = None

    with engine.connect() as conn:
        df = _fetch_location_rows_for_day(conn, date_value, ministry, service)

        if view == "rows":
            rows = df.to_dict(orient="records")
            # massage keys to match the API rows endpoint
            for r in rows:
                r["date"]        = str(pd.to_datetime(r["date"]).date())
                r["ministry"]    = r.pop("ministry_key")
                r["service"]     = r.pop("service_bucket")
                r["location"]    = r.pop("location_name")
                r["attendance"]  = int(r.pop("total_attendance"))
                r["new"]         = int(r.pop("total_new"))
            return {"ok": True, "date": date_value, "rows": rows, "count": len(rows)}

        # Default: nested view
        ministries: dict[str, dict] = {}
        for _, r in df.iterrows():
            m = r["ministry_key"]; s = r["service_bucket"]
            ministries.setdefault(m, {"total": 0, "services": {}})
            ministries[m]["services"].setdefault(s, {"total": 0, "locations": []})
            ministries[m]["services"][s]["locations"].append({
                "id": int(r["location_id"]) if pd.notnull(r["location_id"]) else None,
                "name": r["location_name"],
                "attendance": int(r["total_attendance"]),
                "new": int(r["total_new"]),
            })
            ministries[m]["total"] += int(r["total_attendance"])
            ministries[m]["services"][s]["total"] += int(r["total_attendance"])

        result = {"ok": True, "date": date_value, "ministries": ministries}

        if include_persons:
            pf = _fetch_person_facts_for_day(conn, date_value, ministry)
            persons = []
            for _, p in pf.iterrows():
                persons.append({
                    "person_id": p["person_id"],
                    "ministry": p["ministry"],
                    "service_time": p["service_time"],
                    "event_id": p["event_id"],
                    "campus_id": p["campus_id"],
                    "created_at_utc": (p["created_at_utc"].to_pydatetime().isoformat()
                                       if pd.notnull(p["created_at_utc"]) else None)
                })
            result["persons"] = persons

        return result
