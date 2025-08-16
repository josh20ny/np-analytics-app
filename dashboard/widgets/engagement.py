# dashboard/services/engagement.py
from typing import Optional
import pandas as pd
from sqlalchemy import text
from data import engine

def _scalar(sql: str, params: dict | None = None):
    with engine.connect() as c:
        row = c.execute(text(sql), params or {}).first()
        return row[0] if row else None

def _latest(table: str, date_col: str) -> Optional[pd.Timestamp]:
    return _scalar(f"SELECT MAX({date_col}) FROM {table};")

# ──────────────────────────────────────────────────────────────────────────────
# 1) “This Week” panel
#    - Tier counts from snap_person_week (latest week_end)
#    - Optional Front Door firsts from front_door_weekly for the same week
def get_recent_engagement() -> pd.DataFrame:
    latest = _latest("snap_person_week", "week_end")
    if not latest:
        return pd.DataFrame(columns=["label", "value"])

    tiers = pd.read_sql(
        text("""
            SELECT engaged_tier::int AS tier, COUNT(*)::int AS n
            FROM snap_person_week
            WHERE week_end = :d
            GROUP BY 1
        """),
        engine,
        params={"d": latest},
    ).set_index("tier")["n"].to_dict()

    # Front Door (first-time wins) for context, if present
    fd = pd.read_sql(
        text("""
            SELECT first_time_checkins, first_time_givers, first_time_groups, first_time_serving
            FROM front_door_weekly
            WHERE week_end = :d
            LIMIT 1
        """),
        engine,
        params={"d": latest},
    )
    firsts = fd.iloc[0].to_dict() if not fd.empty else {
        "first_time_checkins": 0, "first_time_givers": 0, "first_time_groups": 0, "first_time_serving": 0
    }

    rows = [
        {"label": "Engaged Tier 3", "value": tiers.get(3, 0)},
        {"label": "Engaged Tier 2", "value": tiers.get(2, 0)},
        {"label": "Engaged Tier 1", "value": tiers.get(1, 0)},
        {"label": "Engaged Tier 0", "value": tiers.get(0, 0)},
        {"label": "First-time Check-ins", "value": firsts["first_time_checkins"]},
        {"label": "First-time Givers",   "value": firsts["first_time_givers"]},
        {"label": "First-time Groups",   "value": firsts["first_time_groups"]},
    ]
    return pd.DataFrame(rows)

# ──────────────────────────────────────────────────────────────────────────────
# 2) Cadence buckets (current)
#    Group the current cadence inference by (signal, bucket).
def get_cadence_summary(signals: tuple[str, ...] = ("attend", "give", "group")) -> pd.DataFrame:
    df = pd.read_sql(
        text("""
            SELECT signal, bucket, COUNT(*)::int AS count
            FROM person_cadence
            WHERE bucket IS NOT NULL
              AND signal = ANY(:sigs)
            GROUP BY 1,2
            ORDER BY 1,2
        """),
        engine,
        params={"sigs": list(signals)},
    )
    return df

# ──────────────────────────────────────────────────────────────────────────────
# 3) Lapsed list (latest flag week)
#    Pull the people flagged most recently from lapse_events and join names/emails.
def get_lapsed_people(limit: int = 100,
                      signals: tuple[str, ...] = ("attend", "give", "serve", "group")) -> pd.DataFrame:
    latest = _latest("lapse_events", "week_flagged")
    if not latest:
        return pd.DataFrame(columns=["person_id","name","signal","observed_none_since","expected_by","missed_cycles","weeks_since"])

    sql = """
        SELECT
          le.person_id,
          COALESCE(pp.first_name,'') || ' ' || COALESCE(pp.last_name,'') AS name,
          COALESCE(pp.email, '') AS email,
          le.signal,
          le.observed_none_since,
          le.expected_by,
          le.missed_cycles,
          ((le.week_flagged - le.observed_none_since) / 7)::int AS weeks_since
        FROM lapse_events le
        LEFT JOIN pco_people pp ON pp.person_id = le.person_id
        WHERE le.week_flagged = :wk
          AND le.signal = ANY(:sigs)
        ORDER BY le.missed_cycles DESC NULLS LAST, le.observed_none_since ASC
        LIMIT :lim
    """
    return pd.read_sql(text(sql), engine, params={"wk": latest, "sigs": list(signals), "lim": limit})

