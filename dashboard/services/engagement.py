from typing import Optional, Tuple
import pandas as pd
from datetime import timedelta
from sqlalchemy import text
from data import engine

def _as_date(x):
    return x.date() if hasattr(x, "date") else x

def _scalar(sql: str, params: dict | None = None):
    with engine.connect() as c:
        row = c.execute(text(sql), params or {}).first()
        return row[0] if row else None

def _latest(table: str, date_col: str) -> Optional[pd.Timestamp]:
    return _scalar(f"SELECT MAX({date_col}) FROM {table};")

# ─────────────────────────────────────────────────────────────
# 1) “This Week” snapshot from snap_person_week + front_door_weekly
#    (uses engaged_tier and first-time counts)
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
        "first_time_checkins": 0, "first_time_givers": 0,
        "first_time_groups": 0, "first_time_serving": 0
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
# (snap_person_week has engaged_tier; cadence/lapse signals live in person_cadence & lapse_events.)

# ─────────────────────────────────────────────────────────────
# 2) Cadence buckets by signal (attend/give/group)
def get_cadence_summary(signals: Tuple[str, ...] = ("attend", "give", "group")) -> pd.DataFrame:
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
# (person_cadence has bucket/missed_cycles/etc.)

# ─────────────────────────────────────────────────────────────
# 3) Newly-lapsed people (most recent flagged week)
def get_lapsed_people(limit: int = 100,
                      signals: Tuple[str, ...] = ("attend", "give", "serve", "group")) -> pd.DataFrame:
    latest = _latest("lapse_events", "week_flagged")
    if not latest:
        return pd.DataFrame(columns=["person_id","name","email","signal","observed_none_since",
                                     "expected_by","missed_cycles","bucket"])

    sql = """
        SELECT
          le.person_id,
          COALESCE(pp.first_name,'') || ' ' || COALESCE(pp.last_name,'') AS name,
          COALESCE(pp.email, '') AS email,
          le.signal,
          le.observed_none_since,
          le.expected_by,
          le.missed_cycles,
          pc.bucket
        FROM lapse_events le
        JOIN pco_people pp ON pp.person_id = le.person_id
        LEFT JOIN person_cadence pc ON pc.person_id = le.person_id AND pc.signal = le.signal
        WHERE le.week_flagged = :wk
          AND le.signal = ANY(:sigs)
        ORDER BY le.missed_cycles DESC, le.expected_by NULLS LAST
        LIMIT :lim
    """
    return pd.read_sql(text(sql), engine, params={"wk": latest, "sigs": list(signals), "lim": limit})

# ─────────────────────────────────────────────────────────────
# 4) Backdoor
def get_back_door_summary() -> pd.DataFrame:
    """Return label/value pairs for KPI row from latest back_door_weekly + front_door_weekly."""
    with engine.connect() as c:
        wk = c.execute(text("SELECT MAX(week_end) FROM back_door_weekly;")).scalar()
        if not wk:
            return pd.DataFrame({"label": [], "value": []})

        bd = c.execute(text("""
            SELECT downshifts_total, downshift_3_to_2, downshift_2_to_1, downshift_1_to_0,
                   new_nla_count, reengaged_count, bdi
            FROM back_door_weekly WHERE week_end = :wk
        """), {"wk": wk}).first()

        fd = c.execute(text("""
            SELECT
              COALESCE(first_time_checkins,0) + COALESCE(first_time_givers,0)
              + COALESCE(first_time_groups,0) + COALESCE(first_time_serving,0) AS front_door_total
            FROM front_door_weekly WHERE week_end = :wk
        """), {"wk": wk}).scalar() or 0

    data = [
        ("Back Door Index",        bd.bdi if bd and bd.bdi is not None else 0),
        ("Down-shifts (total)",    bd.downshifts_total if bd else 0),
        ("New NLA (90d)",          bd.new_nla_count if bd else 0),
        ("Re-engaged",             bd.reengaged_count if bd else 0),
        ("Front Door (sum)",       fd),
        ("Net Movement",           (fd - (bd.downshifts_total + bd.new_nla_count - bd.reengaged_count)) if bd else fd),
    ]
    return pd.DataFrame(data, columns=["label","value"])


def get_new_nla_people(limit: int = 200) -> pd.DataFrame:
    """People who became NLA (90d) this week with tenure fields if present."""
    with engine.connect() as c:
        wk = c.execute(text("SELECT MAX(week_end) FROM no_longer_attends_events;")).scalar()
        if not wk:
            return pd.DataFrame(columns=["person_id","name","email","first_seen_any","last_any_date","campus_id"])
        sql = """
        SELECT n.person_id,
               COALESCE(p.first_name,'') || ' ' || COALESCE(p.last_name,'') AS name,
               COALESCE(p.email,'') AS email,
               n.first_seen_any, n.last_any_date, n.campus_id
        FROM no_longer_attends_events n
        JOIN pco_people p ON p.person_id = n.person_id
        WHERE n.week_end = :wk
        ORDER BY n.last_any_date ASC
        LIMIT :l
        """
        df = pd.read_sql(text(sql), con=engine, params={"wk": wk, "l": limit}, parse_dates=["first_seen_any","last_any_date"])
        return df

def get_downshifts_people(limit: int = 200) -> pd.DataFrame:
    with engine.connect() as c:
        wk_scalar = c.execute(text("SELECT MAX(week_end) FROM engagement_tier_transitions;")).scalar()
        if not wk_scalar:
            return pd.DataFrame(columns=["person_id","name","email","from_tier","to_tier","stopped","campus_id"])
        wk   = _as_date(wk_scalar)
        prev = wk - timedelta(days=7)

        df = pd.read_sql(
            text("""
                WITH prev AS (
                SELECT person_id,
                        gave_ontrack_bool      AS prev_give,
                        served_ontrack_bool    AS prev_serve,
                        in_group_ontrack_bool  AS prev_group
                FROM snap_person_week WHERE week_end = :prev
                ),
                curr AS (
                SELECT person_id,
                        gave_ontrack_bool      AS curr_give,
                        served_ontrack_bool    AS curr_serve,
                        in_group_ontrack_bool  AS curr_group
                FROM snap_person_week WHERE week_end = :wk
                ),
                cad AS (
                SELECT DISTINCT ON (pc.person_id)
                        pc.person_id, pc.bucket
                FROM person_cadence pc
                WHERE pc.signal = 'give'
                ORDER BY pc.person_id, pc.samples_n DESC
                ),
                last_gift AS (
                SELECT person_id, MAX(week_end)::date AS last_gift_week
                FROM f_giving_person_week
                WHERE week_end <= :wk AND gift_count > 0
                GROUP BY person_id
                ),
                stops AS (
                SELECT e.person_id,
                        (pv.prev_serve = TRUE AND co.curr_serve = FALSE) AS stop_serve,
                        (pv.prev_group = TRUE AND co.curr_group = FALSE) AS stop_group,
                        (
                        pv.prev_give = TRUE AND co.curr_give = FALSE
                        AND lg.last_gift_week IS NOT NULL
                        AND ((:wk - lg.last_gift_week) >= GREATEST(
                                60,
                                CASE cad.bucket
                                WHEN 'weekly'   THEN 14
                                WHEN 'biweekly' THEN 28
                                WHEN 'monthly'  THEN 60
                                WHEN '6weekly'  THEN 84
                                ELSE 60
                                END
                            ))
                        ) AS stop_give
                FROM engagement_tier_transitions e
                LEFT JOIN prev pv      ON pv.person_id   = e.person_id
                LEFT JOIN curr co      ON co.person_id   = e.person_id
                LEFT JOIN cad          ON cad.person_id  = e.person_id
                LEFT JOIN last_gift lg ON lg.person_id   = e.person_id
                WHERE e.week_end = :wk
                )
                SELECT e.person_id,
                    COALESCE(p.first_name,'') || ' ' || COALESCE(p.last_name,'') AS name,
                    COALESCE(p.email,'') AS email,
                    e.from_tier, e.to_tier, e.campus_id,
                    ARRAY_REMOVE(ARRAY[
                        CASE WHEN s.stop_give  THEN 'giving'  END,
                        CASE WHEN s.stop_serve THEN 'serving' END,
                        CASE WHEN s.stop_group THEN 'groups'  END
                    ], NULL) AS stopped_signals
                FROM engagement_tier_transitions e
                JOIN pco_people p ON p.person_id = e.person_id
                JOIN stops s      ON s.person_id = e.person_id
                WHERE e.week_end = :wk
                AND (s.stop_serve OR s.stop_group OR s.stop_give)
                ORDER BY e.from_tier DESC, e.to_tier, p.last_name, p.first_name
                LIMIT :l
            """),
            con=engine,
            params={"wk": wk, "prev": prev, "l": int(limit)},
        )

    if "stopped_signals" in df.columns:
        df["stopped"] = df["stopped_signals"].apply(lambda a: ", ".join(a) if isinstance(a, (list, tuple)) and a else "")
        df = df.drop(columns=["stopped_signals"])
        df["stopped"] = df["stopped"].astype("string")
    cols = ["person_id","name","email","from_tier","to_tier","stopped","campus_id"]
    return df.reindex(columns=[c for c in cols if c in df.columns])


def get_downshift_flow_table() -> pd.DataFrame:
    with engine.connect() as c:
        wk_scalar = c.execute(text("SELECT MAX(week_end) FROM engagement_tier_transitions;")).scalar()
        if not wk_scalar:
            return pd.DataFrame(index=[3,2,1], columns=[2,1,0]).fillna(0).astype(int)
        wk   = _as_date(wk_scalar)
        prev = wk - timedelta(days=7)

        df = pd.read_sql(
            text("""
                WITH prev AS (
                  SELECT person_id,
                         gave_ontrack_bool AS prev_give,
                         served_ontrack_bool AS prev_serve,
                         in_group_ontrack_bool AS prev_group
                  FROM snap_person_week WHERE week_end = :prev
                ),
                curr AS (
                  SELECT person_id,
                         gave_ontrack_bool AS curr_give,
                         served_ontrack_bool AS curr_serve,
                         in_group_ontrack_bool AS curr_group
                  FROM snap_person_week WHERE week_end = :wk
                ),
                cad AS (
                  SELECT DISTINCT ON (pc.person_id) pc.person_id, pc.bucket
                  FROM person_cadence pc
                  WHERE pc.signal = 'give'
                  ORDER BY pc.person_id, pc.samples_n DESC
                ),
                last_gift AS (
                  SELECT person_id, MAX(week_end)::date AS last_gift_week
                  FROM f_giving_person_week
                  WHERE week_end <= :wk AND gift_count > 0
                  GROUP BY person_id
                ),
                stops AS (
                  SELECT e.person_id,
                         (pv.prev_serve = TRUE AND co.curr_serve = FALSE) AS stop_serve,
                         (pv.prev_group = TRUE AND co.curr_group = FALSE) AS stop_group,
                         (
                           pv.prev_give = TRUE AND co.curr_give = FALSE
                           AND lg.last_gift_week IS NOT NULL
                           AND ((:wk - lg.last_gift_week) >= GREATEST(
                                 60,
                                 CASE cad.bucket
                                   WHEN 'weekly'   THEN 14
                                   WHEN 'biweekly' THEN 28
                                   WHEN 'monthly'  THEN 60
                                   WHEN '6weekly'  THEN 84
                                   ELSE 60
                                 END
                               ))
                         ) AS stop_give
                  FROM engagement_tier_transitions e
                  LEFT JOIN prev pv     ON pv.person_id   = e.person_id
                  LEFT JOIN curr co     ON co.person_id   = e.person_id
                  LEFT JOIN cad         ON cad.person_id  = e.person_id
                  LEFT JOIN last_gift lg ON lg.person_id  = e.person_id
                  WHERE e.week_end = :wk
                )
                SELECT e.from_tier, e.to_tier, COUNT(*)::int AS n
                FROM engagement_tier_transitions e
                JOIN stops s ON s.person_id = e.person_id
                WHERE e.week_end = :wk
                  AND (s.stop_serve OR s.stop_group OR s.stop_give)
                GROUP BY 1,2
            """),
            con=engine,
            params={"wk": wk, "prev": prev},
        )

    piv = (df.pivot_table(index="from_tier", columns="to_tier", values="n",
                          aggfunc="sum", fill_value=0)
             .reindex(index=[3,2,1], fill_value=0)
             .reindex(columns=[2,1,0], fill_value=0)
             .astype(int))
    piv.index.name = "From ↓ / To →"
    return piv


def get_downshifts_from_pie() -> pd.DataFrame:
    with engine.connect() as c:
        wk_scalar = c.execute(text("SELECT MAX(week_end) FROM engagement_tier_transitions;")).scalar()
        if not wk_scalar:
            return pd.DataFrame({"label": [], "value": []})
        wk   = _as_date(wk_scalar)
        prev = wk - timedelta(days=7)

        df = pd.read_sql(
            text("""
                WITH prev AS (
                  SELECT person_id,
                         gave_ontrack_bool AS prev_give,
                         served_ontrack_bool AS prev_serve,
                         in_group_ontrack_bool AS prev_group
                  FROM snap_person_week WHERE week_end = :prev
                ),
                curr AS (
                  SELECT person_id,
                         gave_ontrack_bool AS curr_give,
                         served_ontrack_bool AS curr_serve,
                         in_group_ontrack_bool AS curr_group
                  FROM snap_person_week WHERE week_end = :wk
                ),
                cad AS (
                  SELECT DISTINCT ON (pc.person_id) pc.person_id, pc.bucket
                  FROM person_cadence pc
                  WHERE pc.signal = 'give'
                  ORDER BY pc.person_id, pc.samples_n DESC
                ),
                last_gift AS (
                  SELECT person_id, MAX(week_end)::date AS last_gift_week
                  FROM f_giving_person_week
                  WHERE week_end <= :wk AND gift_count > 0
                  GROUP BY person_id
                ),
                stops AS (
                  SELECT e.person_id,
                         (pv.prev_serve = TRUE AND co.curr_serve = FALSE) AS stop_serve,
                         (pv.prev_group = TRUE AND co.curr_group = FALSE) AS stop_group,
                         (
                           pv.prev_give = TRUE AND co.curr_give = FALSE
                           AND lg.last_gift_week IS NOT NULL
                           AND ((:wk - lg.last_gift_week) >= GREATEST(
                                 60,
                                 CASE cad.bucket
                                   WHEN 'weekly'   THEN 14
                                   WHEN 'biweekly' THEN 28
                                   WHEN 'monthly'  THEN 60
                                   WHEN '6weekly'  THEN 84
                                   ELSE 60
                                 END
                               ))
                         ) AS stop_give
                  FROM engagement_tier_transitions e
                  LEFT JOIN prev pv     ON pv.person_id   = e.person_id
                  LEFT JOIN curr co     ON co.person_id   = e.person_id
                  LEFT JOIN cad         ON cad.person_id  = e.person_id
                  LEFT JOIN last_gift lg ON lg.person_id  = e.person_id
                  WHERE e.week_end = :wk
                )
                SELECT e.from_tier, COUNT(*)::int AS n
                FROM engagement_tier_transitions e
                JOIN stops s ON s.person_id = e.person_id
                WHERE e.week_end = :wk
                  AND (s.stop_serve OR s.stop_group OR s.stop_give)
                GROUP BY 1
                ORDER BY e.from_tier DESC
            """),
            con=engine,
            params={"wk": wk, "prev": prev},
        )

    return pd.DataFrame({
        "label": [f"From {int(x)}" for x in df["from_tier"]],
        "value": df["n"].astype(int),
    })

