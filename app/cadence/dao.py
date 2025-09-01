# app/cadence/dao.py
from __future__ import annotations
from collections import defaultdict
from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional, Tuple
from sqlalchemy.orm import Session

from app.db import get_conn
from app.cadence.constants import DEFAULT_ROLLING_DAYS

# ──────────────────────────────────────────────────────────────────────────────
# UPSERTS (person_cadence, snap_person_week)
# ──────────────────────────────────────────────────────────────────────────────

def upsert_person_cadence(rows: List[Tuple]) -> int:
    """
    rows: (person_id, signal, median_interval_days, iqr_days, expected_next_date,
           last_seen_date, current_streak, missed_cycles, bucket, samples_n, calc_method, campus_id)
    """
    if not rows:
        return 0
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.executemany(
            """
            INSERT INTO person_cadence
              (person_id, signal, median_interval_days, iqr_days, expected_next_date,
               last_seen_date, current_streak, missed_cycles, bucket, samples_n, calc_method, campus_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (person_id, signal) DO UPDATE SET
              median_interval_days = EXCLUDED.median_interval_days,
              iqr_days             = EXCLUDED.iqr_days,
              expected_next_date   = EXCLUDED.expected_next_date,
              last_seen_date       = EXCLUDED.last_seen_date,
              current_streak       = EXCLUDED.current_streak,
              missed_cycles        = EXCLUDED.missed_cycles,
              bucket               = EXCLUDED.bucket,
              samples_n            = EXCLUDED.samples_n,
              calc_method          = EXCLUDED.calc_method,
              campus_id            = COALESCE(person_cadence.campus_id, EXCLUDED.campus_id);
            """,
            rows
        )
        n = cur.rowcount
        conn.commit()
        return n
    finally:
        cur.close(); conn.close()


def upsert_snap_person_week(rows: List[Tuple]) -> int:
    """
    rows: (person_id, week_start, week_end, attended_bool, gave_ontrack_bool, served_ontrack_bool,
           in_group_ontrack_bool, engaged_tier, checkins_count, gifts_count, serving_occurrences, campus_id)
    """
    if not rows:
        return 0
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.executemany(
            """
            INSERT INTO snap_person_week
              (person_id, week_start, week_end, attended_bool, gave_ontrack_bool, served_ontrack_bool,
               in_group_ontrack_bool, engaged_tier, checkins_count, gifts_count, serving_occurrences, campus_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (person_id, week_end) DO UPDATE SET
              attended_bool         = EXCLUDED.attended_bool,
              gave_ontrack_bool     = EXCLUDED.gave_ontrack_bool,
              served_ontrack_bool   = EXCLUDED.served_ontrack_bool,
              in_group_ontrack_bool = EXCLUDED.in_group_ontrack_bool,
              engaged_tier          = EXCLUDED.engaged_tier,
              checkins_count        = EXCLUDED.checkins_count,
              gifts_count           = EXCLUDED.gifts_count,
              serving_occurrences   = EXCLUDED.serving_occurrences,
              campus_id             = COALESCE(snap_person_week.campus_id, EXCLUDED.campus_id);
            """,
            rows
        )
        n = cur.rowcount
        conn.commit()
        return n
    finally:
        cur.close(); conn.close()

# ──────────────────────────────────────────────────────────────────────────────
# SOURCE FETCHERS used by service layer builds
# ──────────────────────────────────────────────────────────────────────────────

def fetch_giving_events(
    db: Session,
    since: Optional[date],
    *,
    as_of: date,
    rolling_days: int = DEFAULT_ROLLING_DAYS,
) -> Dict[str, List[date]]:
    """
    Return person_id -> [week_end dates] with at least one gift, within rolling window.
    """
    if as_of is None:
        raise ValueError("as_of is required")

    window_start = as_of - timedelta(days=rolling_days - 1)
    effective_start = max(filter(None, [since, window_start]))

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT person_id, week_end
            FROM f_giving_person_week
            WHERE week_end >= %(start)s
              AND week_end <= %(as_of)s
              AND gift_count > 0
            ORDER BY person_id, week_end;
            """,
            {"start": effective_start, "as_of": as_of}
        )
        out: Dict[str, List[date]] = defaultdict(list)
        for pid, wk_end in cur.fetchall():
            out[str(pid)].append(wk_end)
        return out
    finally:
        cur.close(); conn.close()


def fetch_adult_attendance_events(
    since: Optional[date],
    *,
    as_of: date,
    rolling_days: int = DEFAULT_ROLLING_DAYS,
) -> Dict[str, List[date]]:
    """
    Adult attendance proxied by household kid check-ins.
    Returns person_id -> [svc_date], limited to a rolling window. Adults are >=18.
    """
    window_start = as_of - timedelta(days=rolling_days)
    effective_start = max(filter(None, [since, window_start]))

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH adults AS (
                SELECT person_id, household_id
                FROM pco_people
                WHERE birthdate IS NOT NULL
                  AND birthdate <= CURRENT_DATE - INTERVAL '18 years'
            )
            SELECT a.person_id, h.svc_date::date
            FROM adults a
            JOIN household_attendance_vw h
              ON h.household_id = a.household_id
            WHERE h.svc_date >= %s
            ORDER BY a.person_id, h.svc_date;
            """,
            (effective_start,)
        )
        out: Dict[str, List[date]] = defaultdict(list)
        for pid, svc_date in cur.fetchall():
            out[str(pid)].append(svc_date)
        return out
    finally:
        cur.close(); conn.close()


def fetch_group_active_as_of(as_of: date) -> Dict[str, bool]:
    """ person_id -> active in Groups (as of date) """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT m.person_id, TRUE AS active
            FROM f_groups_memberships m
            JOIN pco_groups g ON g.group_id = m.group_id
            WHERE COALESCE(g.group_type,'') ILIKE 'Groups'
              AND m.status = 'active'
              AND (m.first_joined_at IS NULL OR m.first_joined_at::date <= %s)
              AND (m.archived_at IS NULL OR m.archived_at::date > %s)
            GROUP BY m.person_id;
            """,
            (as_of, as_of)
        )
        return {str(pid): True for (pid, _active) in cur.fetchall()}
    finally:
        cur.close(); conn.close()


def fetch_serving_active_as_of(as_of: date) -> Dict[str, bool]:
    """ person_id -> active in serving team (as of date) """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT DISTINCT m.person_id
            FROM f_groups_memberships m
            JOIN pco_groups g ON g.group_id = m.group_id
            WHERE g.is_serving_team = TRUE
              AND m.status = 'active'
              AND (m.first_joined_at IS NULL OR m.first_joined_at::date <= %s)
              AND (m.archived_at IS NULL OR m.archived_at::date > %s);
            """,
            (as_of, as_of),
        )
        return {str(pid): True for (pid,) in cur.fetchall()}
    finally:
        cur.close(); conn.close()


def attended_adults_for_week(week_start: date, week_end: date) -> Dict[str, int]:
    """ person_id -> count of household attendance rows for the week (adult proxy) """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH adults AS (
                SELECT person_id, household_id
                FROM pco_people
                WHERE birthdate IS NOT NULL
                  AND birthdate <= CURRENT_DATE - INTERVAL '18 years'
            )
            SELECT a.person_id, COUNT(*)::int AS c
            FROM adults a
            JOIN household_attendance_vw h
              ON h.household_id = a.household_id
            WHERE h.svc_date BETWEEN %s AND %s
            GROUP BY a.person_id;
            """,
            (week_start, week_end)
        )
        return {str(pid): int(c) for (pid, c) in cur.fetchall()}
    finally:
        cur.close(); conn.close()


def ontrack_give_for_week(week_start: date, week_end: date) -> Dict[str, bool]:
    """
    gave_ontrack_bool:
    - True if gift this week, OR not yet due (expected_next_date > week_end), OR insufficient samples
    - False only when due and missed
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        # gifts this week
        cur.execute(
            """
            SELECT person_id, gift_count
            FROM f_giving_person_week
            WHERE week_end = %s AND gift_count > 0;
            """,
            (week_end,)
        )
        gave_now = {str(pid): True for (pid, _gc) in cur.fetchall()}

        # person cadence expectations
        cur.execute(
            """
            SELECT person_id, expected_next_date, samples_n
            FROM person_cadence
            WHERE signal = 'give';
            """
        )
        out: Dict[str, bool] = {}
        for pid, expected, samples_n in cur.fetchall():
            pid = str(pid)
            if gave_now.get(pid):
                out[pid] = True
            else:
                if expected is None or (samples_n or 0) < 2:
                    out[pid] = True
                else:
                    out[pid] = expected > week_end
        return out
    finally:
        cur.close(); conn.close()

# ──────────────────────────────────────────────────────────────────────────────
# Aggregations used by routes/service
# ──────────────────────────────────────────────────────────────────────────────

def bucket_counts(
    signal: str,
    *,
    week_end: date,
    exclude_lapsed: bool = True,
) -> Dict[str, int]:
    """
    Return counts for cadence buckets among people present in the weekly snapshot:
    keys: {weekly, biweekly, monthly, 6weekly, irregular, one_off}
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        if not exclude_lapsed:
            sql = """
            SELECT COALESCE(pc.bucket,'irregular') AS bucket, COUNT(*)::int AS c
            FROM person_cadence pc
            JOIN snap_person_week s
              ON s.person_id = pc.person_id
             AND s.week_end   = %s
            WHERE pc.signal = %s
            GROUP BY pc.bucket;
            """
            params = [week_end, signal]
        else:
            sql = """
            SELECT COALESCE(pc.bucket,'irregular') AS bucket, COUNT(*)::int AS c
            FROM person_cadence pc
            JOIN snap_person_week s
              ON s.person_id = pc.person_id
             AND s.week_end   = %s
            WHERE pc.signal = %s
              AND NOT (pc.bucket NOT IN ('irregular','one_off') AND pc.missed_cycles >= 3)
            GROUP BY pc.bucket;
            """
            params = [week_end, signal]

        cur.execute(sql, params)
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    tpl = {"weekly": 0, "biweekly": 0, "monthly": 0, "6weekly": 0, "irregular": 0, "one_off": 0}
    for b, c in rows:
        if b in tpl:
            tpl[b] = int(c)
    return tpl


def asof_counts(week_end: date) -> Dict[str, int]:
    """Active-in-Groups & Serving as of a date (for weekly report meta)"""
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH grp AS (
              SELECT COUNT(DISTINCT m.person_id) AS n
              FROM f_groups_memberships m
              JOIN pco_groups g ON g.group_id = m.group_id
              WHERE COALESCE(g.group_type,'') = 'Groups'
                AND (m.first_joined_at IS NULL OR m.first_joined_at::date <= %s)
                AND (m.archived_at   IS NULL OR m.archived_at::date   >  %s)
            ),
            srv AS (
              SELECT COUNT(DISTINCT m.person_id) AS n
              FROM f_groups_memberships m
              JOIN pco_groups g ON g.group_id = m.group_id
              WHERE g.is_serving_team = TRUE
                AND (m.first_joined_at IS NULL OR m.first_joined_at::date <= %s)
                AND (m.archived_at   IS NULL OR m.archived_at::date   >  %s)
            )
            SELECT (SELECT n FROM grp) AS groups_active,
                   (SELECT n FROM srv) AS serving_active;
            """,
            (week_end, week_end, week_end, week_end),
        )
        ga, sa = cur.fetchone()
        return {"in_groups_active": int(ga or 0), "serving_active": int(sa or 0)}
    finally:
        cur.close(); conn.close()

# ── Cadences list (browse) ────────────────────────────────────────────────────
def list_cadences(
    *,
    signal: str,
    bucket: Optional[str],
    exclude_lapsed: bool,
    q: Optional[str],
    order_by: str,
    limit: int,
    offset: int,
) -> Dict:
    valid_orders = {
        "expected_next_date_asc": "c.expected_next_date NULLS LAST, c.last_seen_date DESC",
        "last_seen_desc": "c.last_seen_date DESC",
        "missed_cycles_desc": "c.missed_cycles DESC, c.last_seen_date DESC",
        "samples_desc": "c.samples_n DESC, c.last_seen_date DESC",
    }
    order_sql = valid_orders[order_by]

    where = ["c.signal = %s"]
    params: List = [signal]

    if bucket:
        where.append("c.bucket = %s")
        params.append(bucket)

    if exclude_lapsed:
        where.append("NOT (c.bucket <> 'irregular' AND COALESCE(c.missed_cycles,0) >= 3)")

    if q:
        where.append("(p.first_name ILIKE %s OR p.last_name ILIKE %s OR COALESCE(p.email,'') ILIKE %s)")
        params += [f"%{q}%", f"%{q}%", f"%{q}%"]

    where_sql = " AND ".join(where)
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT COUNT(*) FROM person_cadence c JOIN pco_people p USING (person_id) WHERE {where_sql};",
            params
        )
        total = int(cur.fetchone()[0])

        cur.execute(
            f"""
            SELECT
              c.person_id,
              p.first_name, p.last_name, p.email,
              c.bucket, c.samples_n, c.median_interval_days, c.iqr_days,
              c.last_seen_date, c.expected_next_date, c.missed_cycles
            FROM person_cadence c
            JOIN pco_people p USING (person_id)
            WHERE {where_sql}
            ORDER BY {order_sql}
            LIMIT %s OFFSET %s;
            """,
            params + [limit, offset]
        )
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    items = [{
        "person_id": r[0],
        "name": f"{r[1] or ''} {r[2] or ''}".strip(),
        "email": r[3],
        "bucket": r[4],
        "samples_n": r[5],
        "median_interval_days": r[6],
        "iqr_days": r[7],
        "last_seen_date": r[8].isoformat() if r[8] else None,
        "expected_next_date": r[9].isoformat() if r[9] else None,
        "missed_cycles": r[10],
        "lapsed": (r[4] != "irregular" and (r[10] or 0) >= 3),
    } for r in rows]

    return {"total": total, "rows": items}

# ── CSV sources ───────────────────────────────────────────────────────────────
def downshifts_rows(week_end: date) -> List[Tuple]:
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
          SELECT e.person_id, p.first_name, p.last_name, p.email, e.from_tier, e.to_tier, e.campus_id
          FROM engagement_tier_transitions e
          JOIN pco_people p
                 ON p.person_id::text = e.person_id::text
          WHERE e.week_end = %s AND e.from_tier > e.to_tier
          ORDER BY e.from_tier DESC, e.to_tier ASC, p.last_name, p.first_name;
        """, (week_end,))
        return cur.fetchall()
    finally:
        cur.close(); conn.close()

def nla_rows(week_end: date) -> List[Tuple]:
    """
    NLA = 'no-longer-attends' export (flat, person-centric) built from the
    current lapsed payload tables you already populate during weekly report.
    We keep the columns compatible with your existing CSV.
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
          SELECT
            p.person_id::text,
            COALESCE(p.first_name,'') || ' ' || COALESCE(p.last_name,'') AS name,
            p.email,
            nl.first_seen_any,
            nl.last_attend,
            nl.last_give,
            nl.last_serve,
            nl.last_group,
            nl.last_any
          FROM no_longer_attends_flat nl
          JOIN pco_people p
                 ON p.person_id::text = nl.person_id::text
          WHERE nl.week_end = %s
          ORDER BY nl.last_any NULLS LAST, p.last_name, p.first_name;
        """, (week_end,))
        return cur.fetchall()
    finally:
        cur.close(); conn.close()

# ── Weekly report helpers ─────────────────────────────────────────────────────

def engaged_tier_counts(week_end: date) -> Dict[int, int]:
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT engaged_tier, COUNT(*)::int
            FROM snap_person_week
            WHERE week_end = %s
            GROUP BY engaged_tier
        """, (week_end,))
        return {int(t or 0): int(c or 0) for (t, c) in cur.fetchall()}
    finally:
        cur.close(); conn.close()


def front_door_counts(week_end: date) -> Dict[str, int]:
    """
    Conservative, snapshot-driven definitions:
      • first_time_checkins  = adults who attended this week, never attended in any prior snapshot
      • first_time_givers    = people with gifts_count>0 this week, with no gifts in any prior snapshot
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        # First-time attendees (adult proxy via attended_bool). We rely on your snapshot being adult-filtered.
        cur.execute("""
            WITH curr AS (
              SELECT DISTINCT person_id
              FROM snap_person_week
              WHERE week_end = %s AND attended_bool = TRUE
            ),
            prev AS (
              SELECT DISTINCT person_id
              FROM snap_person_week
              WHERE week_end < %s AND attended_bool = TRUE
            )
            SELECT COUNT(*)::int FROM curr c
            LEFT JOIN prev p USING (person_id)
            WHERE p.person_id IS NULL;
        """, (week_end, week_end))
        ft_attend = int(cur.fetchone()[0] or 0)

        # First-time givers (any gifts recorded in snapshot this week, none prior)
        cur.execute("""
            WITH curr AS (
              SELECT DISTINCT person_id
              FROM snap_person_week
              WHERE week_end = %s AND gifts_count > 0
            ),
            prev AS (
              SELECT DISTINCT person_id
              FROM snap_person_week
              WHERE week_end < %s AND gifts_count > 0
            )
            SELECT COUNT(*)::int FROM curr c
            LEFT JOIN prev p USING (person_id)
            WHERE p.person_id IS NULL;
        """, (week_end, week_end))
        ft_givers = int(cur.fetchone()[0] or 0)

        return {"first_time_checkins": ft_attend, "first_time_givers": ft_givers}
    finally:
        cur.close(); conn.close()


def downshifts_count(week_end: date) -> int:
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT COUNT(*)::int
            FROM engagement_tier_transitions
            WHERE week_end = %s AND from_tier > to_tier
        """, (week_end,))
        return int(cur.fetchone()[0] or 0)
    finally:
        cur.close(); conn.close()


def adult_attendance_avg_4w(week_end: date) -> int:
    """
    Average adult attendance over last 4 Sundays, using snapshot attended_bool.
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            WITH weeks AS (
              SELECT DISTINCT week_end
              FROM snap_person_week
              WHERE week_end <= %s
              ORDER BY week_end DESC
              LIMIT 4
            )
            SELECT COALESCE(AVG(attended_cnt),0)::int
            FROM (
              SELECT w.week_end, SUM(CASE WHEN s.attended_bool THEN 1 ELSE 0 END)::int AS attended_cnt
              FROM weeks w
              JOIN snap_person_week s ON s.week_end = w.week_end
              GROUP BY w.week_end
            ) t;
        """, (week_end,))
        return int(cur.fetchone()[0] or 0)
    finally:
        cur.close(); conn.close()

# ── Lapse detection + NLA persistence ─────────────────────────────────────────
def _households_with_kids_u14(as_of: date) -> set[int]:
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
          SELECT DISTINCT household_id
          FROM pco_people
          WHERE household_id IS NOT NULL
            AND birthdate IS NOT NULL
            AND birthdate > %s - INTERVAL '14 years';
        """, (as_of,))
        return {int(hh) for (hh,) in cur.fetchall()}
    finally:
        cur.close(); conn.close()

def detect_and_upsert_lapses_for_week(week_end: date) -> dict:
    """
    Insert ONLY newly-lapsed people for this week into lapses_weekly.
    Newly-lapsed = meets lapse criteria this week AND has no prior row in lapses_weekly.
    Criteria (same as before):
      - bucket not irregular/one_off
      - missed_cycles >= 3
      - engaged_tier == 0 in the target week
      - household has kids < 14 (gate)
    Returns: {"inserted": n, "by_signal": {...}}
    """
    conn = get_conn(); cur = conn.cursor()

    # Households with kids < 14 (gate)
    kids_hh = _households_with_kids_u14(week_end)

    # Engaged 0 this week
    cur.execute("""
      SELECT person_id::text
      FROM snap_person_week
      WHERE week_end = %s AND engaged_tier = 0
    """, (week_end,))
    eng0 = {str(pid) for (pid,) in cur.fetchall()}

    # Already lapsed at any prior week (used to filter to "newly" this week)
    cur.execute("""
      SELECT person_id::text, signal
      FROM lapses_weekly
      WHERE week_end < %s
    """, (week_end,))
    already = {(str(pid), sig) for (pid, sig) in cur.fetchall()}

    # Current lapse candidates (as of this week_end)
    cur.execute("""
      SELECT pc.person_id::text, pc.signal, pc.bucket,
             COALESCE(pc.missed_cycles,0) AS missed_cycles,
             pc.last_seen_date, pc.expected_next_date,
             p.household_id
      FROM person_cadence pc
      JOIN pco_people p
        ON p.person_id::text = pc.person_id::text
      WHERE pc.bucket NOT IN ('irregular','one_off')
        AND COALESCE(pc.missed_cycles,0) >= 3
    """)
    rows = []
    by_signal = {"attend": 0, "give": 0, "group": 0}
    for pid, sig, bucket, missed, last_seen, expected, hh in cur.fetchall():
        pid = str(pid)
        if (pid, sig) in already:
            continue  # not newly lapsed — we've seen this person+signal in a prior week
        if pid not in eng0:
            continue  # not disengaged this week
        if hh is None or hh not in kids_hh:
            continue  # household gate
        rows.append((week_end, pid, sig, bucket, int(missed), last_seen, expected))
        by_signal[sig] = by_signal.get(sig, 0) + 1

    inserted = 0
    if rows:
        cur.executemany("""
          INSERT INTO lapses_weekly
            (week_end, person_id, signal, bucket, missed_cycles, last_seen_date, expected_next_date)
          VALUES (%s,%s,%s,%s,%s,%s,%s)
          ON CONFLICT DO NOTHING;
        """, rows)
        inserted = cur.rowcount

    conn.commit()
    cur.close(); conn.close()
    return {"inserted": int(inserted or 0), "by_signal": by_signal}

def fetch_new_lapses_for_week(week_end: date, limit: int = 100) -> list[dict]:
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
          SELECT l.person_id::text,
                 COALESCE(p.first_name,'') || ' ' || COALESCE(p.last_name,'') AS name,
                 p.email, l.signal, l.bucket, l.missed_cycles, l.last_seen_date, l.expected_next_date
          FROM lapses_weekly l
          JOIN pco_people p
                 ON p.person_id::text = l.person_id::text
          WHERE l.week_end = %s
          ORDER BY l.signal, l.missed_cycles DESC, p.last_name, p.first_name
          LIMIT %s;
        """, (week_end, limit))
        out = []
        for pid, name, email, sig, bucket, missed, last_seen, expected in cur.fetchall():
            out.append({
                "person_id": str(pid),
                "name": name.strip(),
                "email": email,
                "signal": sig,
                "bucket": bucket,
                "missed_cycles": int(missed or 0),
                "last_seen_date": last_seen.isoformat() if last_seen else None,
                "expected_next_date": expected.isoformat() if expected else None,
            })
        return out
    finally:
        cur.close(); conn.close()

def nla_count(week_end: date) -> int:
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM no_longer_attends_flat WHERE week_end = %s;", (week_end,))
        n = cur.fetchone()[0]
        return int(n or 0)
    finally:
        cur.close(); conn.close()

def _nla_pid_cast_sql(cur) -> str:
    # returns "person_id::bigint" or "person_id::text" depending on the table column type
    cur.execute("""
      SELECT data_type
      FROM information_schema.columns
      WHERE table_schema = current_schema()
        AND table_name = 'no_longer_attends_flat'
        AND column_name = 'person_id'
      LIMIT 1;
    """)
    row = cur.fetchone()
    dtype = (row[0] if row else "text").lower()
    if "bigint" in dtype or dtype in ("integer", "int8"):
        return "person_id::bigint"
    return "person_id::text"

def refresh_no_longer_attends_flat(week_end: date, inactivity_days: int = 180) -> int:
    """
    Rebuild NLA flat rows for week_end from snapshots.
    Only include people with SOME prior engagement (exclude never-active),
    and whose most recent engagement (last_any) is older than inactivity_days.
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("DELETE FROM no_longer_attends_flat WHERE week_end = %s;", (week_end,))
        pid_cast = _nla_pid_cast_sql(cur)  # ← decide cast once

        cur.execute(f"""
          WITH act AS (
            SELECT person_id,
                   MAX(week_end) FILTER (WHERE attended_bool)           AS last_attend,
                   MAX(week_end) FILTER (WHERE gifts_count > 0)         AS last_give,
                   MAX(week_end) FILTER (WHERE served_ontrack_bool)     AS last_serve,
                   MAX(week_end) FILTER (WHERE in_group_ontrack_bool)   AS last_group,
                   MIN(week_end) FILTER (
                     WHERE attended_bool OR gifts_count > 0 OR served_ontrack_bool OR in_group_ontrack_bool
                   ) AS first_seen_any
            FROM snap_person_week
            WHERE week_end <= %s
            GROUP BY person_id
          ),
          agg AS (
            SELECT
              person_id,
              NULLIF(GREATEST(
                COALESCE(last_attend, '-infinity'::date),
                COALESCE(last_give,   '-infinity'::date),
                COALESCE(last_serve,  '-infinity'::date),
                COALESCE(last_group,  '-infinity'::date)
              ), '-infinity'::date) AS last_any,
              first_seen_any, last_attend, last_give, last_serve, last_group
            FROM act
          )
          INSERT INTO no_longer_attends_flat
            (week_end, person_id, first_seen_any, last_attend, last_give, last_serve, last_group, last_any)
          SELECT %s, {pid_cast}, first_seen_any, last_attend, last_give, last_serve, last_group, last_any
          FROM agg
          WHERE last_any IS NOT NULL
            AND last_any <= %s - INTERVAL '%s days';
        """, (week_end, week_end, week_end, inactivity_days))
        inserted = cur.rowcount or 0
        conn.commit()
        return int(inserted)
    finally:
        cur.close(); conn.close()


def sample_nla(week_end: date, limit: int = 100) -> list[dict]:
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
          SELECT f.person_id::text,
                 COALESCE(p.first_name,'') || ' ' || COALESCE(p.last_name,'') AS name,
                 p.email,
                 f.first_seen_any, f.last_attend, f.last_give, f.last_serve, f.last_group, f.last_any
          FROM no_longer_attends_flat f
          JOIN pco_people p
                 ON p.person_id::text = f.person_id::text
          WHERE f.week_end = %s
          ORDER BY f.last_any NULLS LAST, p.last_name, p.first_name
          LIMIT %s;
        """, (week_end, limit))
        out = []
        for pid, name, email, first_any, la, lg, ls, lgp, lany in cur.fetchall():
            out.append({
                "person_id": str(pid),
                "name": name.strip(),
                "email": email,
                "first_seen_any": first_any.isoformat() if first_any else None,
                "last_attend": la.isoformat() if la else None,
                "last_give":   lg.isoformat() if lg else None,
                "last_serve":  ls.isoformat() if ls else None,
                "last_group":  lgp.isoformat() if lgp else None,
                "last_any":    lany.isoformat() if lany else None,
            })
        return out
    finally:
        cur.close(); conn.close()

# ── Person detail helpers ─────────────────────────────────────────────────────

def person_profile(person_id: str) -> Dict:
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
          SELECT person_id::text,
                 COALESCE(first_name,'') AS first_name,
                 COALESCE(last_name,'')  AS last_name,
                 email
          FROM pco_people
          WHERE person_id::text = %s
        """, (str(person_id),))
        row = cur.fetchone()
        if not row:
            return {}
        pid, first, last, email = row
        return {"person_id": str(pid), "first_name": first, "last_name": last, "email": email}
    finally:
        cur.close(); conn.close()

def person_cadences(person_id: str) -> List[Dict]:
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
          SELECT signal, bucket, samples_n, median_interval_days, iqr_days,
                 last_seen_date, expected_next_date, COALESCE(missed_cycles,0) AS missed_cycles
          FROM person_cadence
          WHERE person_id::text = %s
          ORDER BY signal
        """, (str(person_id),))
        out = []
        for sig, bucket, n, med, iqr, last_seen, exp_next, missed in cur.fetchall():
            out.append({
                "signal": sig,
                "bucket": bucket,
                "samples_n": int(n or 0),
                "median_interval_days": (int(med) if med is not None else None),
                "iqr_days": (int(iqr) if iqr is not None else None),
                "last_seen_date": last_seen.isoformat() if last_seen else None,
                "expected_next_date": exp_next.isoformat() if exp_next else None,
                "missed_cycles": int(missed or 0),
            })
        return out
    finally:
        cur.close(); conn.close()

def person_recent_weeks(person_id: str, *, days: int, as_of: Optional[date] = None) -> List[Dict]:
    as_of = as_of or date.today()
    start = as_of - timedelta(days=days)
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
          SELECT week_end,
                 attended_bool,
                 gifts_count,
                 served_ontrack_bool,
                 in_group_ontrack_bool,
                 engaged_tier
          FROM snap_person_week
          WHERE person_id::text = %s
            AND week_end BETWEEN %s AND %s
          ORDER BY week_end DESC
        """, (str(person_id), start, as_of))
        out = []
        for wk, att, gifts, srv, grp, tier in cur.fetchall():
            out.append({
                "week_end": wk.isoformat(),
                "attended": bool(att),
                "gifts_count": int(gifts or 0),
                "served": bool(srv),
                "in_group": bool(grp),
                "engaged_tier": int(tier or 0),
            })
        return out
    finally:
        cur.close(); conn.close()
