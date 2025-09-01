# apps/cadence.py
from __future__ import annotations

import logging
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from statistics import median
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from fastapi import APIRouter, Depends, Query, Response, HTTPException
from sqlalchemy.orm import Session
import pandas as pd
from sqlalchemy import text
from datetime import date as _date, timedelta

from app.db import engine

from app.db import get_conn, get_db
from app.utils.common import (
    CENTRAL_TZ,
    get_last_sunday_cst,
    week_bounds_for,
    get_previous_week_dates_cst,
)

log = logging.getLogger(__name__)
router = APIRouter(prefix="/analytics/cadence", tags=["Analytics"])

from app.cadence.constants import (
    MIN_SAMPLES_FOR_BUCKET,
    DEFAULT_ROLLING_DAYS,
    LAPSE_CYCLES_THRESHOLD,
    REGULAR_MIN_SAMPLES,
    bucket_days,
)


# ─────────────────────────────
# Small math helpers
# ─────────────────────────────

@dataclass
class CadenceStats:
    samples_n: int
    median_days: Optional[int]
    iqr_days: Optional[int]
    bucket: str  # 'weekly' | 'biweekly' | 'monthly' | '6weekly' | 'irregular'

BUCKET_TARGETS = [7, 14, 30, 42]  # weekly, biweekly, monthly(≈4w), 6weekly

def _to_date(val):
    if val is None:
        return None
    if isinstance(val, date):
        return val
    # handle "YYYY-MM-DD" strings
    return date.fromisoformat(val)

def _days_between(ds: Sequence[date]) -> List[int]:
    if not ds or len(ds) < 2:
        return []
    return [(ds[i] - ds[i-1]).days for i in range(1, len(ds))]

def _iqr(vals: Sequence[int]) -> Optional[int]:
    if not vals:
        return None
    vs = sorted(vals)
    n = len(vs)
    if n < 4:
        return None
    mid = n // 2
    q1 = median(vs[:mid])
    q3 = median(vs[-mid:])
    return int(round(q3 - q1))

def _missed_cycles(last_seen: Optional[date], bucket_name: str, as_of: date) -> int:
    """How many expected cycles have been missed since last_seen (0 if N/A)."""
    if not last_seen:
        return 0
    d = bucket_days(bucket_name)
    if d <= 0 or bucket_name == "irregular":
        return 0
    expected_next = last_seen + timedelta(days=d)
    if as_of <= expected_next:
        return 0
    # cycles missed = number of full cycles past expected_next
    return max(0, int((as_of - expected_next).days // d))

def _nearest_bucket(median_days: Optional[int]) -> str:
    if median_days is None:
        return "irregular"
    target = min(BUCKET_TARGETS, key=lambda t: abs(t - median_days))
    if target == 7:  return "weekly"
    if target == 14: return "biweekly"
    if target == 30: return "monthly"
    return "6weekly"

def _calc_stats(dates: Sequence[date]) -> CadenceStats:
    uniq = sorted(set(dates))
    if len(uniq) == 0:
        # We don't upsert rows with 0 samples, but keep this safe.
        return CadenceStats(0, None, None, "none")
    if len(uniq) == 1:
        # Exactly one event in the window → explicitly one_off
        return CadenceStats(1, None, None, "one_off")

    gaps = _days_between(uniq)
    med = int(round(median(gaps))) if gaps else None

    # If the median gap is > 42 days, that's truly "irregular"
    if med is not None and med > 42:
        return CadenceStats(len(uniq), med, _iqr(gaps), "irregular")

    # Otherwise, snap to the nearest standard bucket
    return CadenceStats(len(uniq), med, _iqr(gaps), _nearest_bucket(med))

# ─────────────────────────────
# DB helpers
# ─────────────────────────────

def upsert_person_cadence(rows: List[Tuple]):
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

def upsert_snap_person_week(rows: List[Tuple]):
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
              week_start            = EXCLUDED.week_start,
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

# ─────────────────────────────
# Source pulls
# ─────────────────────────────
# Only ministries that imply a parent in main service
ALLOWED_ATTEND_MINISTRIES = ("Waumba Land", "UpStreet", "Transit")

def _iter_attendance_occurrences(db: Session, since: date) -> list[tuple[str, date]]:
    """
    Household-proxy attendance: infer adult attendance from kids' check-ins
    to Waumba/UpStreet/Transit ONLY (excludes InsideOut).
    Returns rows of (adult_person_id, svc_date).
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH kid_checks AS (
              SELECT DISTINCT k.household_id, c.svc_date
              FROM f_checkins_person c
              JOIN pco_people k ON k.person_id = c.person_id
              WHERE c.svc_date >= %s
                AND c.ministry = ANY(%s)
            ),
            adults AS (
              SELECT person_id, household_id, birthdate
              FROM pco_people
            )
            SELECT a.person_id, kc.svc_date
            FROM kid_checks kc
            JOIN adults a ON a.household_id = kc.household_id
            WHERE a.birthdate IS NULL OR a.birthdate <= (%s::date - INTERVAL '18 years')
            """,
            (since, list(ALLOWED_ATTEND_MINISTRIES), since),
        )
        return [(r[0], r[1]) for r in cur.fetchall()]
    finally:
        cur.close(); conn.close()



def _fetch_giving_events(
    db: Session,
    since: Optional[date],
    *,
    as_of: date,
    rolling_days: int = DEFAULT_ROLLING_DAYS,
) -> Dict[str, List[date]]:
    """
    Returns person_id -> [week_end dates with gift_count > 0] within a rolling window.
    """
    if as_of is None:
        as_of = get_last_sunday_cst()

    # inclusive rolling window ending at `as_of`
    window_start = as_of - timedelta(days=rolling_days - 1)
    effective_start = max(filter(None, [since, window_start]))

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT person_id, week_end
            FROM f_giving_person_week
            WHERE gift_count > 0
              AND week_end >= %(start)s
              AND week_end <= %(as_of)s
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


def _fetch_adult_attendance_events(
    since: Optional[date],
    *,
    as_of: date,
    rolling_days: int = DEFAULT_ROLLING_DAYS,
) -> Dict[str, List[date]]:
    """
    Adult attendance proxied by household kid check-ins.
    Returns person_id -> [svc_date], limited to a rolling window.
    Adults are 18+ by birthdate only.
    """
    window_start = as_of - timedelta(days=rolling_days)
    effective_start = max(filter(None, [since, window_start]))  # latest of provided since or window

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT DISTINCT p.person_id, h.svc_date
            FROM household_attendance_vw h
            JOIN pco_people p ON p.household_id = h.household_id
            WHERE p.birthdate IS NOT NULL
              AND p.birthdate <= CURRENT_DATE - INTERVAL '18 years'
              AND h.svc_date >= %s
            ORDER BY p.person_id, h.svc_date;
            """,
            (effective_start,)
        )
        out: Dict[str, List[date]] = defaultdict(list)
        for pid, svc_date in cur.fetchall():
            out[pid].append(svc_date)
        return out
    finally:
        cur.close(); conn.close()

def _fetch_group_active_asof(as_of: date) -> Dict[str, bool]:
    """ person_id -> active_in_Groups (as of a given date) """
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
        return {pid: True for (pid, _active) in cur.fetchall()}
    finally:
        cur.close(); conn.close()

def _fetch_serving_active_asof(as_of: date) -> Dict[str, bool]:
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
        return {pid: True for (pid,) in cur.fetchall()}
    finally:
        cur.close(); conn.close()


# ─────────────────────────────
# Cadence builders
# ─────────────────────────────

@dataclass
class BuildRowInput:
    person_events: Dict[str, List[date]]
    signal: str
    as_of: date

def _build_rows_for_signal(
    person_events: Dict[str, List[date]],
    signal: str,
    as_of: date
) -> List[Tuple]:
    """
    Convert events -> person_cadence rows (see upsert_person_cadence signature).
    Rules:
      - skip 0-sample people (no row)
      - 1 sample => bucket 'one_off' (no expected_next, missed=0)
      - >=2 samples: if median_days > 42 => 'irregular' (no expected_next, missed=0)
                     else map to nearest standard bucket ('weekly','biweekly','monthly','6weekly')
    """
    rows: List[Tuple] = []

    for pid, dates in person_events.items():
        stats = _calc_stats(dates)
        if stats.samples_n == 0:
            continue

        last_seen = max(dates) if dates else None

        # Choose bucket + stats to persist
        if stats.samples_n == 1:
            bucket = "one_off"
            median_days = None
            iqr_days = None
        else:
            median_days = stats.median_days
            iqr_days = stats.iqr_days
            if median_days is not None and median_days > 42:
                bucket = "irregular"
            else:
                bucket = stats.bucket  # one of weekly|biweekly|monthly|6weekly

        # Only real cadence buckets get expected/missed
        if last_seen and bucket not in ("irregular", "one_off"):
            expected = last_seen + timedelta(days=bucket_days(bucket))
            missed = _missed_cycles(last_seen, bucket, as_of)
        else:
            expected = None
            missed = 0

        rows.append((
            pid,
            signal,
            median_days,
            iqr_days,
            expected,
            last_seen,
            0,            # current_streak (reserved)
            missed,       # drives “lapsed” detection for cadence signals
            bucket,
            stats.samples_n,
            "event_intervals_v2",
            None,         # campus_id
        ))

    return rows



def rebuild_person_cadence(
    db: Session,
    *,
    since: Optional[date] = None,
    signals: Iterable[str] = ("give","attend","group"),
    rolling_days: int = DEFAULT_ROLLING_DAYS,
    as_of: Optional[date] = None,   # ⬅️ add
) -> Dict[str, int]:
    as_of = as_of or get_last_sunday_cst()   # ⬅️ use override when provided
    totals = {"give": 0, "attend": 0, "group": 0}

    if "give" in signals:
        give_events = _fetch_giving_events(db, since, as_of=as_of, rolling_days=rolling_days)
        assert isinstance(give_events, dict), f"giving fetch returned {type(give_events)}"
        rows = _build_rows_for_signal(give_events, "give", as_of)
        totals["give"] = upsert_person_cadence(rows)


    if "attend" in signals:
        attend_events = _fetch_adult_attendance_events(since, as_of=as_of, rolling_days=rolling_days)
        rows = _build_rows_for_signal(attend_events, "attend", as_of)
        totals["attend"] = upsert_person_cadence(rows)
        log.info(
            "[cadence] attend upserted=%s people=%s (window=%sd, IO excluded)",
            totals["attend"], len(attend_events), rolling_days
        )

    if "group" in signals:
        # unchanged logic, still status-as-of (not a cadence), keep as-is
        active = _fetch_group_active_asof(as_of)
        conn = get_conn(); cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT m.person_id, MAX(m.first_joined_at)::date AS last_join
                FROM f_groups_memberships m
                JOIN pco_groups g ON g.group_id = m.group_id
                WHERE COALESCE(g.group_type,'') ILIKE 'Groups'
                GROUP BY m.person_id;
                """
            )
            last_join: Dict[str, Optional[date]] = {pid: d for (pid, d) in cur.fetchall()}
        finally:
            cur.close(); conn.close()

        rows = []
        for pid, last_join_dt in last_join.items():
            rows.append((
                pid, "group",
                None, None,
                None,
                last_join_dt,
                1 if active.get(pid) else 0,
                0,
                "irregular",
                1,
                "status_active_v1",
                None,
            ))
        totals["group"] = upsert_person_cadence(rows)
        log.info("[cadence] group upserted=%s people=%s (active now=%s)",
                 totals["group"], len(rows), sum(1 for v in active.values() if v))

    return totals


# ─────────────────────────────
# Weekly snapshot (Engaged tiers + “on track”)
# ─────────────────────────────

def _ontrack_give_for_week(week_start: date, week_end: date) -> Dict[str, bool]:
    """
    gave_ontrack_bool:
      - True if gift in this week, OR not yet due (expected_next_date > week_end), OR insufficient samples
      - False only when due and missed
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT person_id, gift_count
            FROM f_giving_person_week
            WHERE week_end = %s AND gift_count > 0;
            """,
            (week_end,)
        )
        gave_now = {pid: True for (pid, _g) in cur.fetchall()}

        cur.execute(
            """
            SELECT person_id, expected_next_date, samples_n
            FROM person_cadence
            WHERE signal = 'give';
            """
        )
        out: Dict[str, bool] = {}
        for pid, expected, samples_n in cur.fetchall():
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

def _attended_adults_for_week(week_start: date, week_end: date) -> Dict[str, int]:
    """ person_id -> checkins_count (adult proxy via household attendance for that week) """
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
        return {pid: c for (pid, c) in cur.fetchall()}
    finally:
        cur.close(); conn.close()

def _group_active_for_week(week_end: date) -> Dict[str, bool]:
    return _fetch_group_active_asof(week_end)

def _asof_counts(week_end: date) -> dict:
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


def build_weekly_snapshot(db: Session, *, week_end: date, ensure_cadence: bool = True):
    """ Build snap_person_week for a target week (default: last Sunday CST). """
    if ensure_cadence:
        # Only do this if the caller hasn't already rebuilt cadence
        rebuild_person_cadence(
            db, signals=("give","attend"),  # skip group for speed
            rolling_days=DEFAULT_ROLLING_DAYS,
            as_of=week_end,
        )

    if not week_end:
        week_end = get_last_sunday_cst()
    week_start, wk_end = week_bounds_for(week_end)
    assert wk_end == week_end

    attended = _attended_adults_for_week(week_start, week_end)
    gave_ontrack = _ontrack_give_for_week(week_start, week_end)
    group_active = _group_active_for_week(week_end)
    serving_active = _fetch_serving_active_asof(week_end)

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            "SELECT person_id, gift_count FROM f_giving_person_week WHERE week_end = %s;",
            (week_end,)
        )
        gifts_now = {pid: cnt for (pid, cnt) in cur.fetchall()}
    finally:
        cur.close(); conn.close()

    person_ids = set(attended) | set(gave_ontrack) | set(group_active) | set(serving_active) | set(gifts_now)

    rows: List[Tuple] = []
    for pid in person_ids:
        att_cnt = attended.get(pid, 0)
        att_bool = att_cnt > 0
        give_on = gave_ontrack.get(pid, True)   # default to True if unknown / not due
        served_on = bool(serving_active.get(pid))
        group_on   = bool(group_active.get(pid))

        engaged_tier = int(give_on) + int(served_on) + int(group_on)

        rows.append((
            pid, week_start, week_end,
            att_bool, give_on, served_on, group_on,
            engaged_tier, att_cnt, gifts_now.get(pid, 0), 0,
            None
        ))

    affected = upsert_snap_person_week(rows)
    log.info("[cadence] snap_person_week upserted=%s for week_end=%s (people=%s)",
             affected, week_end, len(rows))
    return {"snap_rows_upserted": affected, "people": len(rows)}

def _avg_adult_attendance_last4(week_end: date) -> Optional[float]:
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH last4 AS (
              SELECT total_attendance
              FROM adult_attendance
              WHERE date <= %s
              ORDER BY date DESC
              LIMIT 4
            )
            SELECT AVG(total_attendance)::float FROM last4;
            """,
            (week_end,)
        )
        row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else None
    finally:
        cur.close(); conn.close()

def _front_door_counts_for_week(week_start: date, week_end: date) -> Tuple[int,int,int,int]:
    """
    First-ever events (lifetime minimum date) whose first date falls inside the week:
      • checkins: first svc_date in f_checkins_person
      • giving:   first week_end with gift_count > 0 in f_giving_person_week
      • groups:   first first_joined_at among groups with group_type ILIKE 'Groups'
      • serving:  first first_joined_at among groups flagged is_serving_team = TRUE
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH first_checkins AS (
              SELECT person_id, MIN(svc_date) AS first_dt
              FROM f_checkins_person GROUP BY 1
            ),
            first_gifts AS (
              SELECT person_id, MIN(week_end) AS first_dt
              FROM f_giving_person_week
              WHERE gift_count > 0
              GROUP BY 1
            ),
            first_groups AS (
              SELECT m.person_id, MIN(m.first_joined_at::date) AS first_dt
              FROM f_groups_memberships m
              JOIN pco_groups g ON g.group_id = m.group_id
              WHERE COALESCE(g.group_type,'') ILIKE 'Groups'
              GROUP BY 1
            ),
            first_serving AS (
              SELECT m.person_id, MIN(m.first_joined_at::date) AS first_dt
              FROM f_groups_memberships m
              JOIN pco_groups g ON g.group_id = m.group_id
              WHERE g.is_serving_team = TRUE
              GROUP BY 1
            )
            SELECT
              (SELECT COUNT(*) FROM first_checkins WHERE first_dt BETWEEN %s AND %s) AS first_time_checkins,
              (SELECT COUNT(*) FROM first_gifts    WHERE first_dt BETWEEN %s AND %s) AS first_time_givers,
              (SELECT COUNT(*) FROM first_groups   WHERE first_dt BETWEEN %s AND %s) AS first_time_groups,
              (SELECT COUNT(*) FROM first_serving  WHERE first_dt BETWEEN %s AND %s) AS first_time_serving
            ;
            """,
            (week_start, week_end, week_start, week_end, week_start, week_end, week_start, week_end)
        )
        cks, gvs, grps, srv = cur.fetchone()
        return int(cks), int(gvs), int(grps), int(srv)
    finally:
        cur.close(); conn.close()


def _upsert_front_door_weekly(week_start: date, week_end: date, counts: Tuple[int,int,int,int]) -> int:
    cks, gvs, grps, srv = counts
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO front_door_weekly
              (week_start, week_end, first_time_checkins, first_time_givers, first_time_groups, first_time_serving, campus_id)
            VALUES (%s,%s,%s,%s,%s,%s,NULL)
            ON CONFLICT (week_start) DO UPDATE SET
              week_end            = EXCLUDED.week_end,
              first_time_checkins = EXCLUDED.first_time_checkins,
              first_time_givers   = EXCLUDED.first_time_givers,
              first_time_groups   = EXCLUDED.first_time_groups,
              first_time_serving  = EXCLUDED.first_time_serving;
            """,
            (week_start, week_end, cks, gvs, grps, srv)
        )
        n = cur.rowcount
        conn.commit()
        return n
    finally:
        cur.close(); conn.close()

def _count_serving_active_asof(as_of: date) -> int:
    """
    Distinct people actively serving as of `as_of` (Sunday),
    based on serving-team memberships that are active at that date.
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(DISTINCT m.person_id)
            FROM f_groups_memberships m
            JOIN pco_groups g ON g.group_id = m.group_id
            WHERE g.is_serving_team = TRUE
              AND m.status = 'active'
              AND (m.first_joined_at IS NULL OR m.first_joined_at::date <= %s)
              AND (m.archived_at   IS NULL OR m.archived_at::date   >  %s)
            ;
            """,
            (as_of, as_of),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    finally:
        cur.close(); conn.close()

# ─────────────────────────────
# Buckets & Lapse Events helpers
# ─────────────────────────────

def _bucket_counts(
    signal: str,
    *,
    week_end: date,
    exclude_lapsed: bool = True,
) -> dict:
    """
    Return counts for cadence buckets among people present in the weekly snapshot:
    {weekly, biweekly, monthly, 6weekly, irregular, one_off}.

    NOTE: This *does not* include zero-sample people (we never wrote rows for them).
    If exclude_lapsed=True, we drop people whose cadence bucket is a real cadence
    AND missed_cycles >= 3.
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        sql = """
        WITH candidates AS (
          SELECT pc.bucket
          FROM person_cadence pc
          JOIN snap_person_week s
            ON s.person_id = pc.person_id
           AND s.week_end  = %s
          WHERE pc.signal = %s
        )
        SELECT bucket, COUNT(*)::int
        FROM candidates
        {where_exclude}
        GROUP BY bucket;
        """

        where_exclude = ""
        params = [week_end, signal]

        if exclude_lapsed:
            # exclude only real cadence buckets that have missed >= 3
            sql = """
            WITH candidates AS (
              SELECT pc.person_id, pc.bucket
              FROM person_cadence pc
              JOIN snap_person_week s
                ON s.person_id = pc.person_id
               AND s.week_end  = %s
              WHERE pc.signal = %s
            )
            SELECT c.bucket, COUNT(*)::int
            FROM candidates c
            JOIN person_cadence pc ON pc.person_id = c.person_id AND pc.signal = %s
            WHERE NOT (pc.bucket NOT IN ('irregular','one_off') AND pc.missed_cycles >= 3)
            GROUP BY c.bucket;
            """
            params = [week_end, signal, signal]

        cur.execute(sql, params)
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    tpl = {"weekly": 0, "biweekly": 0, "monthly": 0, "6weekly": 0, "irregular": 0, "one_off": 0}
    for b, c in rows:
        if b in tpl:
            tpl[b] = int(c)
    return tpl



def _insert_lapse_events(week_end: date, candidates: list[tuple]) -> list[dict]:
    """
    Insert lapse events and return enriched rows (joined with person info) for response.
    candidates: list of (person_id, signal, expected_by, observed_none_since, missed_cycles)
    """
    if not candidates:
        return []

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.executemany(
            """
            INSERT INTO lapse_events
              (person_id, signal, expected_by, observed_none_since, missed_cycles, week_flagged, campus_id)
            VALUES (%s,%s,%s,%s,%s,%s,NULL)
            ON CONFLICT (person_id, signal, week_flagged) DO NOTHING;
            """,
            [(pid, sig, exp, obs, mc, week_end) for (pid, sig, exp, obs, mc) in candidates]
        )
        conn.commit()
    finally:
        cur.close(); conn.close()

    # Pull back details w/ person info for the rows we *intended* to insert (OK if some were deduped)
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT e.person_id, e.signal, e.expected_by, e.observed_none_since, e.missed_cycles,
                   c.bucket, c.last_seen_date,
                   p.first_name, p.last_name, p.email
            FROM lapse_events e
            JOIN person_cadence c ON c.person_id = e.person_id AND c.signal = e.signal
            JOIN pco_people p      ON p.person_id = e.person_id
            WHERE e.week_flagged = %s
            ORDER BY e.signal, p.last_name, p.first_name;
            """,
            (week_end,)
        )
        out = []
        for pid, sig, exp, obs, mc, bucket, last_seen, fn, ln, email in cur.fetchall():
            out.append({
                "person_id": pid,
                "name": f"{fn or ''} {ln or ''}".strip(),
                "email": email,
                "signal": sig,
                "bucket": bucket,
                "last_seen_date": last_seen.isoformat() if last_seen else None,
                "expected_by": exp.isoformat() if exp else None,
                "observed_none_since": obs.isoformat() if obs else None,
                "missed_cycles": mc,
            })
        return out
    finally:
        cur.close(); conn.close()

def _engaged_flag_for_signal_col(signal: str) -> str:
    return {
        "attend": "attended_bool",
        "give":   "gave_ontrack_bool",
        "serve":  "served_ontrack_bool",
        "group":  "in_group_ontrack_bool",
    }[signal]

def _engagement_counts_for_week(week_end: date) -> dict:
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
              SUM(CASE WHEN (gave_on::int + serving_on::int + in_group_on::int)=0 THEN 1 ELSE 0 END) AS engaged0,
              SUM(CASE WHEN (gave_on::int + serving_on::int + in_group_on::int)=1 THEN 1 ELSE 0 END) AS engaged1,
              SUM(CASE WHEN (gave_on::int + serving_on::int + in_group_on::int)=2 THEN 1 ELSE 0 END) AS engaged2,
              SUM(CASE WHEN (gave_on::int + serving_on::int + in_group_on::int)=3 THEN 1 ELSE 0 END) AS engaged3
            FROM person_engagement_weekly
            WHERE week_end = %s;
            """,
            (week_end,),
        )
        e0, e1, e2, e3 = cur.fetchone()
        return {"engaged0": e0 or 0, "engaged1": e1 or 0, "engaged2": e2 or 0, "engaged3": e3 or 0}
    finally:
        cur.close(); conn.close()


def detect_and_write_lapses_for_week(
    week_end: date,
    signals: tuple[str, ...] = ("attend","give"),
    *,
    inactivity_days_for_drop: int = 90,
) -> dict:
    """
    Returns a rich payload:
      - newly: list of first-time lapses inserted this week (after ATTEND gating)
      - counts_by_signal for newly
      - all_lapsed_counts: current rows with missed_cycles >= 3 (after ATTEND gating)
      - no_longer_attends: people with no engagement in any signal for >= inactivity_days_for_drop
      - reengaged: people who had any prior lapse and are now back on-track this week
    ATTEND gating:
      • only count as lapsed if Engaged tier == 0 this week, AND household still has kids < 14.
    """
    # ----- Eligible (this week) with ATTEND gating -----
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH pc AS (
            SELECT person_id, signal, bucket, samples_n, last_seen_date, expected_next_date, missed_cycles
            FROM person_cadence
            WHERE signal = ANY(%s)
                AND samples_n >= 2
                AND bucket NOT IN ('irregular','one_off')
                AND missed_cycles >= 3
            ),
            base AS (
            SELECT pc.*, s.engaged_tier,
                    a.household_id,
                    EXISTS (
                    SELECT 1 FROM pco_people kid
                    WHERE kid.household_id = a.household_id
                        AND kid.birthdate IS NOT NULL
                        AND kid.birthdate > %s::date - INTERVAL '14 years'
                    ) AS has_kid_u14
            FROM pc
            LEFT JOIN pco_people a ON a.person_id = pc.person_id
            LEFT JOIN snap_person_week s
                    ON s.person_id = pc.person_id AND s.week_end = %s
            ),
            eligible AS (
            SELECT *
            FROM base
            WHERE
                (signal <> 'attend')
                OR (signal = 'attend' AND COALESCE(engaged_tier,0) = 0 AND has_kid_u14)
            ),
            first_time AS (
            SELECT e.*
            FROM eligible e
            LEFT JOIN LATERAL (
                SELECT 1 FROM lapse_events le
                WHERE le.person_id = e.person_id AND le.signal = e.signal
                LIMIT 1
            ) prev ON TRUE
            WHERE prev IS NULL
            )
            SELECT person_id, signal, bucket, last_seen_date, expected_next_date, missed_cycles
            FROM first_time
            """,
            (list(signals), week_end, week_end),
        )

        newly_rows = cur.fetchall()

        # Also compute ALL currently lapsed (after gating)
        cur.execute(
            """
            WITH pc AS (
            SELECT person_id, signal
            FROM person_cadence
            WHERE signal = ANY(%s)
                AND samples_n >= 2
                AND bucket NOT IN ('irregular','one_off')
                AND missed_cycles >= 3
            ),
            base AS (
            SELECT pc.person_id, pc.signal, s.engaged_tier,
                    a.household_id,
                    EXISTS (
                    SELECT 1 FROM pco_people kid
                    WHERE kid.household_id = a.household_id
                        AND kid.birthdate IS NOT NULL
                        AND kid.birthdate > %s::date - INTERVAL '14 years'
                    ) AS has_kid_u14
            FROM pc
            LEFT JOIN pco_people a ON a.person_id = pc.person_id
            LEFT JOIN snap_person_week s
                    ON s.person_id = pc.person_id AND s.week_end = %s
            )
            SELECT signal, COUNT(*)::int
            FROM base
            WHERE signal <> 'attend'
            OR (signal = 'attend' AND COALESCE(engaged_tier,0) = 0 AND has_kid_u14)
            GROUP BY signal
            """,
            (list(signals), week_end, week_end),
        )
        all_lapsed_counts = {sig: 0 for sig in signals}
        for sig, cnt in cur.fetchall():
            all_lapsed_counts[sig] = int(cnt)

        # Build insert rows
        candidates = []
        for pid, sig, bucket, last_seen, expected, missed in newly_rows:
            exp = expected or (last_seen + timedelta(days=bucket_days(bucket)) if last_seen and bucket else None)
            obs = last_seen
            candidates.append((pid, sig, exp, obs, missed))
    finally:
        cur.close(); conn.close()

    # ----- Insert newly-lapsed, read back enriched items -----
    inserted_items = _insert_lapse_events(week_end, candidates)
    newly_counts = {s: 0 for s in signals}
    for it in inserted_items:
        if it["signal"] in newly_counts:
            newly_counts[it["signal"]] += 1

    # ----- No-longer-attends (no engagement for 6 months) -----
    drop_before = week_end - timedelta(days=inactivity_days_for_drop)
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH ls AS (
            SELECT person_id,
                    MAX(CASE WHEN signal='attend' THEN last_seen_date END) AS last_attend,
                    MAX(CASE WHEN signal='give'   THEN last_seen_date END) AS last_give,
                    MAX(CASE WHEN signal='serve'  THEN last_seen_date END) AS last_serve
            FROM person_cadence
            WHERE signal IN ('attend','give','serve')
            GROUP BY person_id
            ),
            active_group AS (
            SELECT DISTINCT person_id
            FROM f_groups_memberships
            WHERE status = 'active'
            ),
            inactive AS (
            SELECT ls.person_id
            FROM ls
            LEFT JOIN active_group g ON g.person_id = ls.person_id
            WHERE COALESCE(ls.last_attend, DATE '1900-01-01') <= %s
                AND COALESCE(ls.last_give,   DATE '1900-01-01') <= %s
                AND COALESCE(ls.last_serve,  DATE '1900-01-01') <= %s
                AND g.person_id IS NULL
            )
            SELECT p.person_id, p.first_name, p.last_name, p.email
            FROM inactive i
            JOIN pco_people p ON p.person_id = i.person_id
            LEFT JOIN snap_person_week s
                ON s.person_id = i.person_id
                AND s.week_end  = %s
            WHERE COALESCE(s.engaged_tier, 0) = 0
            ORDER BY p.last_name, p.first_name
            """,
            # NOTE: extra param 'week_end' added at the end:
            (drop_before, drop_before, drop_before, week_end),
        )
        nla_items = [
            {"person_id": pid, "name": f"{fn or ''} {ln or ''}".strip(), "email": email}
            for (pid, fn, ln, email) in cur.fetchall()
        ]
    finally:
        cur.close(); conn.close()


    # ----- Re-engaged this week (had prior lapse, now on-track) -----
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            f"""
            WITH prior AS (
              SELECT DISTINCT person_id, signal
              FROM lapse_events
              WHERE week_flagged < %s
            ),
            now_on AS (
              SELECT person_id, 'attend' AS signal FROM snap_person_week
              WHERE week_end = %s AND attended_bool
              UNION ALL
              SELECT person_id, 'give'   FROM snap_person_week
              WHERE week_end = %s AND gave_ontrack_bool
              UNION ALL
              SELECT person_id, 'serve'  FROM snap_person_week
              WHERE week_end = %s AND served_ontrack_bool
            ),
            re AS (
              SELECT DISTINCT n.person_id, n.signal
              FROM now_on n
              JOIN prior  p USING (person_id, signal)
              WHERE n.signal = ANY(%s)
            )
            SELECT r.person_id, r.signal, pp.first_name, pp.last_name, pp.email
            FROM re r
            JOIN pco_people pp ON pp.person_id = r.person_id
            ORDER BY r.signal, pp.last_name, pp.first_name
            """,
            (week_end, week_end, week_end, week_end, list(signals)),
        )
        re_items = [
            {"person_id": pid, "signal": sig, "name": f"{fn or ''} {ln or ''}".strip(), "email": email}
            for (pid, sig, fn, ln, email) in cur.fetchall()
        ]
        re_counts = {s: 0 for s in signals}
        for it in re_items:
            re_counts[it["signal"]] += 1
    finally:
        cur.close(); conn.close()

    return {
        "newly": {
            "inserted_total": len(inserted_items),
            "inserted_by_signal": newly_counts,
            "items": inserted_items,
        },
        "all_lapsed_counts": all_lapsed_counts,
        "no_longer_attends": {
            "count": len(nla_items),
            "items": nla_items,
            "threshold_days": inactivity_days_for_drop,
        },
        "reengaged": {
            "count_by_signal": re_counts,
            "items": re_items,
        },
    }

def _count_total_lapsed_people(*, signals: Tuple[str, ...] = ("attend","give","serve"), lapse_threshold: int = 3) -> int:
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(DISTINCT person_id)::int
            FROM person_cadence
            WHERE signal = ANY(%s)
              AND samples_n >= 2
              AND bucket NOT IN ('irregular','one_off')
              AND missed_cycles >= %s
            """,
            (list(signals), lapse_threshold)
        )
        (n,) = cur.fetchone()
        return int(n or 0)
    finally:
        cur.close(); conn.close()



def _fetch_newly_lapsed_aggregate(week_end: date) -> tuple[int, dict, list]:
    """
    People who FIRST reached missed_cycles = 3 in this week, by signal.
    Only consider 'attend' and 'give' lapses (we no longer track 'serve' cadence).
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT e.person_id, e.signal, e.expected_by, e.observed_none_since, e.missed_cycles,
                   COALESCE(pc.bucket, 'irregular') AS bucket,
                   p.first_name, p.last_name, p.email
            FROM lapse_events e
            JOIN pco_people p ON p.person_id = e.person_id
            LEFT JOIN person_cadence pc
              ON pc.person_id = e.person_id AND pc.signal = e.signal
            WHERE e.week_flagged = %s
              AND e.missed_cycles = 3
              AND e.signal IN ('attend','give')
            ORDER BY p.last_name, p.first_name, e.signal;
            """,
            (week_end,),
        )
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    by_signal: dict[str, int] = {"attend": 0, "give": 0}
    per_person: dict[str, dict] = {}

    for pid, sig, exp_by, none_since, missed, bucket, fn, ln, email in rows:
        by_signal[sig] = by_signal.get(sig, 0) + 1
        if pid not in per_person:
            per_person[pid] = {
                "person_id": pid,
                "name": f"{fn or ''} {ln or ''}".strip(),
                "email": email,
                "lapsed": []
            }
        per_person[pid]["lapsed"].append({
            "signal": sig,
            "bucket": bucket,
            "expected_by": str(exp_by) if exp_by else None,
            "observed_none_since": str(none_since) if none_since else None,
            "missed_cycles": missed
        })

    items = list(per_person.values())
    return len(items), by_signal, items

def _fetch_newly_no_longer_attends(week_end: date, inactivity_days: int = 90) -> list[dict]:
    """
    People who *first* crossed the 3-month inactivity threshold in this week
    AND are Engaged 0 for week_end. Includes per-signal last dates and first_seen_any.
    Engagement signals considered: attend, give, serve, group.
    """
    drop_before = week_end - timedelta(days=inactivity_days)
    prev_drop_before = week_end - timedelta(days=inactivity_days + 7)

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH ls AS (
              SELECT person_id,
                     MAX(CASE WHEN signal='attend' THEN last_seen_date END) AS last_attend,
                     MAX(CASE WHEN signal='give'   THEN last_seen_date END) AS last_give,
                     MAX(CASE WHEN signal='serve'  THEN last_seen_date END) AS last_serve
              FROM person_cadence
              WHERE signal IN ('attend','give','serve')
              GROUP BY person_id
            ),
            last_grp AS (
              -- If still active at week_end, treat last group activity as week_end; else archived_at
              SELECT person_id,
                     MAX(
                       COALESCE(
                         CASE WHEN archived_at IS NULL THEN %s::date ELSE archived_at::date END,
                         NULL
                       )
                     ) AS last_group
              FROM f_groups_memberships
              GROUP BY person_id
            ),
            greatest_last AS (
              SELECT ls.person_id,
                     GREATEST(
                       COALESCE(ls.last_attend, DATE '1900-01-01'),
                       COALESCE(ls.last_give,   DATE '1900-01-01'),
                       COALESCE(ls.last_serve,  DATE '1900-01-01'),
                       COALESCE(lg.last_group,  DATE '1900-01-01')
                     ) AS last_any
              FROM ls
              LEFT JOIN last_grp lg ON lg.person_id = ls.person_id
            ),
            engaged0 AS (
              SELECT s.person_id
              FROM snap_person_week s
              WHERE s.week_end = %s AND COALESCE(s.engaged_tier,0) = 0
            ),
            newly AS (
              SELECT gl.person_id, gl.last_any
              FROM greatest_last gl
              JOIN engaged0 e0 ON e0.person_id = gl.person_id
              WHERE gl.last_any > %s  -- not already “dropped” before last week
                AND gl.last_any <= %s -- crosses the 90-day boundary this week
            )
            SELECT
              p.person_id, p.first_name, p.last_name, p.email,
              gl.last_any AS last_any_date,
              ls.last_attend, ls.last_give, ls.last_serve, lg.last_group,
              (
                SELECT MIN(x) FROM (VALUES
                  ((SELECT MIN(c.svc_date) FROM f_checkins_person c WHERE c.person_id = p.person_id)),
                  ((SELECT MIN(g.week_end)  FROM f_giving_person_week g WHERE g.person_id = p.person_id AND g.gift_count > 0)),
                  ((SELECT MIN(m.first_joined_at::date) FROM f_groups_memberships m WHERE m.person_id = p.person_id)),
                  (p.created_at_pco::date),
                  (p.first_seen)
                ) AS t(x)
              ) AS first_seen_any
            FROM newly n
            JOIN pco_people p ON p.person_id = n.person_id
            JOIN greatest_last gl ON gl.person_id = n.person_id
            LEFT JOIN ls ON ls.person_id = n.person_id
            LEFT JOIN last_grp lg ON lg.person_id = n.person_id
            ORDER BY p.last_name, p.first_name;
            """,
            (week_end, week_end, prev_drop_before, drop_before),
        )
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    items: list[dict] = []
    for (pid, fn, ln, email, last_any, last_attend, last_give, last_serve, last_group, first_any) in rows:
        items.append({
            "person_id": pid,
            "name": f"{fn or ''} {ln or ''}".strip(),
            "email": email,
            "first_seen_any": (str(first_any) if first_any else None),
            "last_signals": {
                "attend":  (str(last_attend) if last_attend else None),
                "give":    (str(last_give)   if last_give   else None),
                "serve":   (str(last_serve)  if last_serve  else None),
                "group":   (str(last_group)  if last_group  else None),
                "last_any":(str(last_any)    if last_any    else None),
            }
        })
    return items

def _fetch_all_lapsed_people(
    week_end: date,
    *,
    signals: Tuple[str, ...] = ("attend", "give"),
    min_samples: int = REGULAR_MIN_SAMPLES,
    lapse_threshold: int = LAPSE_CYCLES_THRESHOLD,
) -> list[dict]:
    """
    All people currently considered 'lapsed' as-of week_end, with cadence buckets by signal.
    ATTEND gating:
      • engaged_tier == 0 at week_end
      • household has kids < 14
    Only includes real cadence buckets (excludes irregular/one_off).

    Returns dicts like:
      {
        "person_id": "...",
        "name": "First Last",
        "email": "...",
        "signals": ["attend","give"],
        "cadence": {"attend":"weekly","give":"biweekly"},
        "lapsed_cycles": 3
      }
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH pc AS (
              SELECT person_id, signal, bucket, samples_n, missed_cycles
              FROM person_cadence
              WHERE signal = ANY(%s)
                AND samples_n >= %s
                AND bucket NOT IN ('irregular','one_off')
                AND missed_cycles >= %s
            ),
            base AS (
              SELECT pc.*, s.engaged_tier, a.household_id,
                     EXISTS (
                       SELECT 1 FROM pco_people kid
                       WHERE kid.household_id = a.household_id
                         AND kid.birthdate IS NOT NULL
                         AND kid.birthdate > %s::date - INTERVAL '14 years'
                     ) AS has_kid_u14
              FROM pc
              LEFT JOIN pco_people a ON a.person_id = pc.person_id
              LEFT JOIN snap_person_week s
                ON s.person_id = pc.person_id
               AND s.week_end  = %s
            ),
            eligible AS (
              SELECT *
              FROM base
              WHERE COALESCE(engaged_tier,0) = 0
                AND has_kid_u14
            ),
            -- Deduplicate in case multiple rows per (person, signal) slip through
            eligible1 AS (
              SELECT DISTINCT ON (person_id, signal)
                     person_id, signal, bucket, missed_cycles
              FROM eligible
              ORDER BY person_id, signal, missed_cycles DESC
            ),
            rollup AS (
              SELECT e.person_id,
                     JSONB_OBJECT_AGG(e.signal, e.bucket)         AS cadence_by_signal,
                     ARRAY_AGG(DISTINCT e.signal)                  AS signals,
                     MAX(e.missed_cycles)                          AS lapsed_cycles
              FROM eligible1 e
              GROUP BY e.person_id
            )
            SELECT p.person_id, p.first_name, p.last_name, p.email,
                   r.signals, r.lapsed_cycles, r.cadence_by_signal
            FROM rollup r
            JOIN pco_people p ON p.person_id = r.person_id
            ORDER BY r.lapsed_cycles ASC, p.last_name, p.first_name;
            """,
            (list(signals), min_samples, lapse_threshold, week_end, week_end),
        )
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    out = []
    for (pid, fn, ln, email, sig_arr, lc, cadence_json) in rows:
        # psycopg2 may already decode jsonb -> dict; if not, parse from string
        if isinstance(cadence_json, str):
            cadence = json.loads(cadence_json) if cadence_json else {}
        else:
            cadence = cadence_json or {}
        out.append({
            "person_id": pid,
            "name": f"{(fn or '').strip()} {(ln or '').strip()}".strip(),
            "email": email,
            "signals": list(sig_arr or []),
            "cadence": cadence,                       # e.g., {"attend":"weekly","give":"biweekly"}
            "lapsed_cycles": int(lc) if lc is not None else None,
        })
    return out

def write_engagement_tier_transitions(week_end: date) -> int:
    prev = week_end - timedelta(days=7)
    sql = """
    INSERT INTO engagement_tier_transitions
      (person_id, week_end, from_tier, to_tier, delta, campus_id)
    SELECT c.person_id, %s, p.engaged_tier AS from_tier, c.engaged_tier AS to_tier,
           (c.engaged_tier - p.engaged_tier) AS delta, c.campus_id
    FROM snap_person_week c
    JOIN snap_person_week p
      ON p.person_id = c.person_id AND p.week_end = %s
    WHERE c.week_end = %s
      AND c.engaged_tier < p.engaged_tier
    ON CONFLICT (person_id, week_end) DO NOTHING;
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(sql, (week_end, prev, week_end))
        n = cur.rowcount or 0
        conn.commit()
        return n
    finally:
        cur.close(); conn.close()


def persist_new_nla_events(week_end: date, inactivity_days: int = 90) -> int:
    """Insert first-time NLA events for this week; ignore if already recorded."""
    items = _fetch_newly_no_longer_attends(week_end, inactivity_days=inactivity_days)
    if not items:
        return 0

    # Build VALUES list for bulk insert — ensure DATE types
    vals = []
    for i in items:
        last_any_dt = _to_date(i["last_signals"].get("last_any"))
        first_any_dt = _to_date(i.get("first_seen_any"))
        # last_any_date is NOT NULL in the table; sanity guard
        if last_any_dt is None:
            continue
        vals.append((i["person_id"], week_end, last_any_dt, first_any_dt))

    if not vals:
        return 0

    placeholders = ",".join(["(%s,%s,%s,%s)"] * len(vals))
    args = [x for row in vals for x in row]

    sql = f"""
    WITH v(person_id, week_end, last_any_date, first_seen_any) AS (
      VALUES {placeholders}
    )
    INSERT INTO no_longer_attends_events
      (person_id, week_end, last_any_date, first_seen_any, campus_id)
    SELECT v.person_id, v.week_end, v.last_any_date, v.first_seen_any, s.campus_id
    FROM v
    LEFT JOIN snap_person_week s
      ON s.person_id = v.person_id AND s.week_end = v.week_end
    ON CONFLICT (person_id) DO NOTHING;
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(sql, args)
        n = cur.rowcount or 0
        conn.commit()
        return n
    finally:
        cur.close(); conn.close()



def upsert_back_door_weekly(week_end: date, reengaged_count: int = 0) -> dict:
    prev = week_end - timedelta(days=7)
    sql = """
    WITH prev AS (
      SELECT person_id,
             gave_ontrack_bool     AS prev_give,
             served_ontrack_bool   AS prev_serve,
             in_group_ontrack_bool AS prev_group
      FROM snap_person_week
      WHERE week_end = %s
    ),
    curr AS (
      SELECT person_id,
             gave_ontrack_bool     AS curr_give,
             served_ontrack_bool   AS curr_serve,
             in_group_ontrack_bool AS curr_group
      FROM snap_person_week
      WHERE week_end = %s
    ),
    -- most informative cadence bucket per person for giving
    cad AS (
      SELECT DISTINCT ON (pc.person_id)
             pc.person_id, pc.bucket
      FROM person_cadence pc
      WHERE pc.signal = 'give'
      ORDER BY pc.person_id, pc.samples_n DESC
    ),
    -- last gift date up to week_end from fact table
    last_gift AS (
      SELECT person_id, MAX(week_end)::date AS last_gift_week
      FROM f_giving_person_week
      WHERE week_end <= %s AND gift_count > 0
      GROUP BY person_id
    ),
    stops AS (
      SELECT
        e.person_id, e.from_tier, e.to_tier,
        (pv.prev_serve = TRUE AND co.curr_serve = FALSE) AS stop_serve,
        (pv.prev_group = TRUE AND co.curr_group = FALSE) AS stop_group,
        (
          pv.prev_give = TRUE AND co.curr_give = FALSE
          AND lg.last_gift_week IS NOT NULL
          AND ((%s::date - lg.last_gift_week) >= GREATEST(
                60,
                CASE cad.bucket
                  WHEN 'weekly'   THEN 2*7
                  WHEN 'biweekly' THEN 2*14
                  WHEN 'monthly'  THEN 2*30
                  WHEN '6weekly'  THEN 2*42
                  ELSE 60
                END
              ))
        ) AS stop_give
      FROM engagement_tier_transitions e
      LEFT JOIN prev pv   ON pv.person_id   = e.person_id
      LEFT JOIN curr co   ON co.person_id   = e.person_id
      LEFT JOIN cad       ON cad.person_id  = e.person_id
      LEFT JOIN last_gift lg ON lg.person_id = e.person_id
      WHERE e.week_end = %s
    ),
    agg AS (
      SELECT
        COUNT(*) FILTER (WHERE (stop_serve OR stop_group OR stop_give))                                         AS downshifts_total,
        COUNT(*) FILTER (WHERE from_tier=3 AND to_tier=2 AND (stop_serve OR stop_group OR stop_give))           AS d_3_2,
        COUNT(*) FILTER (WHERE from_tier=2 AND to_tier=1 AND (stop_serve OR stop_group OR stop_give))           AS d_2_1,
        COUNT(*) FILTER (WHERE from_tier=1 AND to_tier=0 AND (stop_serve OR stop_group OR stop_give))           AS d_1_0
      FROM stops
    ),
    new_nla AS (
      SELECT COUNT(*) AS c FROM no_longer_attends_events WHERE week_end = %s
    )
    INSERT INTO back_door_weekly
      (week_end, downshifts_total, downshift_3_to_2, downshift_2_to_1, downshift_1_to_0,
       new_nla_count, reengaged_count, bdi)
    SELECT
      %s,
      a.downshifts_total, a.d_3_2, a.d_2_1, a.d_1_0,
      nn.c, %s,
      (a.downshifts_total + nn.c - %s)::numeric
    FROM agg a, new_nla nn
    ON CONFLICT (week_end) DO UPDATE SET
      downshifts_total   = EXCLUDED.downshifts_total,
      downshift_3_to_2   = EXCLUDED.downshift_3_to_2,
      downshift_2_to_1   = EXCLUDED.downshift_2_to_1,
      downshift_1_to_0   = EXCLUDED.downshift_1_to_0,
      new_nla_count      = EXCLUDED.new_nla_count,
      reengaged_count    = EXCLUDED.reengaged_count,
      bdi                = EXCLUDED.bdi
    RETURNING downshifts_total, downshift_3_to_2, downshift_2_to_1, downshift_1_to_0, new_nla_count, reengaged_count, bdi;
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(sql, (
            prev,            # prev
            week_end,        # curr
            week_end,        # last_gift
            week_end,        # stop_give days calc
            week_end,        # stops filter
            week_end,        # new_nla
            week_end,        # INSERT week_end
            reengaged_count, # INSERT value
            reengaged_count, # BDI subtract
        ))
        row = cur.fetchone()
        conn.commit()
        return {
            "downshifts_total": row[0], "downshift_3_to_2": row[1], "downshift_2_to_1": row[2],
            "downshift_1_to_0": row[3], "new_nla_count": row[4], "reengaged_count": row[5], "bdi": row[6],
        }
    finally:
        cur.close(); conn.close()


def backdoor_tenure_stats() -> dict:
    """
    Summarize 'time to leave' (days between first_seen_any and last_any_date)
    from no_longer_attends_events. Returns integers for avg/p50/p90.
    """
    sql = """
    WITH d AS (
      SELECT (last_any_date - first_seen_any) AS days
      FROM no_longer_attends_events
      WHERE first_seen_any IS NOT NULL
    )
    SELECT
      COALESCE(AVG(days), 0)::int AS avg_days,
      COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days), 0)::int AS p50,
      COALESCE(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY days), 0)::int AS p90,
      COUNT(*)::int AS people
    FROM d;
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(sql)
        avg_days, p50, p90, n = cur.fetchone()
    finally:
        cur.close(); conn.close()
    return {"avg_days": int(avg_days or 0), "p50_days": int(p50 or 0), "p90_days": int(p90 or 0), "people": int(n or 0)}

def get_downshifts_people(limit: int = 200) -> pd.DataFrame:
    with engine.connect() as c:
        wk = c.execute(text("SELECT MAX(week_end) FROM engagement_tier_transitions;")).scalar()
        if not wk:
            return pd.DataFrame(columns=["person_id","name","email","from_tier","to_tier","stopped","campus_id"])

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
                )
                SELECT e.person_id,
                       COALESCE(p.first_name,'') || ' ' || COALESCE(p.last_name,'') AS name,
                       COALESCE(p.email,'') AS email,
                       e.from_tier, e.to_tier, e.campus_id,
                       ARRAY_REMOVE(ARRAY[
                         CASE WHEN pv.prev_give = TRUE AND co.curr_give = FALSE
                                   AND lg.last_gift_week IS NOT NULL
                                   AND ((:wk::date - lg.last_gift_week) >= GREATEST(
                                         60,
                                         CASE cad.bucket
                                           WHEN 'weekly'   THEN 2*7
                                           WHEN 'biweekly' THEN 2*14
                                           WHEN 'monthly'  THEN 2*30
                                           WHEN '6weekly'  THEN 2*42
                                           ELSE 60
                                         END
                                       ))
                              THEN 'giving' END,
                         CASE WHEN pv.prev_serve = TRUE AND co.curr_serve = FALSE THEN 'serving' END,
                         CASE WHEN pv.prev_group = TRUE AND co.curr_group = FALSE THEN 'groups' END
                       ], NULL) AS stopped_signals
                FROM engagement_tier_transitions e
                JOIN pco_people p ON p.person_id = e.person_id
                LEFT JOIN prev pv   ON pv.person_id   = e.person_id
                LEFT JOIN curr co   ON co.person_id   = e.person_id
                LEFT JOIN cad       ON cad.person_id  = e.person_id
                LEFT JOIN last_gift lg ON lg.person_id = e.person_id
                WHERE e.week_end = :wk
                ORDER BY e.from_tier DESC, e.to_tier, p.last_name, p.first_name
                LIMIT :l
            """),
            con=engine,
            params={"wk": wk, "prev": wk - pd.Timedelta(days=7), "l": limit},
        )

    df["stopped"] = df["stopped_signals"].apply(lambda arr: ", ".join(arr) if isinstance(arr, list) and arr else "")
    if "stopped_signals" in df.columns:
        df = df.drop(columns=["stopped_signals"])
    return df.reindex(columns=[c for c in ["person_id","name","email","from_tier","to_tier","stopped","campus_id"] if c in df.columns])

def get_downshift_flow(week_end: date) -> dict:
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH prev AS (
              SELECT person_id,
                     gave_ontrack_bool     AS prev_give,
                     served_ontrack_bool   AS prev_serve,
                     in_group_ontrack_bool AS prev_group
              FROM snap_person_week
              WHERE week_end = %s - INTERVAL '7 days'
            ),
            curr AS (
              SELECT person_id,
                     gave_ontrack_bool     AS curr_give,
                     served_ontrack_bool   AS curr_serve,
                     in_group_ontrack_bool AS curr_group
              FROM snap_person_week
              WHERE week_end = %s
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
              WHERE week_end <= %s AND gift_count > 0
              GROUP BY person_id
            ),
            stops AS (
              SELECT e.person_id,
                     (pv.prev_serve = TRUE AND co.curr_serve = FALSE) AS stop_serve,
                     (pv.prev_group = TRUE AND co.curr_group = FALSE) AS stop_group,
                     (
                       pv.prev_give = TRUE AND co.curr_give = FALSE
                       AND lg.last_gift_week IS NOT NULL
                       AND ((%s::date - lg.last_gift_week) >= GREATEST(
                             60,
                             CASE cad.bucket
                               WHEN 'weekly'   THEN 2*7
                               WHEN 'biweekly' THEN 2*14
                               WHEN 'monthly'  THEN 2*30
                               WHEN '6weekly'  THEN 2*42
                               ELSE 60
                             END
                           ))
                     ) AS stop_give
              FROM engagement_tier_transitions e
              LEFT JOIN prev pv   ON pv.person_id   = e.person_id
              LEFT JOIN curr co   ON co.person_id   = e.person_id
              LEFT JOIN cad       ON cad.person_id  = e.person_id
              LEFT JOIN last_gift lg ON lg.person_id = e.person_id
              WHERE e.week_end = %s
            )
            SELECT e.from_tier, e.to_tier, COUNT(*)::int AS c
            FROM engagement_tier_transitions e
            JOIN stops s ON s.person_id = e.person_id
            WHERE e.week_end = %s
              AND (s.stop_serve OR s.stop_group OR s.stop_give)
            GROUP BY 1,2
            ORDER BY from_tier DESC, to_tier DESC;
            """,
            (week_end, week_end, week_end, week_end, week_end, week_end),
        )
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    matrix = [{"from": f, "to": t, "count": int(c)} for (f, t, c) in rows]
    by_from, total = {"3": 0, "2": 0, "1": 0}, 0
    for r in matrix:
        by_from[str(r["from"])] += r["count"]
        total += r["count"]
    return {"matrix": matrix, "from_breakdown": by_from, "total": total}


# ─────────────────────────────
# Routes
# ─────────────────────────────

@router.get("/rebuild", response_model=dict)
def api_rebuild_cadence(
    signals: str = Query("give,attend,group", description="Comma list: give, attend, group, serve"),
    since: Optional[str] = Query(None, description="Only consider events on/after YYYY-MM-DD (for attend/give/group)"),
    rolling_days: int = Query(DEFAULT_ROLLING_DAYS, ge=30, le=730, description="Rolling window for cadence stats"),
    week_end: Optional[str] = Query(None, description="Sunday YYYY-MM-DD used for 'serve' (and any as-of counts)"),
    db: Session = Depends(get_db),
):
    # Parse inputs
    sigs = [s.strip().lower() for s in signals.split(",") if s.strip()]
    since_dt = date.fromisoformat(since) if since else None
    week_end_dt = date.fromisoformat(week_end) if week_end else get_last_sunday_cst()

    # Only pass true cadence signals into the cadence builder
    cadence_sigs = [s for s in sigs if s in ("attend", "give", "group")]

    totals = {}
    if cadence_sigs:
        totals.update(
            rebuild_person_cadence(
                db,
                since=since_dt,
                signals=cadence_sigs,
                rolling_days=rolling_days,
            )
        )

    # New: serving is not a cadence; we return the active count as-of week_end
    if "serve" in sigs:
        totals["serve"] = _count_serving_active_asof(week_end_dt)

    return {
        "status": "ok",
        "signals": sigs,
        "since": str(since_dt) if since_dt else None,
        "week_end": str(week_end_dt),
        "rolling_days": rolling_days,
        **totals,
    }


@router.get("/snap-week", response_model=dict)
def api_snap_week(
    week_end: Optional[str] = Query(None, description="Week end date (Sunday, YYYY-MM-DD). Defaults to last Sunday CST."),
    db: Session = Depends(get_db)
):
    week_end_dt = date.fromisoformat(week_end) if week_end else None
    res = build_weekly_snapshot(db, week_end=week_end_dt)
    if not week_end_dt:
        ws, we = get_previous_week_dates_cst()
        return {"status": "ok", "week_start": ws, "week_end": we, **res}
    else:
        ws, we = week_bounds_for(week_end_dt)
        return {"status": "ok", "week_start": str(ws), "week_end": str(we), **res}


@router.get("/attendance-buckets", response_model=dict)
def api_attendance_buckets(
    window_days: int = Query(DEFAULT_ROLLING_DAYS, ge=30, le=730, description="Rolling window to compute cadence."),
    exclude_lapsed: bool = Query(True, description="Exclude people who missed ≥3 cycles."),
    db: Session = Depends(get_db),
):
    """
    Rebuild attendance cadence (rolling window) and return bucket counts.
    'Lapsed' = missed_cycles ≥ 3 for non-irregular buckets.
    """
    rebuild_person_cadence(db, signals=["attend"], rolling_days=window_days)

    conn = get_conn(); cur = conn.cursor()
    try:
        if exclude_lapsed:
            cur.execute(
                """
                SELECT bucket, COUNT(*)::int
                FROM person_cadence
                WHERE signal='attend'
                  AND NOT (bucket <> 'irregular' AND missed_cycles >= %s)
                GROUP BY bucket;
                """,
                (LAPSE_CYCLES_THRESHOLD,)
            )
        else:
            cur.execute(
                """
                SELECT bucket, COUNT(*)::int
                FROM person_cadence
                WHERE signal='attend'
                GROUP BY bucket;
                """
            )
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    # normalize output
    buckets = {"weekly": 0, "biweekly": 0, "monthly": 0, "6weekly": 0, "irregular": 0}
    for b, c in rows:
        if b in buckets:
            buckets[b] = int(c)

    return {
        "status": "ok",
        "window_days": window_days,
        "exclude_lapsed": exclude_lapsed,
        "buckets": buckets,
        "lapsed_threshold_cycles": LAPSE_CYCLES_THRESHOLD,
        "min_samples_for_bucket": MIN_SAMPLES_FOR_BUCKET,
    }

def build_weekly_report(
    db: Session,
    week_end: Optional[date] = None,
    *,
    ensure_snapshot: bool = True,
    persist_front_door: bool = True,
    rolling_days: int = DEFAULT_ROLLING_DAYS,
) -> dict:
    if not week_end:
        week_end = get_last_sunday_cst()
    week_start, wk_end = week_bounds_for(week_end)
    assert wk_end == week_end

    # 1) Rebuild cadences (attend, give; "group" is status-based but harmless to include)
    rebuild_person_cadence(db, signals=("give", "attend", "group"), rolling_days=rolling_days)

    # 2) Ensure snapshot for this Sunday (if your snapshot supports giving lookback, pass 60 there)
    if ensure_snapshot:
        build_weekly_snapshot(db, week_end=week_end, ensure_cadence=False)

    write_engagement_tier_transitions(week_end)

    # 3) Cadence buckets (serve cadence removed)
    buckets = {
        "attend": _bucket_counts("attend", week_end=week_end, exclude_lapsed=True),
        "give":   _bucket_counts("give",   week_end=week_end, exclude_lapsed=True),
    }

    # 4) Front Door
    fd_counts = _front_door_counts_for_week(week_start, week_end)
    if persist_front_door:
        _upsert_front_door_weekly(week_start, week_end, fd_counts)

    backdoor_tenure_stats()
    # 5) Engaged tiers directly from snapshot (no person_engagement_weekly table needed)
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT engaged_tier, COUNT(*)::int
            FROM snap_person_week
            WHERE week_end = %s
            GROUP BY engaged_tier;
            """,
            (week_end,)
        )
        tiers = {int(t): int(c) for (t, c) in cur.fetchall()}
    finally:
        cur.close(); conn.close()

    engaged = {
        "engaged0": tiers.get(0, 0),
        "engaged1": tiers.get(1, 0),
        "engaged2": tiers.get(2, 0),
        "engaged3": tiers.get(3, 0),
    }

    # 6) Lapses & no-longer-attends
    payload = detect_and_write_lapses_for_week(
        week_end,
        signals=("attend","give"),  # groups excluded from “lapsed” logic
        inactivity_days_for_drop=90,
    )
    new_lapsed_total_people = payload.get("new_total_people", 0)
    new_lapsed_by_signal    = payload.get("new_by_signal", {})
    all_lapsed_by_signal    = payload.get("all_lapsed_counts", {})  # current stock by signal

    # Items that *hit* threshold this week, split out for UI (attend vs give)
    items_attend = [i for i in payload.get("items", []) if any(l["signal"]=="attend" for l in i["lapsed"])]
    items_give   = [i for i in payload.get("items", []) if any(l["signal"]=="give"   for l in i["lapsed"])]

    # Totals in stock, limited to the signals above and excluding irregular/one_off
    total_lapsed_people = _count_total_lapsed_people(signals=("attend","give"), lapse_threshold=3)

    # 7) Average adult attendance (last 4 Sundays)
    avg4 = _avg_adult_attendance_last4(week_end)

    asof = _asof_counts(week_end)

    # persist new NLA events for tenure analytics
    persist_new_nla_events(week_end, inactivity_days=90)

    # use the payload's reengaged count if available (fallback 0)
    reengaged_ct = int(payload.get("reengaged_count", 0)) if isinstance(payload.get("reengaged_count", 0), (int, float)) else 0

    # upsert Back Door weekly roll-up
    bd = upsert_back_door_weekly(week_end, reengaged_count=reengaged_ct)
    flow = get_downshift_flow(week_end)


    return {
        "week_start": str(week_start),
        "week_end": str(week_end),
        "cadence_buckets": buckets,
        "engaged": engaged,
        "front_door": {
            "first_time_checkins": fd_counts[0],
            "first_time_givers":   fd_counts[1],
            "first_time_groups":   fd_counts[2],
            "first_time_serving":  fd_counts[3],
        },
        "back_door": {
            "downshifts_total": bd["downshifts_total"],
            "downshift_3_to_2": bd["downshift_3_to_2"],
            "downshift_2_to_1": bd["downshift_2_to_1"],
            "downshift_1_to_0": bd["downshift_1_to_0"],
            "from_breakdown":   flow["from_breakdown"],  
            "flow":             flow["matrix"],          
            "new_nla_count":    bd["new_nla_count"],
            "reengaged_count":  bd["reengaged_count"],
            "bdi":              float(bd["bdi"]),
        },
        "lapses": {
            "new_this_week_total": new_lapsed_total_people,
            "new_by_signal": new_lapsed_by_signal,
            "total_lapsed_people": total_lapsed_people,
            "all_lapsed_by_signal": all_lapsed_by_signal,
            "items_attend": items_attend,
            "items_give": items_give,
            "all_lapsed_people": _fetch_all_lapsed_people(week_end, signals=("attend","give")),  # ★ NEW
        },
        "no_longer_attends": {
            "added_this_week": len(_fetch_newly_no_longer_attends(week_end, inactivity_days=90)),
            "items":           _fetch_newly_no_longer_attends(week_end, inactivity_days=90),
        },
        "as_of": asof,
        "adult_attendance_avg_4w": avg4,
        "notes": [
            "Engaged tiers use snapshot booleans (giving + serving + in_group). Attendance isn’t part of the tier.",
            "Lapsed = missed ≥ 3 cycles; items_* lists include those hitting 3 missed cycles this week.",
            "“No longer attends” = no engagement for 90 days; list shows only those added this week.",
        ],
    }



@router.get("/cadences", response_model=dict)
def api_list_cadences(
    signal: str = Query(..., pattern="^(attend|give)$"),
    bucket: Optional[str] = Query(None, pattern="^(weekly|biweekly|monthly|6weekly|irregular)$"),
    exclude_lapsed: bool = Query(True),
    q: Optional[str] = Query(None, description="Search name or email (ILIKE)"),
    order_by: str = Query("expected_next_date_asc", pattern="^(expected_next_date_asc|last_seen_desc|missed_cycles_desc|samples_desc)$"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    Browse attendance/giving cadences. Excludes lapsed (missed_cycles>=3) by default for non-irregular buckets.
    """
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
        where.append("NOT (c.bucket <> 'irregular' AND c.missed_cycles >= 3)")

    if q:
        where.append("((p.first_name || ' ' || p.last_name) ILIKE %s OR p.email ILIKE %s)")
        like = f"%{q}%"
        params.extend([like, like])

    where_sql = " AND ".join(where) if where else "TRUE"

    # total count
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM person_cadence c
            JOIN pco_people p USING (person_id)
            WHERE {where_sql};
            """,
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
        } for r in cur.fetchall()]
    finally:
        cur.close(); conn.close()

    return {
        "status": "ok",
        "signal": signal,
        "bucket": bucket,
        "exclude_lapsed": exclude_lapsed,
        "order_by": order_by,
        "limit": limit,
        "offset": offset,
        "total": total,
        "items": items,
    }

@router.get("/person/{person_id}", response_model=dict)
def api_person_cadence(person_id: str, days: int = Query(180, ge=30, le=730)):
    """Show a person's attendance & giving cadence + recent events."""
    # basic person
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("SELECT first_name, last_name, email, household_id FROM pco_people WHERE person_id = %s;", (person_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="person not found")
        first, last, email, hh_id = row

        # cadences
        cur.execute(
            """
            SELECT signal, bucket, samples_n, median_interval_days, iqr_days,
                   last_seen_date, expected_next_date, missed_cycles
            FROM person_cadence
            WHERE person_id = %s AND signal IN ('attend','give');
            """,
            (person_id,)
        )
        cad = {}
        for s, b, n, med, iqr, last_seen, exp_next, miss in cur.fetchall():
            cad[s] = {
                "bucket": b, "samples_n": n, "median_interval_days": med, "iqr_days": iqr,
                "last_seen_date": last_seen.isoformat() if last_seen else None,
                "expected_next_date": exp_next.isoformat() if exp_next else None,
                "missed_cycles": miss,
                "lapsed": (b != "irregular" and (miss or 0) >= 3),
            }

        # attendance events (adult proxy via household)
        start = date.today() - timedelta(days=days)
        cur.execute(
            """
            SELECT svc_date
            FROM household_attendance_vw
            WHERE household_id = %s AND svc_date >= %s
            ORDER BY svc_date DESC;
            """,
            (hh_id, start)
        )
        attend_dates = [d.isoformat() for (d,) in cur.fetchall()]

        # giving events (any gift weeks)
        cur.execute(
            """
            SELECT week_end, gift_count
            FROM f_giving_person_week
            WHERE person_id = %s AND week_end >= %s AND gift_count > 0
            ORDER BY week_end DESC;
            """,
            (person_id, start)
        )
        giving_weeks = [{"week_end": we.isoformat(), "gift_count": gc} for (we, gc) in cur.fetchall()]

    finally:
        cur.close(); conn.close()

    return {
        "status": "ok",
        "person": {"person_id": person_id, "name": f"{first} {last}".strip(), "email": email},
        "cadence": cad,
        "events": {"attendance_dates": attend_dates, "giving_weeks": giving_weeks},
    }

@router.api_route("/reset", methods=["GET","POST"], response_model=dict)
def api_reset_cadence(
    confirm: str = Query(..., description="Must equal 'RESET-CADENCE'"),
    backfill: bool = Query(False, description="If true, (re)build multiple weeks"),
    signals: str = Query("give,attend", description="Comma list: give,attend,group"),
    start_date: Optional[date] = Query(None, description="YYYY-MM-DD; first Sunday on/after this date"),
    end_date: Optional[date] = Query(None, description="YYYY-MM-DD; last Sunday on/before this date"),
    db: Session = Depends(get_db),
):
    if confirm != "RESET-CADENCE":
        raise HTTPException(status_code=400, detail="Confirmation token mismatch")

    sigs = tuple(s.strip().lower() for s in signals.split(",") if s.strip() in {"give","attend","group"})

    # 1) Clear derived tables
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE lapse_events;")
        cur.execute("TRUNCATE TABLE snap_person_week;")
        cur.execute("TRUNCATE TABLE person_cadence;")
        conn.commit()
    finally:
        cur.close(); conn.close()

    last_sun = get_last_sunday_cst()

    if not backfill:
        # One-week rebuild (current week only)
        rebuild_person_cadence(db, signals=sigs, rolling_days=DEFAULT_ROLLING_DAYS, as_of=last_sun)
        build_weekly_snapshot(db, week_end=last_sun)
        payload = detect_and_write_lapses_for_week(last_sun, signals=("attend","give"), inactivity_days_for_drop=90)
        return {
            "status": "ok",
            "mode": "current_week_only",
            "week_end": str(last_sun),
            "newly_lapsed_by_signal": payload.get("counts_by_signal", {}),
            "all_lapsed_by_signal": payload.get("all_lapsed_counts", {}),
        }

    # backfill = True → rebuild everything
    def _earliest_for_signals(sigs: tuple[str, ...]) -> date:
        # Only look at sources we’re rebuilding
        q = []
        if "attend" in sigs:
            q.append("SELECT MIN(svc_date)::date AS d FROM f_checkins_person")
        if "give" in sigs:
            q.append("SELECT MIN(week_end)::date   AS d FROM f_giving_person_week")
        if "group" in sigs:
            q.append("SELECT MIN(first_joined_at::date) AS d FROM f_groups_memberships")
        sql = " WITH mins AS (" + " UNION ALL ".join(q) + ") SELECT MIN(d)::date FROM mins;"
        conn = get_conn(); cur = conn.cursor()
        try:
            cur.execute(sql)
            d = cur.fetchone()[0]
            return d or last_sun
        finally:
            cur.close(); conn.close()

    start = start_date or _earliest_for_signals(sigs)
    end   = end_date or last_sun

    # Snap to Sundays: first Sunday ≥ start, last Sunday ≤ end
    first_sunday = start + timedelta(days=(6 - start.weekday()) % 7)             # Mon=0..Sun=6
    last_sunday  = end - timedelta(days=((end.weekday() + 1) % 7))               # back to prior/same Sunday

    if first_sunday > last_sunday:
        raise HTTPException(status_code=400, detail=f"No Sundays in range {start}..{end}")

    # Iterate Sundays
    weeks_processed = 0
    wk = first_sunday
    while wk <= last_sunday:
        rebuild_person_cadence(db, signals=sigs, rolling_days=DEFAULT_ROLLING_DAYS, as_of=wk)
        build_weekly_snapshot(db, week_end=wk)
        
        write_engagement_tier_transitions(wk)
        detect_and_write_lapses_for_week(wk, signals=("attend","give"), inactivity_days_for_drop=90)
        persist_new_nla_events(wk, inactivity_days=90)
        
        weeks_processed += 1
        wk += timedelta(days=7)

    return {
        "status": "ok",
        "mode": "range_backfill",
        "signals": sigs,
        "from": str(first_sunday),
        "to": str(last_sunday),
        "weeks_processed": weeks_processed,
    }


@router.get("/weekly-report", response_model=dict)
def api_weekly_report(
    week_end: Optional[str] = Query(None, description="Sunday YYYY-MM-DD; defaults to last Sunday"),
    ensure_snapshot: bool = Query(True, description="Rebuild snapshot for this week before reporting"),
    persist_front_door: bool = Query(True, description="Upsert counts into front_door_weekly"),
    rolling_days: int = Query(DEFAULT_ROLLING_DAYS, ge=60, le=730),
    db: Session = Depends(get_db),
):
    week_end_dt = date.fromisoformat(week_end) if week_end else None
    report = build_weekly_report(
        db,
        week_end=week_end_dt,
        ensure_snapshot=ensure_snapshot,
        persist_front_door=persist_front_door,
        rolling_days=rolling_days,
    )
    return {"status": "ok", **report}

@router.get("/backdoor/export/nla.csv")
def export_nla(week_end: Optional[str] = Query(None)):
    wk = date.fromisoformat(week_end) if week_end else get_last_sunday_cst()
    items = _fetch_newly_no_longer_attends(wk, inactivity_days=90)
    # CSV rows
    lines = ["person_id,name,email,first_seen_any,last_attend,last_give,last_serve,last_group,last_any"]
    for i in items:
        ls = i["last_signals"]
        lines.append(",".join([
            i["person_id"],
            f"\"{i['name']}\"",
            (i["email"] or ""),
            (i["first_seen_any"] or ""),
            (ls.get("attend") or ""),
            (ls.get("give") or ""),
            (ls.get("serve") or ""),
            (ls.get("group") or ""),
            (ls.get("last_any") or ""),
        ]))
    csv = "\n".join(lines)
    return Response(content=csv, media_type="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=nla_{wk}.csv"})

@router.get("/backdoor/export/downshifts.csv")
def export_downshifts(week_end: Optional[str] = Query(None)):
    wk = date.fromisoformat(week_end) if week_end else get_last_sunday_cst()
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
          SELECT e.person_id, p.first_name, p.last_name, p.email, e.from_tier, e.to_tier, e.campus_id
          FROM engagement_tier_transitions e
          JOIN pco_people p ON p.person_id = e.person_id
          WHERE e.week_end = %s
          ORDER BY e.from_tier DESC, p.last_name, p.first_name;
        """, (wk,))
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    lines = ["person_id,name,email,from_tier,to_tier,campus_id"]
    for pid, fn, ln, email, f, t, campus in rows:
        lines.append(",".join([pid, f"\"{(fn or '')} {(ln or '')}\"".strip(), email or "", str(f), str(t), campus or ""]))
    csv = "\n".join(lines)
    return Response(content=csv, media_type="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=downshifts_{wk}.csv"})
