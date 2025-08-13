# apps/cadence.py
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from statistics import median
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from fastapi import HTTPException
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db import get_conn, get_db
from app.utils.common import (
    CENTRAL_TZ,
    get_last_sunday_cst,
    week_bounds_for,
    get_previous_week_dates_cst,
)

log = logging.getLogger(__name__)
router = APIRouter(prefix="/analytics/cadence", tags=["Analytics"])

# Add near other constants
MIN_SAMPLES_FOR_BUCKET = 4
DEFAULT_ROLLING_DAYS = 180
LAPSE_CYCLES_THRESHOLD = 3


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
    d = _bucket_days(bucket_name)
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
    gaps = _days_between(uniq)
    if len(uniq) <= 1:
        return CadenceStats(len(uniq), None, None, "irregular")
    med = int(round(median(gaps))) if gaps else None
    return CadenceStats(len(uniq), med, _iqr(gaps), _nearest_bucket(med))

def _bucket_days(name: str) -> int:
    return {"weekly": 7, "biweekly": 14, "monthly": 30, "6weekly": 42}.get(name, 28)

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
    window_start = as_of - timedelta(days=rolling_days)
    effective_start = max(filter(None, [since, window_start]))

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT person_id, week_end
            FROM f_giving_person_week
            WHERE gift_count > 0
              AND week_end >= %s
            ORDER BY person_id, week_end;
            """,
            (effective_start,)
        )
        out: Dict[str, List[date]] = defaultdict(list)
        for pid, wk_end in cur.fetchall():
            out[pid].append(wk_end)
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
              AND m.first_joined_at::date <= %s
              AND (m.archived_at IS NULL OR m.archived_at::date > %s)
            GROUP BY m.person_id;
            """,
            (as_of, as_of)
        )
        return {pid: True for (pid, _active) in cur.fetchall()}
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
    Enforces:
      - last 6 months (handled by fetchers)
      - MIN_SAMPLES_FOR_BUCKET guard
      - missed_cycles & expected_next_date at 'as_of'
    """
    rows: List[Tuple] = []
    for pid, dates in person_events.items():
        stats = _calc_stats(dates)
        last_seen = max(dates) if dates else None

        # Low-sample guard
        if stats.samples_n < MIN_SAMPLES_FOR_BUCKET:
            bucket = "irregular"
            median_days = None
            iqr = None
        else:
            bucket = stats.bucket
            median_days = stats.median_days
            iqr = stats.iqr_days

        # Expectations & lapses
        expected = None
        if last_seen and bucket != "irregular":
            expected = last_seen + timedelta(days=_bucket_days(bucket))
        missed = _missed_cycles(last_seen, bucket, as_of)

        rows.append((
            pid,
            signal,
            median_days,
            iqr,
            expected,
            last_seen,
            0,           # current_streak (can add later)
            missed,      # missed_cycles drives “lapsed”
            bucket,
            stats.samples_n,
            "event_intervals_v2",
            None,        # campus_id
        ))
    return rows


def rebuild_person_cadence(
    db: Session,
    *,
    since: Optional[date] = None,
    signals: Iterable[str] = ("give","attend","group"),
    rolling_days: int = DEFAULT_ROLLING_DAYS,
) -> Dict[str, int]:
    """Rebuild cadence for selected signals using a rolling window (default 180 days)."""
    as_of = get_last_sunday_cst()
    totals = {"give": 0, "attend": 0, "group": 0}

    if "give" in signals:
        give_events = _fetch_giving_events(db, since, as_of=as_of, rolling_days=rolling_days)
        rows = _build_rows_for_signal(give_events, "give", as_of)
        totals["give"] = upsert_person_cadence(rows)
        log.info("[cadence] give upserted=%s people=%s (window=%sd)", totals["give"], len(give_events), rolling_days)

    if "attend" in signals:
        # Use IO-excluding kid check-ins (Waumba, UpStreet, Transit) to infer adult attendance.
        # Respect the same rolling window you pass to other signals.
        window_start = as_of - timedelta(days=rolling_days)

        # Pull raw occurrences as (person_id, svc_date)
        occ_rows = _iter_attendance_occurrences(db, since=window_start)

        # Group into {person_id: [dates...]} within the window
        per_person_dates: dict[str, list[date]] = defaultdict(list)
        for pid, svc_dt in occ_rows:
            if window_start <= svc_dt <= as_of:
                per_person_dates[pid].append(svc_dt)

        # De-dup and sort each person’s dates (some families may have multiple kid check-ins same day)
        attend_events = {pid: sorted(set(dts)) for pid, dts in per_person_dates.items()}

        # Reuse your existing builder (expects {person_id: [dates...]})
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

def build_weekly_snapshot(db: Session, week_end: Optional[date] = None) -> Dict[str, int]:
    """ Build snap_person_week for a target week (default: last Sunday CST). """
    if not week_end:
        week_end = get_last_sunday_cst()
    week_start, wk_end = week_bounds_for(week_end)
    assert wk_end == week_end

    attended = _attended_adults_for_week(week_start, week_end)
    gave_ontrack = _ontrack_give_for_week(week_start, week_end)
    group_active = _group_active_for_week(week_end)

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            "SELECT person_id, gift_count FROM f_giving_person_week WHERE week_end = %s;",
            (week_end,)
        )
        gifts_now = {pid: cnt for (pid, cnt) in cur.fetchall()}
    finally:
        cur.close(); conn.close()

    person_ids = set(attended.keys()) | set(gave_ontrack.keys()) | set(group_active.keys()) | set(gifts_now.keys())

    rows: List[Tuple] = []
    for pid in person_ids:
        att_cnt = attended.get(pid, 0)
        att_bool = att_cnt > 0
        give_on = gave_ontrack.get(pid, True)   # default to True if unknown / not due
        serve_on = False                        # placeholder until serving cadence is wired
        group_on = group_active.get(pid, False)

        engaged_tier = int(give_on) + int(serve_on) + int(group_on)

        rows.append((
            pid, week_start, week_end,
            att_bool, give_on, serve_on, group_on,
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
            )
            SELECT
              (SELECT COUNT(*) FROM first_checkins WHERE first_dt BETWEEN %s AND %s) AS first_time_checkins,
              (SELECT COUNT(*) FROM first_gifts    WHERE first_dt BETWEEN %s AND %s) AS first_time_givers,
              (SELECT COUNT(*) FROM first_groups   WHERE first_dt BETWEEN %s AND %s) AS first_time_groups,
              0 AS first_time_serving -- placeholder until serving is wired
            ;
            """,
            (week_start, week_end, week_start, week_end, week_start, week_end)
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

# ─────────────────────────────
# Buckets & Lapse Events helpers
# ─────────────────────────────

def _bucket_counts(signal: str, *, exclude_lapsed: bool = True) -> dict:
    """
    Return {weekly, biweekly, monthly, 6weekly, irregular} counts for a signal
    (attendance/giving/serve). By default excludes lapsed (missed_cycles >= 3).
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        if exclude_lapsed:
            cur.execute(
                """
                SELECT bucket, COUNT(*)::int
                FROM person_cadence
                WHERE signal = %s
                  AND NOT (bucket <> 'irregular' AND missed_cycles >= 3)
                GROUP BY bucket;
                """,
                (signal,)
            )
        else:
            cur.execute(
                """
                SELECT bucket, COUNT(*)::int
                FROM person_cadence
                WHERE signal = %s
                GROUP BY bucket;
                """,
                (signal,)
            )
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    tpl = {"weekly": 0, "biweekly": 0, "monthly": 0, "6weekly": 0, "irregular": 0}
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


def _bucket_days(bucket: str) -> int:
    return {"weekly": 7, "biweekly": 14, "monthly": 28, "6weekly": 42}.get(bucket, 9999)

def _engaged_flag_for_signal_col(signal: str) -> str:
    return {
        "attend": "attended_bool",
        "give":   "gave_ontrack_bool",
        "serve":  "served_ontrack_bool",
        "group":  "in_group_ontrack_bool",
    }[signal]

def detect_and_write_lapses_for_week(
    week_end: date,
    signals: tuple[str, ...] = ("attend","give","serve"),
    *,
    inactivity_days_for_drop: int = 180,
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
              SELECT person_id, signal, bucket, last_seen_date, expected_next_date, missed_cycles
              FROM person_cadence
              WHERE signal = ANY(%s) AND bucket <> 'irregular' AND missed_cycles >= 3
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
                -- For non-attendance signals, no extra gating:
                (signal <> 'attend')
                -- For attendance: must be NOT engaged (tier 0) and still have kids < 14
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
              WHERE signal = ANY(%s) AND bucket <> 'irregular' AND missed_cycles >= 3
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
            exp = expected or (last_seen + timedelta(days=_bucket_days(bucket)) if last_seen and bucket else None)
            obs = exp
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

def _count_total_lapsed_people(lapse_threshold: int = 3) -> int:
    """
    Unique people who are currently lapsed in ANY tracked signal (attend/give/serve),
    excluding irregular buckets.
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(DISTINCT person_id)
            FROM person_cadence
            WHERE signal IN ('attend','give','serve')
              AND bucket <> 'irregular'
              AND missed_cycles >= %s
            """,
            (lapse_threshold,),
        )
        return cur.fetchone()[0] or 0
    finally:
        cur.close(); conn.close()


def _fetch_newly_lapsed_aggregate(week_end: date) -> tuple[int, dict, list]:
    """
    Aggregate *newly* lapsed people for the given week_end from lapse_events.
    Returns: (unique_people_total, by_signal_counts, items[])
    items[] = [{ person_id, name, email, lapsed: [{signal,bucket,expected_by,observed_none_since,missed_cycles}] }]
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT e.person_id, e.signal, e.expected_by, e.observed_none_since, e.missed_cycles,
                   pc.bucket,
                   p.first_name, p.last_name, p.email
            FROM lapse_events e
            JOIN pco_people p ON p.person_id = e.person_id
            LEFT JOIN person_cadence pc ON pc.person_id = e.person_id AND pc.signal = e.signal
            WHERE e.week_flagged = %s
            ORDER BY p.last_name, p.first_name, e.signal
            """,
            (week_end,),
        )
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    by_signal: dict[str,int] = {"attend": 0, "give": 0, "serve": 0}
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
            "expected_by": str(exp_by),
            "observed_none_since": str(none_since),
            "missed_cycles": missed
        })

    items = list(per_person.values())
    return len(items), by_signal, items


def _fetch_newly_no_longer_attends(week_end: date, inactivity_days: int = 180) -> list[dict]:
    """
    People who *first* crossed the 6-month inactivity threshold in this week
    AND are Engaged 0 for week_end. Also returns first signal/profile date and last signal dates.
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
              SELECT person_id, MAX(first_joined_at::date) AS last_group
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
              WHERE gl.last_any > %s  -- not already “dropped” last week
                AND gl.last_any <= %s -- first time this week
            )
            SELECT
              p.person_id, p.first_name, p.last_name, p.email,
              gl.last_any AS last_any_date,
              -- provide per-signal lasts
              ls.last_attend, ls.last_give, ls.last_serve, lg.last_group,
              -- first signal or profile created/first_seen
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
            ORDER BY p.last_name, p.first_name
            """,
            (week_end, prev_drop_before, drop_before),
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
                "attend": (str(last_attend) if last_attend else None),
                "give":   (str(last_give)   if last_give   else None),
                "serve":  (str(last_serve)  if last_serve  else None),
                "group":  (str(last_group)  if last_group  else None),
                "last_any": (str(last_any) if last_any else None),
            }
        })
    return items



# ─────────────────────────────
# Routes
# ─────────────────────────────

@router.get("/rebuild", response_model=dict)
def api_rebuild_cadence(
    signals: str = Query("give,attend,group", description="Comma list of signals: give,attend,group"),
    since: Optional[str] = Query(None, description="Only consider events on/after this date (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    sigs = [s.strip().lower() for s in signals.split(",") if s.strip()]
    since_dt = date.fromisoformat(since) if since else None
    totals = rebuild_person_cadence(db, since=since_dt, signals=sigs)
    return {"status": "ok", "signals": sigs, "since": str(since_dt) if since_dt else None, **totals}

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
    """
    One-call weekly pipeline:
      1) Rebuild cadences for attend, give, group (rolling window).
      2) Build weekly snapshot rows (snap_person_week).
      3) Bucket histograms for attend/give/serve (serve zeros until wired).
      4) Front Door firsts (persist to front_door_weekly).
      5) Engaged tier counts (from snapshot).
      6) Detect & write first-time lapse events for attend/give/serve (returns list).
      7) 4-week average adult attendance.
    """
    # Week window (Mon..Sun)
    if not week_end:
        week_end = get_last_sunday_cst()
    week_start, wk_end = week_bounds_for(week_end)
    assert wk_end == week_end

    # 1) Rebuild cadences (rolling window; group is status-based)
    rebuild_person_cadence(db, signals=("give","attend","group"), rolling_days=rolling_days)

    # 2) Ensure snapshot
    if ensure_snapshot:
        build_weekly_snapshot(db, week_end=week_end)

    # 3) Buckets (exclude lapsed)
    buckets = {
        "attend": _bucket_counts("attend", exclude_lapsed=True),
        "give":   _bucket_counts("give",   exclude_lapsed=True),
        "serve":  _bucket_counts("serve",  exclude_lapsed=True),  # likely all zeros for now
    }

    # 4) Front Door (persist)
    fd_counts = _front_door_counts_for_week(week_start, week_end)
    if persist_front_door:
        _upsert_front_door_weekly(week_start, week_end, fd_counts)

    # 5) Engaged tiers for the week
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
        "engaged3": tiers.get(3, 0),  # serve not yet wired
    }

    # 6) Lapses & no-longer-attends
    #    First: write/refresh lapse events & dropouts for this week (side-effects in DB)
    detect_and_write_lapses_for_week(
        week_end,
        signals=("attend","give","serve"),
        inactivity_days_for_drop=180,
    )

    #    Then: shape the response the way you want
    new_lapsed_total_people, new_lapsed_by_signal, newly_lapsed_items = _fetch_newly_lapsed_aggregate(week_end)
    total_lapsed_people = _count_total_lapsed_people(lapse_threshold=3)

    nla_added_items = _fetch_newly_no_longer_attends(week_end, inactivity_days=180)


    # 7) Average adult attendance (last 4 Sundays)
    avg4 = _avg_adult_attendance_last4(week_end)

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
            "lapses": {
                # show only “newly lapsed this week” + a total rollup
                "new_this_week_total": new_lapsed_total_people,   # unique people newly lapsed this week
                "new_by_signal": new_lapsed_by_signal,            # e.g. {"attend": 118, "give": 9, "serve": 0}
                "total_lapsed_people": total_lapsed_people,       # unique people currently lapsed in any signal
                "items": newly_lapsed_items,                      # ONLY those newly lapsed this week (detailed)
            },
            "no_longer_attends": {
                # show only “added this week” and their details
                "added_this_week": len(nla_added_items),
                "items": nla_added_items,                         # has first_seen_any + per-signal last dates
            },
            "adult_attendance_avg_4w": avg4,
            "notes": [
                "Cadences computed over a rolling 180-day window; low samples (<4) => irregular.",
                "Attendance lapses count only if Engaged tier == 0 AND household still has a child <14 (InsideOut excluded).",
                "Lapsed = missed >= 3 cycles; items list only the newly lapsed this week.",
                "“No longer attends” = no engagement for 180 days; list shows only those added this week.",
                "Serving cadence/buckets will populate once serving signals are wired.",
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

