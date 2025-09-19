# ============================
# app/planning_center/checkins_location_model/rollup.py
# ============================
from __future__ import annotations

from datetime import date
import asyncpg

# Heuristic new = profile created same day as service (CST)
# (Later we can swap to cadence person-facts without changing callers.)

ROLLUP_DELETE = """
DELETE FROM attendance_by_location_daily
WHERE date = $1::date;
"""

ROLLUP_INSERT = """
WITH params AS (
  SELECT $1::date AS svc_date
), raw AS (
  SELECT *
  FROM pco_checkins_raw r
  JOIN params p ON TRUE
  WHERE (r.created_at_pco AT TIME ZONE 'America/Chicago')::date = p.svc_date
), placed AS (
  -- place each checkin on its room and all ancestor nodes
  SELECT
    (raw.created_at_pco AT TIME ZONE 'America/Chicago')::date AS date,
    COALESCE(NULLIF(raw.service_bucket, ''),
             CASE
               WHEN date_part('hour', raw.starts_at AT TIME ZONE 'America/Chicago') = 9 THEN '9:30 AM'
               WHEN date_part('hour', raw.starts_at AT TIME ZONE 'America/Chicago') = 11 THEN '11:00 AM'
               WHEN date_part('hour', raw.starts_at AT TIME ZONE 'America/Chicago') IN (16,17) THEN '4:30 PM'
               ELSE ''
             END) AS service_bucket,
    path.ancestor_id AS location_id,
    COALESCE(NULLIF(raw.ministry_key, ''), 'UNKNOWN') AS ministry_key,
    raw.person_id,
    raw.person_created_at
  FROM raw
  JOIN pco_location_paths path
    ON path.descendant_id = raw.location_id
), first_time AS (
  SELECT
    p.date,
    p.service_bucket,
    p.location_id,
    p.ministry_key,
    p.person_id,
    CASE
      WHEN p.person_created_at IS NULL THEN FALSE
      ELSE ((p.person_created_at AT TIME ZONE 'America/Chicago')::date = p.date)
    END AS is_new
  FROM placed p
)
INSERT INTO attendance_by_location_daily (
  date, service_bucket, location_id, ministry_key, total_attendance, total_new
)
SELECT
  date,
  NULLIF(service_bucket,'') AS service_bucket,
  location_id,
  ministry_key,
  COUNT(*) AS total_attendance,
  SUM(CASE WHEN is_new THEN 1 ELSE 0 END) AS total_new
FROM first_time
GROUP BY 1,2,3,4
ORDER BY 1,2,4,3;
"""

async def rollup_day(conn: asyncpg.Connection, svc_date: date) -> int:
    """Delete then insert rollups for a given date. Returns rows inserted."""
    del_tag = await conn.execute(ROLLUP_DELETE, svc_date)
    try:
        _ = int(str(del_tag).split()[-1])
    except Exception:
        pass

    ins_tag = await conn.execute(ROLLUP_INSERT, svc_date)
    try:
        return int(str(ins_tag).split()[-1])
    except Exception:
        return 0
