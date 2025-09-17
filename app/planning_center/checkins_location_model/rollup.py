# app/planning_center/checkins_location_model/rollup.py
from datetime import date
import asyncpg

# DELETE then INSERT for an idempotent rebuild of one local (America/Chicago) Sunday
ROLLUP_DELETE = """
DELETE FROM attendance_by_location_daily
WHERE date = $1::date
"""

ROLLUP_INSERT = """
INSERT INTO attendance_by_location_daily (
  date, service_bucket, location_id, ministry_key, total_attendance, total_new
)
SELECT
  $1::date                                   AS date,
  r.service_bucket                            AS service_bucket,
  COALESCE(r.location_id, 'UNKNOWN')          AS location_id,
  COALESCE(r.ministry_key, 'UNKNOWN')         AS ministry_key,
  COUNT(*)                                    AS total_attendance,
  SUM(CASE WHEN r.new_flag THEN 1 ELSE 0 END) AS total_new
FROM pco_checkins_raw r
WHERE (r.starts_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago')::date = $1::date
GROUP BY r.service_bucket,
         COALESCE(r.location_id, 'UNKNOWN'),
         COALESCE(r.ministry_key, 'UNKNOWN')
"""

async def rollup_day(conn: asyncpg.Connection, svc_date: date) -> int:
    """Rebuild the rollup rows for a given local date. Returns row count inserted."""
    await conn.execute(ROLLUP_DELETE, svc_date)
    await conn.execute(ROLLUP_INSERT, svc_date)
    row = await conn.fetchrow(
        "SELECT COUNT(*) AS n FROM attendance_by_location_daily WHERE date = $1::date",
        svc_date,
    )
    return int(row["n"] or 0)
