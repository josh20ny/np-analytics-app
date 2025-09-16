# ============================
# app/planning_center/checkins_location_model/rollup.py
# ============================
from __future__ import annotations
from typing import Optional
from datetime import date

import asyncpg

ROLLUP_DELETE = """
DELETE FROM attendance_by_location_daily
WHERE date = $1
"""

ROLLUP_INSERT = """
INSERT INTO attendance_by_location_daily (
  date, service_bucket, location_id, ministry_key, total_attendance, total_new
)
SELECT
  (starts_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago')::date AS svc_date,
  service_bucket,
  COALESCE(location_id, 'UNKNOWN') AS location_id,
  COALESCE(ministry_key, 'UNKNOWN') AS ministry_key,
  COUNT(*) AS total_attendance,
  SUM(CASE WHEN new_flag THEN 1 ELSE 0 END) AS total_new
FROM pco_checkins_raw
WHERE (starts_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago')::date = $1
GROUP BY 1,2,3,4
"""

async def rollup_day(conn: asyncpg.Connection, svc_date: date) -> None:
    await conn.execute(ROLLUP_DELETE, svc_date)
    await conn.execute(ROLLUP_INSERT, svc_date)