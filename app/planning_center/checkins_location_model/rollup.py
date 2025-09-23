from __future__ import annotations
from asyncpg import Connection
from datetime import date as _date
from typing import Literal

async def rollup_day(
    conn: Connection,
    d: _date,
    dedupe_scope: Literal["service", "day"] = "service",
) -> int:
    """
    Set-based rollup for a Sunday (CST). For each person within a ministry+service,
    pick the deepest (leaf-most) location to avoid double-counting parents.
    Upserts into attendance_by_location_daily and returns rows upserted.
    """
    partition = (
        "svc_date, person_id, ministry_key, service_bucket"  # capacity: count per service
        if dedupe_scope == "service"
        else "svc_date, person_id, ministry_key"             # unique per day (optional view)
    )

    sql = """
    WITH day_rows AS (
      SELECT
        pr.person_id,
        (pr.created_at_pco AT TIME ZONE 'America/Chicago')::date AS svc_date,
        pr.ministry_key           AS ministry_raw,
        pr.service_bucket         AS service_raw,
        pr.location_id,
        pr.created_at_pco         AS created_at_pco_utc,
        pr.person_created_at      AS person_created_at_utc
      FROM pco_checkins_raw pr
      WHERE (pr.created_at_pco AT TIME ZONE 'America/Chicago')::date = $1::date
        AND pr.person_id IS NOT NULL
        AND trim(pr.person_id::text) <> ''
        AND pr.service_bucket IS NOT NULL
        AND trim(pr.service_bucket::text) <> ''
        AND pr.location_id IS NOT NULL
    ),
    normalized AS (
      SELECT
        dr.person_id,
        dr.svc_date,
        /* 1) normalize if present; 2) else derive from location name */
        COALESCE(
          CASE
            WHEN dr.ministry_raw IN ('Waumba Land','WaumbaLand','Waumba land','WaumbaLand ') THEN 'Waumba Land'
            WHEN dr.ministry_raw IN ('Upstreet','UpStreet','Up Street')                       THEN 'UpStreet'
            WHEN dr.ministry_raw IN ('Transit ','Transit')                                    THEN 'Transit'
            WHEN dr.ministry_raw IN ('Inside Out','InsideOut','InsideOut ')                   THEN 'InsideOut'
            ELSE NULL
          END,
          CASE
            WHEN lower(pl.name) LIKE '%waumba%' THEN 'Waumba Land'
            WHEN lower(pl.name) LIKE '%upstreet%' OR lower(pl.name) LIKE 'up street%' THEN 'UpStreet'
            WHEN lower(pl.name) LIKE '%transit%' THEN 'Transit'
            WHEN lower(pl.name) LIKE '%insideout%' OR lower(pl.name) LIKE '%inside out%' THEN 'InsideOut'
            ELSE NULL
          END
        ) AS ministry_key,
        CASE
          WHEN dr.service_raw IN ('930','9:30','9:30AM','9:30 am','09:30','09:30 AM')       THEN '9:30 AM'
          WHEN dr.service_raw IN ('1100','11:00','11:00AM','11:00 am')                      THEN '11:00 AM'
          WHEN dr.service_raw IN ('1630','4:30','4:30PM','4:30 pm','16:30','16:30 PM')     THEN '4:30 PM'
          ELSE dr.service_raw
        END AS service_bucket,
        dr.location_id,
        dr.created_at_pco_utc,
        dr.person_created_at_utc
      FROM day_rows dr
      LEFT JOIN pco_locations pl
        ON pl.location_id = dr.location_id
    ),
    -- Precompute depth for all locations (via closure table).
    depth_cte AS (
      SELECT descendant_id AS location_id, MAX(depth) AS depth
      FROM pco_location_paths
      GROUP BY descendant_id
    ),
    ranked AS (
      SELECT
        n.*,
        COALESCE(d.depth, 0) AS location_depth,
        ROW_NUMBER() OVER (
          PARTITION BY __PARTITION__
          ORDER BY COALESCE(d.depth, 0) DESC, created_at_pco_utc ASC
        ) AS rn
      FROM normalized n
      LEFT JOIN depth_cte d ON d.location_id = n.location_id
    ),
    dedup AS (
      SELECT
        svc_date,
        ministry_key,
        service_bucket,
        location_id,
        (created_at_pco_utc     AT TIME ZONE 'America/Chicago')::date AS chk_date_local,
        (person_created_at_utc  AT TIME ZONE 'America/Chicago')::date AS person_created_local
      FROM ranked
      WHERE rn = 1
        AND ministry_key IS NOT NULL
    ),
    agg AS (
      SELECT
        svc_date::date              AS date,
        ministry_key,
        service_bucket,
        location_id,
        COUNT(*)                    AS total_attendance,
        COUNT(*) FILTER (
          WHERE person_created_local IS NOT NULL
            AND chk_date_local = person_created_local
        )                           AS total_new
      FROM dedup
      GROUP BY 1,2,3,4
    )
    INSERT INTO attendance_by_location_daily
      (date, ministry_key, service_bucket, location_id, total_attendance, total_new)
    SELECT date, ministry_key, service_bucket, location_id, total_attendance, total_new
    FROM agg
    ON CONFLICT (date, service_bucket, location_id, ministry_key)
    DO UPDATE SET
      total_attendance = EXCLUDED.total_attendance,
      total_new        = EXCLUDED.total_new
    RETURNING 1
    """.replace("__PARTITION__", partition)

    rows = await conn.fetch(sql, d)  # $1 is svc_date
    return len(rows)
