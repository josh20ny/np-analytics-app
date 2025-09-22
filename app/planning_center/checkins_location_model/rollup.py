from __future__ import annotations
from asyncpg import Connection
from datetime import date as _date

async def rollup_day(conn: Connection, d: _date) -> int:
    """
    Set-based rollup for a Sunday (CST). For each person within a ministry+service,
    pick the DEEPEST location (leaf-most) to avoid double-counting parents (e.g., "Transit",
    "9:30 Service", "Upstreet & WaumbaLand"). Write per-location counts into
    attendance_by_location_daily and return the number of rows upserted.
    """
    rows = await conn.fetch(
        """
        WITH day_rows AS (
          SELECT
            pr.person_id::text                AS person_id,
            ($1)::date                        AS svc_date,
            pr.ministry_key::text             AS ministry_raw,
            pr.service_bucket::text           AS service_raw,
            pr.location_id                    AS location_id,
            pr.created_at_pco                 AS created_at_pco_utc,
            pr.person_created_at              AS person_created_at_utc
          FROM pco_checkins_raw pr
          WHERE (pr.created_at_pco AT TIME ZONE 'America/Chicago')::date = $1::date
            AND pr.person_id IS NOT NULL
            AND trim(pr.person_id::text) <> ''
            AND pr.ministry_key IS NOT NULL
            AND pr.service_bucket IS NOT NULL
            AND pr.location_id IS NOT NULL
        ),
        normalized AS (
          SELECT
            person_id,
            svc_date,
            CASE
              WHEN ministry_raw IN ('Waumba Land','WaumbaLand','Waumba land','WaumbaLand ') THEN 'Waumba Land'
              WHEN ministry_raw IN ('Upstreet','UpStreet','Up Street')                       THEN 'UpStreet'
              WHEN ministry_raw IN ('Transit ','Transit')                                    THEN 'Transit'
              WHEN ministry_raw IN ('Inside Out','InsideOut','InsideOut ')                   THEN 'InsideOut'
              ELSE ministry_raw
            END AS ministry_key,
            CASE
              WHEN service_raw IN ('930','9:30','9:30AM','9:30 am','09:30','09:30 AM')       THEN '9:30 AM'
              WHEN service_raw IN ('1100','11:00','11:00AM','11:00 am')                      THEN '11:00 AM'
              WHEN service_raw IN ('1630','4:30','4:30PM','4:30 pm','16:30','16:30 PM')     THEN '4:30 PM'
              ELSE service_raw
            END AS service_bucket,
            location_id,
            created_at_pco_utc,
            person_created_at_utc
          FROM day_rows
        ),

        -- Precompute depth for all locations (via closure table).
        -- depth_cte: one row per location with its maximum depth value.
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
              PARTITION BY svc_date, person_id, ministry_key, service_bucket
              ORDER BY COALESCE(d.depth, 0) DESC, created_at_pco_utc ASC
            ) AS rn
          FROM normalized n
          LEFT JOIN depth_cte d
            ON d.location_id = n.location_id
        ),

        -- one row per person per ministry+service: the DEEPEST location they touched
        dedup AS (
          SELECT
            svc_date,
            ministry_key,
            service_bucket,
            location_id,
            (created_at_pco_utc    AT TIME ZONE 'America/Chicago')::date AS chk_date_local,
            (person_created_at_utc AT TIME ZONE 'America/Chicago')::date AS person_created_local
          FROM ranked
          WHERE rn = 1
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
        ON CONFLICT (date, ministry_key, service_bucket, location_id)
        DO UPDATE SET
          total_attendance = EXCLUDED.total_attendance,
          total_new        = EXCLUDED.total_new
        RETURNING 1
        """,
        d,
    )
    return len(rows)
