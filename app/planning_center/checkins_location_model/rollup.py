from __future__ import annotations
from asyncpg import Connection
from datetime import date as _date
from typing import Literal

async def rollup_day(
    conn: Connection,
    d: _date,
    dedupe_scope: Literal["service", "day"] = "service",
    log_duplicates: bool = True,
) -> int:
    """
    Set-based rollup for a Sunday (CST). For each person within a ministry+service,
    pick the deepest (leaf-most) location to avoid double-counting parents.
    If dedupe_scope='service', we log extra scans for the same person+ministry+service
    into pco_checkins_unplaced with reason_codes=['duplicate_same_ministry_service'].
    """
    # ---------- optional: log duplicate scans (service-level only) ----------
    if log_duplicates and dedupe_scope == "service":
        dup_sql = """
        WITH day_rows AS (
          SELECT
            pr.checkin_id,
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
            dr.checkin_id,
            dr.person_id,
            dr.svc_date,
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
          LEFT JOIN depth_cte d ON d.location_id = n.location_id
        ),
        dupes AS (
          SELECT
            n.svc_date,
            n.checkin_id    AS dropped_checkin_id,
            n.person_id,
            n.created_at_pco_utc,
            n.ministry_key,
            n.service_bucket,
            n.location_id    AS dropped_location_id,
            k.checkin_id     AS kept_checkin_id,
            k.location_id    AS kept_location_id
          FROM ranked n
          JOIN ranked k
            ON k.svc_date = n.svc_date
           AND k.person_id = n.person_id
           AND k.ministry_key = n.ministry_key
           AND k.service_bucket = n.service_bucket
           AND k.rn = 1
          WHERE n.rn > 1
        )
        INSERT INTO pco_checkins_unplaced
          (checkin_id, person_id, created_at_pco, reason_codes, details)
        SELECT
          d.dropped_checkin_id,
          d.person_id::text,
          d.created_at_pco_utc,
          ARRAY['duplicate_same_ministry_service'],
          jsonb_build_object(
            'svc_date', d.svc_date::text,
            'ministry_key', d.ministry_key,
            'service_bucket', d.service_bucket,
            'dropped_location_id', d.dropped_location_id,
            'kept_checkin_id', d.kept_checkin_id,
            'kept_location_id', d.kept_location_id
          )
        FROM dupes d
        ON CONFLICT (checkin_id) DO UPDATE
          SET reason_codes = (
                SELECT ARRAY(
                  SELECT DISTINCT x
                  FROM unnest(pco_checkins_unplaced.reason_codes || EXCLUDED.reason_codes) AS x
                )
              ),
              details = pco_checkins_unplaced.details || EXCLUDED.details;
        """
        await conn.execute(dup_sql, d)

    # ---------- main rollup upsert ----------
    partition = (
        "svc_date, person_id, ministry_key, service_bucket"  # capacity: dedupe within a service
        if dedupe_scope == "service"
        else "svc_date, person_id, ministry_key"             # unique per day (no cross-service dupes)
    )

    sql = """
    WITH day_rows AS (
      SELECT
        pr.checkin_id,
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
        dr.checkin_id,
        dr.person_id,
        dr.svc_date,
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

