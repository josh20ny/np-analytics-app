from __future__ import annotations
from asyncpg import Connection
from datetime import date as _date

UP = "UpStreet"
WL = "Waumba Land"
TR = "Transit"
IO = "InsideOut"

S930 = "9:30 AM"
S1100 = "11:00 AM"
S1630 = "4:30 PM"

async def write_legacy_slim(conn: Connection, d: _date) -> None:
    """
    Populate legacy slim tables from attendance_by_location_daily for date d.
    Tables (exact schemas):
      - upstreet_attendance(date PK, attendance_930, attendance_1100, total_attendance,
                            new_kids_930, new_kids_1100, total_new_kids)
      - waumbaland_attendance( same columns as upstreet_attendance )
      - transit_attendance(   same columns as upstreet_attendance )
      - insideout_attendance(date PK, total_attendance, new_students)
    All values default to 0 if no rows exist for a bucket. Upsert by date.
    """

    # UpStreet
    await conn.execute(
        """
        INSERT INTO upstreet_attendance
            (date, attendance_930, attendance_1100, total_attendance,
             new_kids_930, new_kids_1100, total_new_kids)
        VALUES (
            $1,
            COALESCE((SELECT SUM(total_attendance) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2 AND service_bucket=$3), 0),
            COALESCE((SELECT SUM(total_attendance) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2 AND service_bucket=$4), 0),
            COALESCE((SELECT SUM(total_attendance) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2), 0),
            COALESCE((SELECT SUM(total_new) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2 AND service_bucket=$3), 0),
            COALESCE((SELECT SUM(total_new) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2 AND service_bucket=$4), 0),
            COALESCE((SELECT SUM(total_new) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2), 0)
        )
        ON CONFLICT (date) DO UPDATE SET
            attendance_930   = EXCLUDED.attendance_930,
            attendance_1100  = EXCLUDED.attendance_1100,
            total_attendance = EXCLUDED.total_attendance,
            new_kids_930     = EXCLUDED.new_kids_930,
            new_kids_1100    = EXCLUDED.new_kids_1100,
            total_new_kids   = EXCLUDED.total_new_kids
        """,
        d, UP, S930, S1100,
    )

    # WaumbaLand
    await conn.execute(
        """
        INSERT INTO waumbaland_attendance
            (date, attendance_930, attendance_1100, total_attendance,
             new_kids_930, new_kids_1100, total_new_kids)
        VALUES (
            $1,
            COALESCE((SELECT SUM(total_attendance) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2 AND service_bucket=$3), 0),
            COALESCE((SELECT SUM(total_attendance) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2 AND service_bucket=$4), 0),
            COALESCE((SELECT SUM(total_attendance) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2), 0),
            COALESCE((SELECT SUM(total_new) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2 AND service_bucket=$3), 0),
            COALESCE((SELECT SUM(total_new) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2 AND service_bucket=$4), 0),
            COALESCE((SELECT SUM(total_new) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2), 0)
        )
        ON CONFLICT (date) DO UPDATE SET
            attendance_930   = EXCLUDED.attendance_930,
            attendance_1100  = EXCLUDED.attendance_1100,
            total_attendance = EXCLUDED.total_attendance,
            new_kids_930     = EXCLUDED.new_kids_930,
            new_kids_1100    = EXCLUDED.new_kids_1100,
            total_new_kids   = EXCLUDED.total_new_kids
        """,
        d, WL, S930, S1100,
    )

    # Transit
    await conn.execute(
        """
        INSERT INTO transit_attendance
            (date, attendance_930, attendance_1100, total_attendance,
             new_kids_930, new_kids_1100, total_new_kids)
        VALUES (
            $1,
            COALESCE((SELECT SUM(total_attendance) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2 AND service_bucket=$3), 0),
            COALESCE((SELECT SUM(total_attendance) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2 AND service_bucket=$4), 0),
            COALESCE((SELECT SUM(total_attendance) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2), 0),
            COALESCE((SELECT SUM(total_new) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2 AND service_bucket=$3), 0),
            COALESCE((SELECT SUM(total_new) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2 AND service_bucket=$4), 0),
            COALESCE((SELECT SUM(total_new) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2), 0)
        )
        ON CONFLICT (date) DO UPDATE SET
            attendance_930   = EXCLUDED.attendance_930,
            attendance_1100  = EXCLUDED.attendance_1100,
            total_attendance = EXCLUDED.total_attendance,
            new_kids_930     = EXCLUDED.new_kids_930,
            new_kids_1100    = EXCLUDED.new_kids_1100,
            total_new_kids   = EXCLUDED.total_new_kids
        """,
        d, TR, S930, S1100,
    )

    # InsideOut (single service 4:30 PM)
    await conn.execute(
        """
        INSERT INTO insideout_attendance
            (date, total_attendance, new_students)
        VALUES (
            $1,
            COALESCE((SELECT SUM(total_attendance) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2), 0),
            COALESCE((SELECT SUM(total_new) FROM attendance_by_location_daily
                      WHERE date=$1 AND ministry_key=$2), 0)
        )
        ON CONFLICT (date) DO UPDATE SET
            total_attendance = EXCLUDED.total_attendance,
            new_students     = EXCLUDED.new_students
        """,
        d, IO,
    )
