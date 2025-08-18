# scripts/backfill_serving.py
"""
Backfill weekly serving rollups into serving_volunteers_weekly.

Usage examples:
  # Backfill a fixed date range (inclusive on Sundays)
  python -m scripts.backfill_serving --start 2024-01-01 --end 2025-08-10

  # Backfill the last N Sundays (ending at last Sunday CST)
  python -m scripts.backfill_serving --weeks 52

  # Discover the earliest first_joined_at among *curated* serving teams and backfill
  python -m scripts.backfill_serving --from-first

Notes:
  - Uses the exact curated mapping in app.planning_center.serving
  - Counts are computed *as of* each Sunday (first_joined_at <= date < archived_at)
  - Idempotent: UPSERT overwrites any existing row for that week_end
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from typing import Iterable, List, Optional, Tuple

from app.db import get_conn
from app.planning_center import serving as srv


# ────────────────────────────────────────────────────────────────────────────────
# Date helpers (CST-style last Sunday, Sunday iteration)
# ────────────────────────────────────────────────────────────────────────────────

def last_sunday(today: Optional[date] = None) -> date:
    d = today or datetime.now().date()
    # Monday=0..Sunday=6 → distance to last Sunday
    return d - timedelta(days=((d.weekday() + 1) % 7))


def snap_to_sunday(d: date) -> date:
    # advance to the next Sunday if not already a Sunday
    return d + timedelta(days=(6 - d.weekday()))


def iter_sundays(start: date, end: date) -> Iterable[date]:
    """Yield Sundays from start..end inclusive. start/end can be any day."""
    s = snap_to_sunday(start)
    e = snap_to_sunday(end)
    cur = s
    while cur <= e:
        yield cur
        cur = cur + timedelta(days=7)


# ────────────────────────────────────────────────────────────────────────────────
# Earliest curated serving membership date (for --from-first)
# ────────────────────────────────────────────────────────────────────────────────

def earliest_curated_serving_date() -> Optional[date]:
    """Scan memberships and return the earliest first_joined_at for curated teams.

    We fetch minimal columns and filter via the same classifier used by summary to
    avoid SQL duplication of the curated list.
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT m.first_joined_at::date AS joined_on, g.group_type, g.name
            FROM f_groups_memberships m
            JOIN pco_groups g ON g.group_id = m.group_id
            WHERE m.first_joined_at IS NOT NULL
            """
        )
        earliest: Optional[date] = None
        for joined_on, gt, name in cur.fetchall():
            cats = srv._classify_categories(gt, name)
            if not cats:
                continue
            if joined_on is None:
                continue
            if earliest is None or joined_on < earliest:
                earliest = joined_on
        return earliest
    finally:
        cur.close(); conn.close()


# ────────────────────────────────────────────────────────────────────────────────
# Upsert helper (reuses serving module's UPSERT to keep logic in one place)
# ────────────────────────────────────────────────────────────────────────────────

def upsert_week(week_end: date) -> Tuple[int, dict]:
    total, by_cat = srv._serving_counts_by_category(week_end)
    srv._upsert_serving_weekly(week_end, total, by_cat)
    return total, by_cat


# ────────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────────

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Backfill serving weekly rollups")
    ap.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    ap.add_argument("--end", type=str, help="End date (YYYY-MM-DD); default last Sunday")
    ap.add_argument("--weeks", type=int, help="Alternatively, backfill the last N Sundays")
    ap.add_argument("--from-first", action="store_true", help="Backfill from earliest curated membership")
    ap.add_argument("--dry-run", action="store_true", help="Compute but do not write")
    ap.add_argument("--verbose", action="store_true", help="Print per-week details")

    args = ap.parse_args(argv)

    # Determine range
    if args.weeks and (args.start or args.end or args.from_first):
        ap.error("--weeks is exclusive with --start/--end/--from-first")
    if args.from_first and (args.start or args.weeks):
        ap.error("--from-first is exclusive with --start and --weeks")

    end_dt = date.fromisoformat(args.end) if args.end else last_sunday()

    if args.weeks:
        start_dt = end_dt - timedelta(days=7*(args.weeks-1))
    elif args.from_first:
        first = earliest_curated_serving_date()
        if not first:
            print("No curated serving memberships found.")
            return 0
        start_dt = first
    elif args.start:
        start_dt = date.fromisoformat(args.start)
    else:
        ap.error("Provide one of --weeks, --from-first, or --start (with optional --end)")

    print(f"Backfilling serving from {start_dt} to {end_dt} (Sundays inclusive)")

    count_weeks = 0
    for sunday in iter_sundays(start_dt, end_dt):
        total, by_cat = srv._serving_counts_by_category(sunday)
        if args.verbose:
            print(f"  {sunday}: total={total} by_cat={by_cat}")
        if not args.dry_run:
            srv._upsert_serving_weekly(sunday, total, by_cat)
        count_weeks += 1

    print(f"Done. Processed {count_weeks} Sundays.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
