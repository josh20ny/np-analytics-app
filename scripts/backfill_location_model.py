#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import List, Tuple, Optional

import httpx
from tqdm import tqdm


DEFAULT_BASE_URL = "http://localhost:8000"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill location-model check-ins via local API.")
    p.add_argument("--start", required=True, help="Inclusive start date (YYYY-MM-DD)")
    p.add_argument("--end", required=True, help="Exclusive end date (YYYY-MM-DD)")
    p.add_argument("--event-id", default="522222", help="PCO event id to sync locations for")
    # Backward compatible flags
    p.add_argument("--sync-locations-first", action="store_true",
                   help="Sync locations once before processing any days (compat with older script)")
    p.add_argument("--sync-locations", choices=["never", "once", "sundays", "daily"], default="once",
                   help="How often to call /sync-locations: never|once|sundays|daily (default: once)")
    # New conveniences (aliases)
    p.add_argument("--only-sundays", action="store_true", help="Process only Sundays in the date range")
    p.add_argument("--resync-locations-each-sunday", action="store_true",
                   help="Alias for --sync-locations sundays")
    p.add_argument("--skip-empty", action="store_true",
                   help="Skip days that appear to have no check-ins (best-effort; server will already noop fast).")
    p.add_argument("--max-splits", type=int, default=8,
                   help="Maximum recursive splits of a day's window when the server returns 5xx (default: 8).")
    p.add_argument("--sleep-between", type=float, default=0.0,
                   help="Seconds to sleep between days (default: 0).")
    p.add_argument("--base-url", default=DEFAULT_BASE_URL,
                   help=f"Base URL of your FastAPI server (default: {DEFAULT_BASE_URL})")
    # Accepted but ignored (so your old command lines continue to work)
    p.add_argument("--per-page", type=int, default=None,
                   help="Accepted for compatibility; ignored. Page sizing is handled server-side.")
    return p.parse_args()


def iso_utc_start_of_day(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def daterange(start: date, end: date) -> List[date]:
    days: List[date] = []
    cur = start
    while cur < end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def filter_sundays(days: List[date]) -> List[date]:
    # Monday=0 ... Sunday=6
    return [d for d in days if d.weekday() == 6]


@dataclass
class BackfillConfig:
    base_url: str
    event_id: str
    sync_mode: str  # "never" | "once" | "sundays" | "daily"
    max_splits: int
    sleep_between: float
    skip_empty: bool


async def call_sync_locations(client: httpx.AsyncClient, cfg: BackfillConfig) -> dict:
    url = f"{cfg.base_url}/checkins-location/sync-locations/{cfg.event_id}"
    r = await client.post(url, timeout=120)
    r.raise_for_status()
    return r.json()


async def call_ingest_range(
    client: httpx.AsyncClient,
    cfg: BackfillConfig,
    day: date,
    t0: datetime,
    t1: datetime,
    splits_left: int,
) -> None:
    """
    Try to ingest [t0,t1) for a given day, splitting on 5xx up to splits_left.
    """
    params = {
        "created_at_gte": t0.isoformat().replace("+00:00", "Z"),
        "created_at_lte": t1.isoformat().replace("+00:00", "Z"),
    }
    url = f"{cfg.base_url}/checkins-location/ingest-day/{day.isoformat()}"
    try:
        r = await client.post(url, params=params, timeout=180)
        if r.status_code >= 500:
            raise httpx.HTTPStatusError("server error", request=r.request, response=r)
        r.raise_for_status()
        return
    except (httpx.HTTPError) as e:
        if splits_left <= 0:
            raise
        # split the range and recurse
        mid = t0 + (t1 - t0) / 2
        await call_ingest_range(client, cfg, day, t0, mid, splits_left - 1)
        await call_ingest_range(client, cfg, day, mid, t1, splits_left - 1)


async def process_day(
    client: httpx.AsyncClient,
    cfg: BackfillConfig,
    day: date,
    resync_today: bool,
) -> Tuple[bool, Optional[str]]:
    """
    Process one day. Returns (ok, error_message).
    """
    try:
        if resync_today and cfg.sync_mode != "never":
            await call_sync_locations(client, cfg)

        # Build [gte, lte) for the day
        t0 = iso_utc_start_of_day(day)
        t1 = iso_utc_start_of_day(day + timedelta(days=1))

        await call_ingest_range(client, cfg, day, t0, t1, cfg.max_splits)
        return True, None
    except Exception as e:
        return False, str(e)


async def main_async() -> None:
    args = parse_args()

    # Normalize flags
    sync_mode = args.sync_locations
    if args.resync_locations_each_sunday:
        sync_mode = "sundays"

    cfg = BackfillConfig(
        base_url=args.base_url.rstrip("/"),
        event_id=args.event_id,
        sync_mode=sync_mode,
        max_splits=args.max_splits,
        sleep_between=args.sleep_between,
        skip_empty=args.skip_empty,
    )

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    days = daterange(start, end)
    if args.only_sundays:
        days = filter_sundays(days)

    async with httpx.AsyncClient() as client:
        # Optional one-time sync
        if args.sync_locations_first or cfg.sync_mode == "once":
            try:
                res = await call_sync_locations(client, cfg)
                print(f"synced locations for event {cfg.event_id} → {res}")
            except Exception as e:
                print(f"WARNING: initial sync-locations failed: {e}")

        # Iterate with progress bar
        it = tqdm(days, desc="Backfilling", unit="day")
        for d in it:
            resync_today = (
                (cfg.sync_mode == "daily") or
                (cfg.sync_mode == "sundays" and d.weekday() == 6)
            )

            ok, err = await process_day(client, cfg, d, resync_today)
            if ok:
                it.set_postfix_str(f"{d.isoformat()} ✓")
            else:
                it.set_postfix_str(f"{d.isoformat()} ✗")
                print(f"\nFailed {d.isoformat()}: {err}")

            if cfg.sleep_between:
                await asyncio.sleep(cfg.sleep_between)


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\nInterrupted.")


if __name__ == "__main__":
    main()