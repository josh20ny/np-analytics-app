# backfill_giving.py

import requests
import time
from datetime import date, timedelta
from tqdm import tqdm
import argparse

# Defaults (override with CLI flags as needed)
BASE_URL = "http://localhost:8000"
ENDPOINT = "/planning-center/giving/weekly-summary"

# Go back to this (Monday) inclusive. Adjust as needed.
# Tip: set this to the first Monday you want to capture.
START_WEEK = date(2022, 1, 3)  # first Monday of 2022

# Gentle pacing to avoid rate limits
REQUEST_SLEEP_SECS = 1.5
TIMEOUT_SECS = 60


def last_full_week_monday_sunday() -> tuple[date, date]:
    """
    Returns the most recent completed Mon..Sun week as (monday, sunday),
    in local server time (naive dates).
    """
    today = date.today()
    # Find last Sunday (0 = Monday ... 6 = Sunday)
    last_sunday = today - timedelta(days=((today.weekday() + 1) % 7 or 7) - 1)
    # Corresponding Monday
    last_monday = last_sunday - timedelta(days=6)
    return last_monday, last_sunday


def generate_weeks(monday_end: date, stop_monday: date):
    """
    Yield (week_start, week_end) pairs stepping backwards Mon..Sun
    until week_start < stop_monday.
    """
    week_start, week_end = monday_end, monday_end + timedelta(days=6)
    while week_start >= stop_monday:
        yield week_start, week_end
        week_start -= timedelta(days=7)
        week_end -= timedelta(days=7)


def main():
    parser = argparse.ArgumentParser(description="Backfill Planning Center Giving weekly summaries.")
    parser.add_argument("--base-url", default=BASE_URL, help="API base URL (default: http://localhost:8000)")
    parser.add_argument("--start-week", default=START_WEEK.isoformat(),
                        help="Earliest Monday to include (YYYY-MM-DD). Default: %(default)s")
    parser.add_argument("--mode", choices=["gross", "net"], default="gross",
                        help='Total mode passed to API (default: "gross")')
    parser.add_argument("--sleep", type=float, default=REQUEST_SLEEP_SECS,
                        help="Seconds to sleep between requests (default: 1.5)")
    parser.add_argument("--timeout", type=int, default=TIMEOUT_SECS,
                        help="HTTP timeout seconds (default: 60)")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be requested, don’t call API.")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    start_week = date.fromisoformat(args.start_week)

    # Get the most recent completed Mon..Sun
    last_mon, last_sun = last_full_week_monday_sunday()

    # Align the generator to last_mon..last_sun and walk back to start_week
    weeks = list(generate_weeks(last_mon, start_week))
    print(f"\n🚀 Starting giving backfill from week {last_mon}..{last_sun} back to {start_week} "
          f"({len(weeks)} weeks total)\n")

    for wk_start, wk_end in tqdm(weeks, desc="Backfilling weeks", unit="week"):
        try:
            if args.dry_run:
                tqdm.write(f"[DRY RUN] Would GET {base_url}{ENDPOINT}?start={wk_start}&end={wk_end}&mode={args.mode}")
            else:
                resp = requests.get(
                    f"{base_url}{ENDPOINT}",
                    params={
                        "start": wk_start.isoformat(),
                        "end": wk_end.isoformat(),
                        "mode": args.mode,
                        "debug": "false",
                    },
                    timeout=args.timeout,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    total = data.get("total_giving")
                    units = data.get("giving_units")
                    tqdm.write(f"✅ {wk_start}..{wk_end} | total=${total:.2f} | units={units}")
                else:
                    tqdm.write(f"❌ HTTP {resp.status_code} for {wk_start}..{wk_end}: {resp.text}")
                    break
        except Exception as e:
            tqdm.write(f"❌ Exception on week {wk_start}..{wk_end}: {e}")
            break

        time.sleep(args.sleep)

    print("\n🎉 Giving backfill complete.")


if __name__ == "__main__":
    main()
