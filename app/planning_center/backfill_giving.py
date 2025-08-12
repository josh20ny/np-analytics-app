# backfill_giving.py

import requests
import time
from datetime import date, timedelta
from tqdm import tqdm
import argparse
import logging

# Defaults (override with CLI flags as needed)
BASE_URL = "http://localhost:8000"
ENDPOINT = "/planning-center/giving/weekly-summary"

# Go back to this (Monday) inclusive. Adjust as needed.
START_WEEK = date(2022, 1, 3)  # first Monday of 2022

# Gentle pacing to avoid rate limits
REQUEST_SLEEP_SECS = 1.0
TIMEOUT_SECS = 60
MAX_RETRIES = 3
RETRY_BACKOFF = 1.5  # seconds, multiplied by attempt number

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill_giving")

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})


def last_full_week_monday_sunday() -> tuple[date, date]:
    """
    Most recent completed Mon..Sun in local server time (naive dates).
    Monday = 0 ... Sunday = 6
    """
    today = date.today()
    last_sunday = today - timedelta(days=(today.weekday() + 1) % 7)
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


def call_week(base_url: str, wk_start: date, wk_end: date, mode: str, timeout: int) -> dict:
    """
    Call the API with minimal retries; return JSON or error dict.
    """
    url = f"{base_url}{ENDPOINT}"
    params = {
        "start": wk_start.isoformat(),
        "end": wk_end.isoformat(),
        "mode": mode,
        "debug": "false",
        # If you later add a 'replace_week' toggle on the API, you can send it here:
        # "replace_week": "true",
    }

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        t0 = time.perf_counter()
        try:
            resp = SESSION.get(url, params=params, timeout=timeout)
            dur = time.perf_counter() - t0
            if resp.status_code == 200:
                data = resp.json()
                data["_elapsed_s"] = round(dur, 2)
                data["_status"] = 200
                return data
            else:
                last_err = f"HTTP {resp.status_code}: {resp.text[:250]}"
                log.warning("Week %s..%s attempt %s failed in %.2fs → %s", wk_start, wk_end, attempt, dur, last_err)
        except Exception as e:
            dur = time.perf_counter() - t0
            last_err = f"Exception: {e}"
            log.warning("Week %s..%s attempt %s raised in %.2fs → %s", wk_start, wk_end, attempt, dur, e)

        # Backoff before next try
        time.sleep(RETRY_BACKOFF * attempt)

    return {"status": "error", "error": last_err or "unknown error", "_status": 0}


def main():
    parser = argparse.ArgumentParser(description="Backfill Planning Center Giving weekly summaries.")
    parser.add_argument("--base-url", default=BASE_URL, help="API base URL (default: http://localhost:8000)")
    parser.add_argument("--start-week", default=START_WEEK.isoformat(),
                        help="Earliest Monday to include (YYYY-MM-DD). Default: %(default)s")
    parser.add_argument("--mode", choices=["gross", "net"], default="gross",
                        help='Total mode passed to API (default: "gross")')
    parser.add_argument("--sleep", type=float, default=REQUEST_SLEEP_SECS,
                        help="Seconds to sleep between requests (default: 1.0)")
    parser.add_argument("--timeout", type=int, default=TIMEOUT_SECS,
                        help="HTTP timeout seconds (default: 60)")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be requested; don’t call API.")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    start_monday = date.fromisoformat(args.start_week)

    # Most recent completed Mon..Sun
    last_mon, last_sun = last_full_week_monday_sunday()

    # Walk back from last_mon..last_sun to start_monday (inclusive)
    weeks = list(generate_weeks(last_mon, start_monday))
    log.info("🚀 Starting giving backfill from %s..%s back to %s (%s weeks total)",
             last_mon, last_sun, start_monday, len(weeks))

    total_weeks_ok = 0
    total_weeks_err = 0
    sum_totals = 0.0
    t_start = time.perf_counter()

    for wk_start, wk_end in tqdm(weeks, desc="Backfilling weeks", unit="week"):
        if args.dry_run:
            tqdm.write(f"[DRY RUN] Would GET {base_url}{ENDPOINT}?start={wk_start}&end={wk_end}&mode={args.mode}")
            continue

        res = call_week(base_url, wk_start, wk_end, args.mode, args.timeout)

        # status line w/ timing
        elapsed = res.get("_elapsed_s")
        status = res.get("_status")
        tqdm.write(f"→ {wk_start}..{wk_end} [{status or 'ERR'}]{'' if elapsed is None else f' ({elapsed}s)'}")

        if res.get("status") == "success":
            try:
                total = float(res.get("total_giving", 0.0) or 0.0)
            except Exception:
                total = 0.0
            units = int(res.get("giving_units", 0) or 0)
            sum_totals += total
            total_weeks_ok += 1
            tqdm.write(f"✅ {wk_start}..{wk_end}: total=${total:,.2f} | units={units}")
        else:
            total_weeks_err += 1
            tqdm.write(f"❌ {wk_start}..{wk_end}: {res.get('error') or res}")

        time.sleep(args.sleep)

    dur = time.perf_counter() - t_start
    log.info("🎉 Backfill complete. Weeks OK=%s, errors=%s, sum total=$%,.2f, elapsed=%.1fs",
             total_weeks_ok, total_weeks_err, sum_totals, dur)


if __name__ == "__main__":
    main()
