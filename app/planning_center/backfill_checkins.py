import requests
import time
from datetime import date, timedelta
from tqdm import tqdm
import logging

BASE_URL = "http://localhost:8000"
ENDPOINT = "/planning-center/checkins"

# Adjust this to however far back you want to backfill:
START_DATE = date(2023, 1, 1)

# Logging setup (timestamped INFO lines)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill_checkins")

SLEEP_BETWEEN = 1.5     # seconds between calls
MAX_RETRIES    = 3      # simple retry for transient HTTP errors
RETRY_BACKOFF  = 2.0    # seconds (multiplied per retry)


def get_previous_sundays(start_date):
    sundays = []
    while start_date >= START_DATE:
        sundays.append(start_date)
        start_date -= timedelta(days=7)
    return sundays


def call_day(d: date) -> dict:
    """Call the API with minimal retries; return JSON or error dict."""
    url = f"{BASE_URL}{ENDPOINT}"

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        t0 = time.perf_counter()
        try:
            resp = requests.get(
                url,
                params={"date": d.isoformat(), "write_person_facts": "true", "log_person_facts": "true"},
                timeout=60,
            )
            dur = time.perf_counter() - t0
            if resp.status_code == 200:
                data = resp.json()
                data["_elapsed_s"] = round(dur, 2)
                data["_status"] = 200
                return data
            else:
                last_err = f"HTTP {resp.status_code}: {resp.text[:200]}"
                log.warning("Day %s attempt %s failed in %.2fs → %s", d, attempt, dur, last_err)
        except Exception as e:
            dur = time.perf_counter() - t0
            last_err = f"Exception: {e}"
            log.warning("Day %s attempt %s raised in %.2fs → %s", d, attempt, dur, e)

        time.sleep(RETRY_BACKOFF * attempt)

    return {"status": "error", "error": last_err or "unknown error", "_status": 0}


def main():
    today = date.today()
    most_recent_sunday = date(2025, 8, 3)  # today - timedelta(days=(today.weekday() + 1) % 7)
    sundays = get_previous_sundays(most_recent_sunday)

    log.info("🚀 Starting backfill from %s to %s (%s Sundays total)", most_recent_sunday, START_DATE, len(sundays))

    total_person_facts = 0
    total_checkins = 0
    ok_days = 0
    err_days = 0
    t_start = time.perf_counter()

    for d in tqdm(sundays, desc="Backfilling Sundays", unit="week"):
        res = call_day(d)

        elapsed = res.get("_elapsed_s")
        status  = res.get("_status")
        tqdm.write(f"→ {d} [{status or 'ERR'}]{'' if elapsed is None else f' ({elapsed}s)'}")

        if res.get("status") == "success":
            chk = int(res.get("checkins_count", 0) or 0)
            pfa = int(res.get("person_facts_attempted", 0) or 0)
            pfi = int(res.get("person_facts_inserted", 0) or 0)
            suffix = f" (person-facts attempted={pfa}, affected={pfi})"

            total_checkins += chk
            total_person_facts += pfi
            ok_days += 1

            if chk == 0:
                tqdm.write(f"⚠️  No checkins found for {d}.")
            else:
                tqdm.write(f"✅ {d}: {chk} check-ins processed{suffix}.")
        else:
            err_days += 1
            tqdm.write(f"❌ {d}: {res.get('error') or res}")

        time.sleep(SLEEP_BETWEEN)

    t_total = time.perf_counter() - t_start
    log.info(
        "🎉 Backfill complete. Days OK=%s, errors=%s, person-facts=%s, checkins=%s, elapsed=%.1fs",
        ok_days, err_days, total_person_facts, total_checkins, t_total
    )


if __name__ == "__main__":
    main()
