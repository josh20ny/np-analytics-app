# backfill_checkins.py

import requests
import time
from datetime import date, timedelta
from tqdm import tqdm

BASE_URL = "http://localhost:8000"
ENDPOINT = "/planning-center/checkins"

# Adjust this to however far back you want to backfill:
START_DATE = date(2023, 1, 1)


def get_previous_sundays(start_date):
    sundays = []
    while start_date >= START_DATE:
        sundays.append(start_date)
        start_date -= timedelta(days=7)
    return sundays


def main():
    today = date.today()
    most_recent_sunday = date(2025, 7, 27) #today - timedelta(days=(today.weekday() + 1) % 7)
    sundays = get_previous_sundays(most_recent_sunday)

    print(f"\n🚀 Starting backfill from {most_recent_sunday} to {START_DATE} ({len(sundays)} Sundays total)\n")

    for d in tqdm(sundays, desc="Backfilling Sundays", unit="week"):
        try:
            resp = requests.get(f"{BASE_URL}{ENDPOINT}", params={"date": d.isoformat()}, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                if "checkins_count" in data:
                    if data["checkins_count"] == 0:
                        tqdm.write(f"⚠️ No checkins found for {d}.")
                        continue
                    tqdm.write(f"✅ Success for {d}: {data['checkins_count']} check-ins processed.")
                else:
                    tqdm.write(f"⚠️ Response missing 'checkins_count' key — {data}")
            else:
                tqdm.write(f"❌ HTTP Error {resp.status_code} for {d}: {resp.text}")
                break
        except Exception as e:
            tqdm.write(f"❌ Exception on {d}: {e}")
            break

        time.sleep(1.5)  # Buffer between requests to avoid rate-limiting

    print("\n🎉 Backfill complete.")


if __name__ == "__main__":
    main()

