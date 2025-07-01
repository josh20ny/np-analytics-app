# scheduler.py

import os
import time
import logging

import schedule
import requests
from dotenv import load_dotenv

# ─── Load .env & Config ──────────────────────────────────────────────────────
load_dotenv()

# Base URL for your FastAPI app (override via .env if needed)
BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# ─── Generic API Caller ──────────────────────────────────────────────────────
def call_api(endpoint: str, label: str):
    """
    GETs BASE_URL + endpoint and logs success / failure.
    """
    url = f"{BASE_URL}{endpoint}"
    try:
        resp = requests.get(url, timeout=30)
        if resp.ok:
            logging.info(f"✅ {label} succeeded (status {resp.status_code})")
        else:
            logging.warning(f"⚠️ {label} returned {resp.status_code}: {resp.text}")
    except Exception as e:
        logging.error(f"❌ Exception during {label}: {e}", exc_info=True)


# ─── Job Schedule Definitions ────────────────────────────────────────────────
# time_str is HH:MM (24-hour), endpoint is your FastAPI path, label for logs
JOBS = [
    ("08:00", "/youtube/weekly-summary",  "YouTube weekly summary"),
    ("08:01", "/youtube/livestreams",     "YouTube livestream tracking"),
    ("08:02", "/attendance/process-sheet","Adult attendance processing"),
    ("08:05", "/mailchimp/weekly-summary","Mailchimp weekly summary"),
    ("08:10", "/planning-center/checkins","Planning Center check-ins"),
]


def schedule_jobs():
    for t, endpoint, label in JOBS:
        #schedule.every().monday.at(t).do(call_api, endpoint, label)
        schedule.every().minute.do(call_api, endpoint, label)
        logging.info(f"Scheduled '{label}' at Mondays {t}")


# ─── Entrypoint ─────────────────────────────────────────────────────────────
def main():
    schedule_jobs()
    logging.info("⏱ Scheduler started. Waiting for jobs…")
    while True:
        schedule.run_pending()
        time.sleep(60)  # wake up every minute and check


if __name__ == "__main__":
    main()

