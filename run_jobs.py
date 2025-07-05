# run_jobs.py

import os
import logging
import requests

# ─── CONFIG ───────────────────────────────────────────────────────────────────
# You can set API_BASE_URL in Render’s env-vars; otherwise it falls back here.
BASE_URL = os.getenv(
    "API_BASE_URL",
    "https://np-analytics-app.onrender.com"
)

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ─── JOBS ─────────────────────────────────────────────────────────────────────
JOBS = [
    ("/youtube/weekly-summary",  "YouTube weekly summary"),
    ("/youtube/livestreams",     "YouTube livestream tracking"),
    ("/attendance/process-sheet","Adult attendance processing"),
    ("/mailchimp/weekly-summary","Mailchimp weekly summary"),
    ("/planning-center/checkins","Planning Center check-ins"),
    ("/planning-center/groups","Planning Center Groups"),
]

# ─── RUNNER ───────────────────────────────────────────────────────────────────
def call_api(endpoint: str, label: str):
    url = BASE_URL.rstrip("/") + endpoint
    try:
        resp = requests.get(url, timeout=30)
        if resp.ok:
            logging.info(f"✅ {label} succeeded ({resp.status_code})")
        else:
            logging.warning(f"⚠️ {label} returned {resp.status_code}: {resp.text}")
    except Exception:
        logging.exception(f"❌ Exception during {label}")

def main():
    for endpoint, label in JOBS:
        call_api(endpoint, label)

if __name__ == "__main__":
    main()
