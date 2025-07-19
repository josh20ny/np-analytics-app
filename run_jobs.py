import os
import logging
import requests
import time
import weekly_summary.main

BASE_URL = os.getenv(
    "API_BASE_URL",
    "https://np-analytics-app.onrender.com"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

JOBS = [
    ("/youtube/weekly-summary",  "YouTube weekly summary"),
    ("/youtube/livestreams",     "YouTube livestream tracking"),
    ("/attendance/process-sheet","Adult attendance processing"),
    ("/mailchimp/weekly-summary","Mailchimp weekly summary"),
    ("/planning-center/groups",  "Planning Center Groups"),
    ("/planning-center/checkins","Planning Center check-ins"),
]

LOG_OUTPUT = []

def call_api_and_capture(endpoint: str, label: str):
    url = BASE_URL.rstrip("/") + endpoint
    try:
        resp = requests.get(url, timeout=30)
        status_line = f"{label}: {resp.status_code} - {resp.reason}"
        if resp.ok:
            LOG_OUTPUT.append(f"✅ {status_line}")
            if "checkins" in endpoint:
                try:
                    data = resp.json()
                    dbg = data.get("debug_text", "")
                    return dbg
                except Exception as e:
                    print(f"❌ Error parsing JSON for checkins debug: {e}")
        else:
            LOG_OUTPUT.append(f"⚠️ {status_line}\n{resp.text}")
    except Exception as e:
        LOG_OUTPUT.append(f"❌ {label}: {str(e)}")
    return ""

def call_api(endpoint: str, label: str):
    _ = call_api_and_capture(endpoint, label)

def main():
    WAKEUP_DELAY = 10
    debug_text = ""

    try:
        resp = requests.get(BASE_URL.rstrip("/") + "/docs", timeout=10)
        logging.info(f"🌐  Warm-up ping returned {resp.status_code}")
    except Exception:
        logging.info(f"⏱️  Waiting {WAKEUP_DELAY}s for app to spin up…")
        time.sleep(WAKEUP_DELAY)

    for idx, (endpoint, label) in enumerate(JOBS):
        print(f"📡 Calling route: {endpoint} – {label}")
        if "checkins" in endpoint:
            debug_text = call_api_and_capture(endpoint, label)
        else:
            call_api(endpoint, label)

        print(f"✅ Finished: {label}: {resp.status_code}")
        if idx < len(JOBS) - 1:
            logging.info("⏱️  Sleeping 10s before next job…")
            time.sleep(10)

    weekly_summary.main.run(LOG_OUTPUT, debug_text)

if __name__ == "__main__":
    main()
