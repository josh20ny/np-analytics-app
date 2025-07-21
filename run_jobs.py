# run_jobs.py
from dotenv import load_dotenv
load_dotenv()

import os
import time
import requests
from clickup_app.clickup_client import ClickUpService

# Configuration via environment variables
BASE_URL = os.getenv("BASE_URL", "https://np-analytics-app.onrender.com")
WAKEUP_DELAY = int(os.getenv("WAKEUP_DELAY", "10"))

# List of API routes and their descriptive labels
JOBS = [
    ("/youtube/weekly-summary", "YouTube weekly summary"),
    ("/youtube/livestreams", "YouTube livestream tracking"),
    ("/attendance/process-sheet", "Adult attendance processing"),
    ("/mailchimp/weekly-summary", "Mailchimp weekly summary"),
    ("/planning-center/groups", "Planning Center Groups"),
    ("/planning-center/checkins", "Planning Center check-ins"),
]

def call_job(endpoint: str, label: str) -> str:
    """Call an API route and log its result."""
    print(f"📡 Calling route: {endpoint} – {label}")
    try:
        response = requests.get(f"{BASE_URL.rstrip('/')}{endpoint}")
        status = response.status_code
        if status == 200:
            print(f"✅ Finished: {label}: {status}")
            return response.text or ""
        else:
            print(f"❌ Failed: {label}: {status}")
            return ""
    except Exception as e:
        print(f"❌ Error calling {endpoint}: {e}")
        return ""

def main():
    """Run all configured jobs, compile a summary, and post to ClickUp."""
    # Warm-up ping to ensure the service is responsive
    try:
        resp = requests.get(f"{BASE_URL.rstrip('/')}/docs", timeout=10)
        print(f"🌐 Warm-up ping returned {resp.status_code}")
    except Exception:
        print(f"⏱️ Waiting {WAKEUP_DELAY}s for app to spin up…")
        time.sleep(WAKEUP_DELAY)

    # Execute each job and collect outputs
    results = []
    for endpoint, label in JOBS:
        result = call_job(endpoint, label)
        results.append(f"{label} returned: {result}")
        time.sleep(WAKEUP_DELAY)

    # Compile report and send via ClickUpService
    report = "\n\n".join(results)
    clickup = ClickUpService()
    clickup.send_message(report)

if __name__ == "__main__":
    main()