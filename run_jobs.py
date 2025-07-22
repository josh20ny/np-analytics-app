# run_jobs.py
from dotenv import load_dotenv
load_dotenv()

import os
import time
import logging
import requests
import json

from clickup_app.clickup_client import ClickUpService
from weekly_summary.data_access import (
    fetch_all_with_yoy,
    fetch_all_mailchimp_rows_for_latest_week
)
from weekly_summary.formatter import format_summary
from weekly_summary.report_builder import build_full_report

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
    """Run all configured jobs, compile a JSON dump, and post to ClickUp."""
    # 1) Warm-up ping
    try:
        resp = requests.get(f"{BASE_URL.rstrip('/')}/docs", timeout=10)
        print(f"🌐 Warm-up ping returned {resp.status_code}")
    except Exception:
        print(f"⏱️ Waiting {WAKEUP_DELAY}s for app to spin up…")
        time.sleep(WAKEUP_DELAY)

    # 2) Execute each job and collect JSON outputs
    outputs = {}
    for endpoint, label in JOBS:
        # show that we’re calling
        print(f"📡 Calling route: {endpoint} – {label}")
        
        # perform the call (call_job also prints status)
        result_text = call_job(endpoint, label)
        
        # echo the raw response text
        print(f"📥 {label} returned: {result_text}")
        
        # attempt to parse as JSON
        try:
            outputs[label] = json.loads(result_text or "{}")
        except json.JSONDecodeError:
            outputs[label] = {"error": "invalid JSON", "raw": result_text}

        time.sleep(WAKEUP_DELAY)

    # 3) Build one big JSON report
    report = json.dumps(outputs, indent=2)
    print("📝 Compiled JSON report, sending to ClickUp")

    # 4) Send raw JSON dump to ClickUp
    clickup = ClickUpService()
    clickup.send_message(report)


if __name__ == "__main__":
    main()
