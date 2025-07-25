import os
import time
import json
import requests

from dotenv import load_dotenv
from clickup_app.clickup_client import ClickUpService
from clickup_app.assistant_client import run_assistant_with_tools

# Load environment variables
load_dotenv()

# Configuration
BASE_URL = os.getenv("BASE_URL", "https://np-analytics-app.onrender.com")
WAKEUP_DELAY = int(os.getenv("WAKEUP_DELAY", "10"))

# List of API endpoints to call and their labels
JOBS = [
    ("/youtube/weekly-summary", "YouTube weekly summary"),
    ("/youtube/livestreams", "YouTube livestream tracking"),
    ("/attendance/process-sheet", "Adult attendance processing"),
    ("/mailchimp/weekly-summary", "Mailchimp weekly summary"),
    ("/planning-center/groups", "Planning Center Groups"),
    ("/planning-center/checkins", "Planning Center check-ins"),
]


def call_job(endpoint: str, label: str) -> str:
    """Call an API route and return its raw text response."""
    print(f"📡 Calling route: {endpoint} – {label}")
    try:
        response = requests.get(f"{BASE_URL.rstrip('/')}{endpoint}", timeout=30)
        if response.status_code == 200:
            print(f"✅ Finished: {label} (200)")
            return response.text or ""
        else:
            print(f"❌ Failed: {label} ({response.status_code})")
            return ""
    except Exception as e:
        print(f"❌ Error calling {endpoint}: {e}")
        return ""


def main():
    """Run all jobs, compile JSON, ask Assistant for summary, and post to ClickUp."""
    # Warm-up ping to ensure the service is up
    try:
        ping = requests.get(f"{BASE_URL.rstrip('/')}/docs", timeout=10)
        print(f"🌐 Warm-up ping returned {ping.status_code}")
    except Exception:
        print(f"⏱️ Waiting {WAKEUP_DELAY}s for app to spin up…")
        time.sleep(WAKEUP_DELAY)

    # Execute each job and collect raw JSON
    outputs: dict[str, any] = {}
    for endpoint, label in JOBS:
        raw = call_job(endpoint, label)
        print(f"📥 {label} returned: {raw}")
        try:
            outputs[label] = json.loads(raw or "{}")
        except json.JSONDecodeError:
            outputs[label] = {"error": "invalid JSON", "raw": raw}
        time.sleep(WAKEUP_DELAY)

    # Compile a single JSON report
    report = json.dumps(outputs, indent=2)
    print("📝 Compiled JSON report")

    # Prompt the Assistant
    prompt = (
        "Can you turn this JSON data into a nice summary of this week's data?"
        f"\n\n{report}"
    )
    summary = run_assistant_with_tools(prompt)
    print("📝 Assistant summary generated")

    # Post the summary to ClickUp
    clickup = ClickUpService()
    clickup.send_message(summary)


if __name__ == "__main__":
    main()
