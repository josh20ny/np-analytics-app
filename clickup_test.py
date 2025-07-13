# clickup_test.py
from dotenv import load_dotenv
import os, requests

load_dotenv()  # read your .env

TOKEN     = os.getenv("CLICKUP_TOKEN")
WORKSPACE = os.getenv("CLICKUP_WORKSPACE_ID")
CHANNEL   = os.getenv("CLICKUP_CHANNEL_ID")

url = (
    f"https://api.clickup.com/api/v3/"
    f"workspaces/{WORKSPACE}/chat/channels/{CHANNEL}/messages"
)

print("Posting to:", url)  # should show no 'None' in URL

headers = {
    "Authorization": TOKEN,
    "Content-Type":  "application/json"
}
payload = {
    "content": "👋 This is a test message in the ‘test’ channel!",
    "type":    "message"
}

resp = requests.post(url, json=payload, headers=headers)
print(resp.status_code, resp.text)
