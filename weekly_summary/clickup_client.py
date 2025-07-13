# weekly_summary/clickup_client.py

import os
import requests
from dotenv import load_dotenv

load_dotenv()  # so it reads your .env

CLICKUP_TOKEN      = os.getenv("CLICKUP_TOKEN")
CLICKUP_WORKSPACE  = os.getenv("CLICKUP_WORKSPACE_ID")
CLICKUP_CHANNEL    = os.getenv("CLICKUP_CHANNEL_ID")

BASE_URL = "https://api.clickup.com/api/v3"

def post_message(content: str, msg_type: str = "message"):
    """
    Posts `content` into the ClickUp Chat channel you configured.
    """
    url = (
        f"{BASE_URL}/workspaces/{CLICKUP_WORKSPACE}"
        f"/chat/channels/{CLICKUP_CHANNEL}/messages"
    )
    headers = {
        "Authorization": CLICKUP_TOKEN,
        "Content-Type":  "application/json"
    }
    payload = {
        "content": content,
        "type":    msg_type
    }
    resp = requests.post(url, json=payload, headers=headers)
    resp.raise_for_status()
    return resp.json()
