# clickup_app/clickup_client.py

import os
import requests
from .config import (
    CLICKUP_BOT_ACCESS_TOKEN,
    CLICKUP_WORKSPACE_ID,
    CLICKUP_CHANNEL_ID,
    CLICKUP_FALLBACK_TASK_ID,
    USE_CLICKUP_CHAT_V3,
)

API_V3_BASE = os.getenv("CLICKUP_API_BASE", "https://api.clickup.com/api/v3")
API_V2_BASE = os.getenv("CLICKUP_API_BASE_V2", "https://api.clickup.com/api/v2")


class ClickUpService:
    def __init__(self):
        token = CLICKUP_BOT_ACCESS_TOKEN
        if not token:
            raise RuntimeError("CLICKUP_BOT_ACCESS_TOKEN is required")
        self.token = token
        self.auth_header = {"Authorization": f"Bearer {self.token}"}

    def send_message(self, content: str, channel_id: str = None):
        """
        Send a message via Chat v3 or fallback to task comments based on feature flag.
        """
        target_channel = channel_id or CLICKUP_CHANNEL_ID
        if USE_CLICKUP_CHAT_V3:
            return self._post_chat_v3(content, target_channel)
        else:
            return self._post_comment_fallback(content)

    def _post_chat_v3(self, content: str, channel_id: str):
        """
        Post a chat message using the ClickUp v3 API.
        """
        url = f"{API_V3_BASE}/workspaces/{CLICKUP_WORKSPACE_ID}/chat/channels/{channel_id}/messages"
        payload = {
            "type": "message",
            "content_format": "text/md",
            "content": content,
        }
        headers = {
            **self.auth_header,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code != 201:
            raise RuntimeError(f"ClickUp v3 Chat error {resp.status_code}: {resp.text}")
        return resp.json()

    def _post_comment_fallback(self, content: str):
        """
        Post the message as a comment on a fallback task when Chat v3 is unavailable.
        """
        task_id = CLICKUP_FALLBACK_TASK_ID
        if not task_id:
            raise RuntimeError("CLICKUP_BOT_FALLBACK_TASK_ID is required for fallback comments.")
        url = f"{API_V2_BASE}/task/{task_id}/comment"
        payload = {"comment_text": content}
        headers = {
            **self.auth_header,
            "Content-Type": "application/json",
        }
        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"Comment fallback error {resp.status_code}: {resp.text}")
        return resp.json()



