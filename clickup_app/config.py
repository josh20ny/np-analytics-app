# clickup_app/config.py

import os

# ── Core settings ────────────────────────────────────────────────────
CLICKUP_WORKSPACE_ID     = os.getenv("CLICKUP_WORKSPACE_ID")
CLICKUP_CHANNEL_ID       = os.getenv("CLICKUP_CHANNEL_ID")
CLICKUP_BOT_ACCESS_TOKEN = os.getenv("CLICKUP_BOT_ACCESS_TOKEN")
CLICKUP_FALLBACK_TASK_ID = os.getenv("CLICKUP_BOT_FALLBACK_TASK_ID")

# ── Feature flags ────────────────────────────────────────────────────
# Toggle between v3 chat vs comment fallback
toggle = os.getenv("USE_CLICKUP_CHAT_V3", "false").lower()
USE_CHAT_V3 = toggle in ("1", "true", "yes")

# ── Helper validations ─────────────────────────────────────────────────
if not CLICKUP_BOT_ACCESS_TOKEN:
    raise RuntimeError("Missing required env var: CLICKUP_BOT_ACCESS_TOKEN")
if not CLICKUP_WORKSPACE_ID:
    raise RuntimeError("Missing required env var: CLICKUP_WORKSPACE_ID")
