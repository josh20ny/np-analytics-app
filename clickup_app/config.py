# clickup_app/config.py

import os
from dotenv import load_dotenv

load_dotenv()  # 📥 pull in .env

# ─── OAuth (for future Team-Bot OAuth flow) ────────────────────────────────
CLIENT_ID     = os.getenv("CLICKUP_CLIENT_ID")
CLIENT_SECRET = os.getenv("CLICKUP_CLIENT_SECRET")
REDIRECT_URI  = os.getenv("CLICKUP_REDIRECT_URI")
SCOPES        = os.getenv(
    "CLICKUP_SCOPES",
    "chat:write,chat:read,chat:webhook,user:read,team:read"
)

# ─── Chat integration & feature flags ──────────────────────────────────────
CLICKUP_BOT_ACCESS_TOKEN = os.getenv("CLICKUP_BOT_ACCESS_TOKEN")
CLICKUP_WORKSPACE_ID     = os.getenv("CLICKUP_WORKSPACE_ID")
CLICKUP_CHANNEL_ID       = os.getenv("CLICKUP_CHANNEL_ID")
CLICKUP_FALLBACK_TASK_ID = os.getenv("CLICKUP_BOT_FALLBACK_TASK_ID")

USE_CLICKUP_CHAT_V3 = (
    os.getenv("USE_CLICKUP_CHAT_V3", "false").lower() == "true"
)

# ─── API base URLs (overrideable) ─────────────────────────────────────────
API_BASE_V3 = os.getenv("CLICKUP_API_BASE", "https://api.clickup.com/api/v3")
API_BASE_V2 = os.getenv("CLICKUP_API_BASE_V2", "https://api.clickup.com/api/v2")