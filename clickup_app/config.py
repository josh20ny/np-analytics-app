# clickup_app/config.py

import os
from dotenv import load_dotenv

load_dotenv()

# OAuth app credentials (from ClickUp’s App settings)
CLIENT_ID     = os.getenv("CLICKUP_CLIENT_ID")
CLIENT_SECRET = os.getenv("CLICKUP_CLIENT_SECRET")

# Where ClickUp will redirect after auth
REDIRECT_URI  = os.getenv(
    "CLICKUP_REDIRECT_URI",
    "https://78c0a1896c5c.ngrok-free.app/auth/callback"
)

# Scope string: add more scopes here as you expand
SCOPES        = "chat:write chat:webhook"
