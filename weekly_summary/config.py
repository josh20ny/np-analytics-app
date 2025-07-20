# weekly_summary/config.py
import os
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()  # loads .env from project root

DATABASE_URL         = os.getenv("DATABASE_URL")
CLICKUP_TOKEN        = os.getenv("CLICKUP_BOT_ACCESS_TOKEN")
CLICKUP_WORKSPACE_ID = os.getenv("CLICKUP_WORKSPACE_ID")
CLICKUP_CHANNEL_ID      = os.getenv("CLICKUP_CHANNEL_ID")

if not all([DATABASE_URL, CLICKUP_TOKEN, CLICKUP_CHANNEL_ID]):
    raise RuntimeError("One of the required env vars is missing.")