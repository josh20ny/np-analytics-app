# weekly_summary/config.py
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

# ─── PRIMARY CONFIG ───────────────────────────────────────────────────────────

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    db_name     = os.getenv("DB_NAME")
    db_user     = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host     = os.getenv("DB_HOST", "localhost")
    db_port     = os.getenv("DB_PORT", "5432")

    if all([db_name, db_user, db_password]):
        DATABASE_URL = (
            f"postgresql://{quote_plus(db_user)}:{quote_plus(db_password)}@{db_host}:{db_port}/{db_name}"
        )
    else:
        raise RuntimeError("Missing database credentials: DATABASE_URL or DB_NAME/USER/PASSWORD required.")

# ─── OPTIONAL TOKENS (warn only if missing) ───────────────────────────────────

CLICKUP_TOKEN = os.getenv("CLICKUP_BOT_ACCESS_TOKEN")
CLICKUP_WORKSPACE_ID = os.getenv("CLICKUP_WORKSPACE_ID")
CLICKUP_CHANNEL_ID = os.getenv("CLICKUP_CHANNEL_ID")

missing = []
if not CLICKUP_TOKEN:
    missing.append("CLICKUP_BOT_ACCESS_TOKEN")
if not CLICKUP_CHANNEL_ID:
    missing.append("CLICKUP_CHANNEL_ID")

if missing:
    print(f"⚠️  Warning: Missing optional env vars: {', '.join(missing)}")

# You can later check for critical ones before using them
