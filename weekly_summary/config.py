# weekly_summary/config.py
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()  # Load .env

# DATABASE_URL fallback builder
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
        raise RuntimeError(
            "Missing database credentials: DATABASE_URL or DB_NAME/DB_USER/DB_PASSWORD required."
        )

# Optional ClickUp settings
CLICKUP_TOKEN        = os.getenv("CLICKUP_BOT_ACCESS_TOKEN")
CLICKUP_WORKSPACE_ID = os.getenv("CLICKUP_WORKSPACE_ID")
CLICKUP_CHANNEL_ID   = os.getenv("CLICKUP_CHANNEL_ID")

# Minimal warnings for missing optional vars
_missing = []
if not CLICKUP_TOKEN:
    _missing.append("CLICKUP_BOT_ACCESS_TOKEN")
if not CLICKUP_CHANNEL_ID:
    _missing.append("CLICKUP_CHANNEL_ID")
if _missing:
    print(f"⚠️ Warning: Missing optional env vars: {', '.join(_missing)}")
