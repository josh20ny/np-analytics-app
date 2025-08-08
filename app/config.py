from pathlib import Path
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

# Load local .env for development
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

class Settings(BaseSettings):
    # ─── Google Sheets ──────────────────────────────────────────────────────────
    # In Render, the service account JSON will be mounted here
    GOOGLE_SERVICE_ACCOUNT_FILE: str = "/etc/secrets/google_service_account.json"
    GOOGLE_SPREADSHEET_ID: str
    GOOGLE_SHEET_NAME: str

    # ─── Database ───────────────────────────────────────────────────────────────
    DATABASE_URL: str
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: str
    DB_PORT: int

    # ─── Mailchimp ─────────────────────────────────────────────────────────────
    MAILCHIMP_API_KEY: str
    MAILCHIMP_SERVER_PREFIX: str
    MAILCHIMP_AUDIENCE_NORTHPOINT: str
    MAILCHIMP_AUDIENCE_INSIDEOUT: str
    MAILCHIMP_AUDIENCE_TRANSIT: str
    MAILCHIMP_AUDIENCE_UPSTREET: str
    MAILCHIMP_AUDIENCE_WAUMBA: str

    # ─── Planning Center ───────────────────────────────────────────────────────
    PLANNING_CENTER_APP_ID: str
    PLANNING_CENTER_SECRET: str
    PLANNING_CENTER_BASE_URL: str = "https://api.planningcenteronline.com"
    GENERAL_GIVING_FUND_ID: str

    # ─── YouTube ────────────────────────────────────────────────────────────────
    YOUTUBE_API_KEY: str
    CHANNEL_ID: str

    # ─── Optional Extras ────────────────────────────────────────────────────────
    API_BASE_URL: str = ""

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"

# single settings instance for the whole app
settings = Settings()



