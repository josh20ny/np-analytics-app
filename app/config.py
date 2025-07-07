# app/config.py
import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

# load .env from project root
load_dotenv()

class Settings:
    # Google Sheets
    # path to your JSON file; default for local dev
    SERVICE_ACCOUNT_FILE: str = os.getenv(
        "GOOGLE_SERVICE_ACCOUNT_FILE", "google_service_account.json"
    )
    SPREADSHEET_ID: str
    SHEET_NAME: str
    class Config:
        env_file = ".env"

    # Database
    DB_NAME   = os.getenv("DB_NAME", "")
    DB_USER   = os.getenv("DB_USER", "")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_HOST   = os.getenv("DB_HOST", "")
    DB_PORT   = int(os.getenv("DB_PORT", 5432))

    # Mailchimp
    MAILCHIMP_API_KEY       = os.getenv("MAILCHIMP_API_KEY", "")
    MAILCHIMP_SERVER_PREFIX = os.getenv("MAILCHIMP_SERVER_PREFIX", "")
    MAILCHIMP_AUDIENCE_NORTHPOINT = os.getenv("MAILCHIMP_AUDIENCE_NORTHPOINT", "")
    MAILCHIMP_AUDIENCE_INSIDEOUT  = os.getenv("MAILCHIMP_AUDIENCE_INSIDEOUT", "")
    MAILCHIMP_AUDIENCE_TRANSIT    = os.getenv("MAILCHIMP_AUDIENCE_TRANSIT", "")
    MAILCHIMP_AUDIENCE_UPSTREET   = os.getenv("MAILCHIMP_AUDIENCE_UPSTREET", "")
    MAILCHIMP_AUDIENCE_WAUMBA     = os.getenv("MAILCHIMP_AUDIENCE_WAUMBA", "")

    # Planning Center
    PLANNING_CENTER_APP_ID = os.getenv("PLANNING_CENTER_APP_ID", "")
    PLANNING_CENTER_SECRET = os.getenv("PLANNING_CENTER_SECRET", "")

    # YouTube
    YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
    CHANNEL_ID      = os.getenv("CHANNEL_ID", "")

# single settings instance you can import everywhere


settings = Settings()


