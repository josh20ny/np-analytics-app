# app/config.py
from pathlib import Path
from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from pydantic import Field
from urllib.parse import urlparse
from typing import Optional, List
import os

# Load local .env for development
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")


class Settings(BaseSettings):
    # ─── Google Sheets ──────────────────────────────────────────────────────────
    # In Render, the service account JSON will be mounted here
    GOOGLE_SERVICE_ACCOUNT_FILE: str = "/etc/secrets/google_service_account.json"
    GOOGLE_SPREADSHEET_ID: str
    GOOGLE_SHEET_NAME: str

    # ─── Google / GA4 (annotated so Pydantic is happy) ─────────────────────────
    # These are read directly from the environment; we normalize them below.
    GOOGLE_APPLICATION_CREDENTIALS: Optional[str] = Field(default=None)
    GA4_PROPERTY_ID: Optional[str] = Field(default=None)

    # Raw CSV strings from env; normalized below to lists.
    GA4_WHATS_NEXT_PATHS: str = Field(default="/whatsnext")          # comma-separated
    GA4_GIVING_DOMAINS: str = Field(default="churchcenter.com")      # comma-separated (hostnames or URLs)

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


# ─── Normalization helpers (outside the class) ─────────────────────────────────
def _normalize_domains(csv_value: str) -> List[str]:
    """
    Accepts a CSV of domains or URLs; returns a list of lowercase hostnames.
    Examples:
      "https://npaustin.churchcenter.com/giving, churchcenter.com" -> ["npaustin.churchcenter.com", "churchcenter.com"]
    """
    out: List[str] = []
    for raw in csv_value.split(","):
        raw = raw.strip()
        if not raw:
            continue
        parsed = urlparse(raw)
        host = parsed.netloc if parsed.netloc else raw
        out.append(host.lower())
    return out


def _normalize_paths(csv_value: str) -> List[str]:
    """
    Accepts a CSV of paths and ensures each starts with '/'.
    Example: "whatsnext, /serve" -> ["/whatsnext", "/serve"]
    """
    out: List[str] = []
    for raw in csv_value.split(","):
        p = raw.strip()
        if not p:
            continue
        if not p.startswith("/"):
            p = "/" + p
        out.append(p)
    return out


# ─── Public, module-level config your app can import ───────────────────────────
# Prefer GOOGLE_APPLICATION_CREDENTIALS; fall back to GOOGLE_SERVICE_ACCOUNT_FILE
GOOGLE_ADC_PATH: Optional[str] = settings.GOOGLE_APPLICATION_CREDENTIALS or settings.GOOGLE_SERVICE_ACCOUNT_FILE

GA4_PROPERTY_ID: Optional[str] = settings.GA4_PROPERTY_ID
GA4_WHATS_NEXT_PATHS: List[str] = _normalize_paths(settings.GA4_WHATS_NEXT_PATHS)
GA4_GIVING_DOMAINS: List[str] = _normalize_domains(settings.GA4_GIVING_DOMAINS)

# Optional: debug prints (comment out in prod)
# print("GA4_PROPERTY_ID:", GA4_PROPERTY_ID)
# print("GOOGLE_ADC_PATH:", GOOGLE_ADC_PATH)
# print("GA4_WHATS_NEXT_PATHS:", GA4_WHATS_NEXT_PATHS)
# print("GA4_GIVING_DOMAINS:", GA4_GIVING_DOMAINS)

def _resolve_adc_path() -> str:
    """
    Prefer GOOGLE_APPLICATION_CREDENTIALS, then GOOGLE_SERVICE_ACCOUNT_FILE.
    If relative, resolve from project root (two levels up from this file).
    """
    candidates = [settings.GOOGLE_APPLICATION_CREDENTIALS, settings.GOOGLE_SERVICE_ACCOUNT_FILE]
    project_root = Path(__file__).resolve().parents[1]  # repo root (…/np-analytics-app)
    for cand in candidates:
        if not cand:
            continue
        p = Path(cand).expanduser()
        if not p.is_absolute():
            p = (project_root / p).resolve()
        if p.exists():
            # ensure Google libs can also find it
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(p)
            return str(p)
    raise FileNotFoundError(
        f"Google service account JSON not found. Checked: {candidates}. "
        f"Working dir: {os.getcwd()}. Project root: {project_root}"
    )

GOOGLE_ADC_PATH = _resolve_adc_path()