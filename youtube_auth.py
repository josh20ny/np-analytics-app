import os
from pathlib import Path
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# The OAuth scopes required for YouTube Analytics
SCOPES = ["https://www.googleapis.com/auth/yt-analytics.readonly"]

# Optional: override your client-secrets JSON via env var
CLIENT_SECRET_FILE = os.environ.get("YOUTUBE_CLIENT_SECRET_PATH", "client_secret.json")

def get_youtube_analytics_service():
    """
    Returns an authorized YouTube Analytics service object.
    Credentials are stored to YOUTUBE_TOKEN_PATH if set,
    otherwise under /tmp/.credentials/youtube_token.json.
    """
    # 1) Use explicit env‐var path if provided
    token_path = os.environ.get("YOUTUBE_TOKEN_PATH")
    if token_path:
        Path(token_path).parent.mkdir(parents=True, exist_ok=True)
    else:
        # 2) Otherwise, fall back under /tmp
        fallback_dir = Path("/tmp") / ".credentials"
        fallback_dir.mkdir(parents=True, exist_ok=True)
        token_path = str(fallback_dir / "youtube_token.json")

    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    # If no valid creds, do the OAuth dance
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CLIENT_SECRET_FILE, SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save for next time
        with open(token_path, "w") as f:
            f.write(creds.to_json())

    # Build and return the Analytics service
    return build("youtubeAnalytics", "v2", credentials=creds)

