import os
from pathlib import Path
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# The OAuth scopes required for YouTube Analytics
SCOPES = ["https://www.googleapis.com/auth/yt-analytics.readonly"]

# Optional: path to your client secrets JSON via env var
CLIENT_SECRET_FILE = os.environ.get(
    "YOUTUBE_CLIENT_SECRET_PATH",
    "client_secret.json"
)

# Optional: override token location via env var
TOKEN_PATH_ENV = os.environ.get("YOUTUBE_TOKEN_PATH")


def get_youtube_analytics_service():
    """
    Returns an authorized YouTube Analytics service object.
    The token is stored at the path specified by YOUTUBE_TOKEN_PATH
    or in /tmp/.credentials/youtube_token.json by default.
    """
    # Determine where to store credential token
    if TOKEN_PATH_ENV:
        token_path = TOKEN_PATH_ENV
        # ensure directory exists
        Path(token_path).parent.mkdir(parents=True, exist_ok=True)
    else:
        fallback_dir = Path('/tmp') / '.credentials'
        fallback_dir.mkdir(parents=True, exist_ok=True)
        token_path = str(fallback_dir / 'youtube_token.json')

    creds = None
    # Load existing credentials if available
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    # If no valid credentials, go through OAuth flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CLIENT_SECRET_FILE, SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_path, 'w') as token_file:
            token_file.write(creds.to_json())

    # Build and return the service
    return build('youtubeAnalytics', 'v2', credentials=creds)

