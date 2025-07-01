# youtube_auth.py

import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/yt-analytics.readonly"]

def get_youtube_analytics_service():
    creds = None
    token_path = "token.json"
    client_secret_file = "client_secret_429513113002-0h67df6t61ntchrb5ofokia7ur4a2pas.apps.googleusercontent.com.json"  # replace with your actual file if different

    # Load existing credentials
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    # If no valid credentials, do OAuth flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
            creds = flow.run_local_server(port=8080)
        
        # Save the credentials
        with open(token_path, "w") as token:
            token.write(creds.to_json())

    # Return the authenticated YouTube Analytics service
    return build("youtubeAnalytics", "v2", credentials=creds)
