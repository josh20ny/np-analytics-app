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
    CLIENT_SECRET_FILE = os.environ.get(
        "YOUTUBE_CLIENT_SECRET_PATH",
        "client_secret_2_429513113002-0h67df6t61ntchrb5ofokia7ur4a2pas.apps.googleusercontent.com.json"   # fallback if you didn’t set it
    )
 
     # Load existing credentials
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
 
    # If no valid credentials, do OAuth flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
             creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server()
         
        # Save the credentials
        with open(token_path, "w") as token:
            token.write(creds.to_json())
 
    # Return the authenticated YouTube Analytics service
    return build("youtubeAnalytics", "v2", credentials=creds)
