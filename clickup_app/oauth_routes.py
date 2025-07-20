# clickup_app/oauth_routes.py

# clickup_app/oauth_routes.py

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse
from urllib.parse import urlencode
import requests

from clickup_app.config   import CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, SCOPES
from clickup_app.crud     import create_or_update_token
from clickup_app.database import init_db
from app.db               import get_db
from sqlalchemy.orm       import Session

router = APIRouter()

@router.get("/auth/start")
def start_auth():
    from clickup_app.config import CLIENT_ID, REDIRECT_URI, SCOPES
    url = (
        "https://app.clickup.com/api"
        f"?client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&scope={SCOPES}"
    )
    return {"auth_url": url}


@router.get("/auth/callback")
def clickup_callback(code: str, db: Session = Depends(get_db)):
    """
    Exchange code for an access token, fetch the user's teams,
    and persist the token for the first workspace.
    """
    # 1) Exchange the code for a token
    token_url = "https://api.clickup.com/api/v2/oauth/token"
    resp = requests.post(
        token_url,
        data={
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "code":          code,
            "redirect_uri":  REDIRECT_URI,
            "grant_type":    "authorization_code",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()

    # 2) Determine which workspace(s) were granted
    teams_resp = requests.get(
        "https://api.clickup.com/api/v2/team",
        headers={"Authorization": data["access_token"]},
    )
    if teams_resp.status_code != 200:
        raise HTTPException(status_code=teams_resp.status_code, detail=teams_resp.text)
    teams = teams_resp.json().get("teams", [])
    if not teams:
        raise HTTPException(status_code=400, detail="No authorized teams found")
    workspace_id = teams[0]["id"]

    # 3) Persist the token in our DB
    init_db()  # ensures clickup_tokens table exists
    create_or_update_token(
        db,
        workspace_id,
        data["access_token"],
        data.get("refresh_token", ""),
        data.get("expires_in", 3600),
    )

    return {"status": "ok", "workspace_id": workspace_id}

