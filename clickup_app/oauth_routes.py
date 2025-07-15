# clickup_app/oauth_routes.py

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse
import requests

from clickup_app.config   import CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, SCOPES
from clickup_app.crud     import create_or_update_token
from clickup_app.database import init_db
# ← replace this import
from app.db               import get_db
from sqlalchemy.orm       import Session

router = APIRouter()

@router.get("/auth/clickup")
def clickup_auth():
    authorize_url = (
        f"https://app.clickup.com/api?"
        f"client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&response_type=code"
        f"&scope={SCOPES}"
    )
    return RedirectResponse(authorize_url)

@router.get("/auth/callback")
def clickup_callback(code: str, db: Session = Depends(get_db)):
    # 1) Exchange code for token
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
    data = resp.json()
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=data.get("err") or "Token exchange failed")

    # 2) Determine which workspaces the user granted
    teams_resp = requests.get(
        "https://api.clickup.com/api/v2/team",
        headers={"Authorization": data["access_token"]}
    )
    teams_data = teams_resp.json()
    if not teams_data.get("teams"):
        raise HTTPException(400, "No authorized teams found")
    workspace_id = teams_data["teams"][0]["id"]

    # 3) Persist in your DB
    init_db()
    create_or_update_token(
        db,
        workspace_id,
        data["access_token"],
        data.get("refresh_token", ""),
        data.get("expires_in", 3600)
    )

    return {"status": "ok", "workspace_id": workspace_id}
