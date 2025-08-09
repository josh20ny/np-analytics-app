# app/planning_center/oauth_routes.py

from datetime import datetime, timedelta
from urllib.parse import urlencode

import requests
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.config import settings
from app.db import get_db
from app.models import PlanningCenterToken

router = APIRouter(
    prefix="/planning-center/oauth",
    tags=["Planning Center OAuth"],
)

@router.get("/start")
def start_auth():
    params = {
        "client_id":     settings.PLANNING_CENTER_APP_ID,
        "redirect_uri":  settings.API_BASE_URL + "/planning-center/oauth/callback",
        "response_type": "code",
        "scope":         "calendar check_ins giving groups people services",
    }
    url = "https://api.planningcenteronline.com/oauth/authorize?" + urlencode(params)
    return RedirectResponse(url)

@router.get("/callback")
def callback(code: str, db: Session = Depends(get_db)):
    # Exchange code for tokens
    token_resp = requests.post(
        "https://api.planningcenteronline.com/oauth/token",
        data={
            "grant_type":    "authorization_code",
            "code":          code,
            "redirect_uri":  settings.API_BASE_URL + "/planning-center/oauth/callback",
            "client_id":     settings.PLANNING_CENTER_APP_ID,
            "client_secret": settings.PLANNING_CENTER_SECRET,
        },
        headers={"Accept": "application/json"},
        timeout=15
    )
    if token_resp.status_code != 200:
        raise HTTPException(status_code=token_resp.status_code, detail=token_resp.text)

    data = token_resp.json()
    expires_at = datetime.utcnow() + timedelta(seconds=data["expires_in"])

    token = PlanningCenterToken(
        workspace_id="global",
        access_token=data["access_token"],
        refresh_token=data["refresh_token"],
        expires_at=expires_at
    )
    db.merge(token)
    db.commit()

    return {"status": "ok"}

def get_pco_headers(db: Session) -> dict:
    """
    Pulls the saved tokens, refreshes if expired, and returns
    headers for any PCO API request.
    """
    token_row = db.query(PlanningCenterToken).filter_by(workspace_id="global").one()

    # Refresh if expired
    if token_row.expires_at <= datetime.utcnow():
        resp = requests.post(
            "https://api.planningcenteronline.com/oauth/token",
            data={
                "grant_type":    "refresh_token",
                "refresh_token": token_row.refresh_token,
                "client_id":     settings.PLANNING_CENTER_APP_ID,
                "client_secret": settings.PLANNING_CENTER_SECRET,
            },
            headers={"Accept": "application/json"},
            timeout=15
        )
        resp.raise_for_status()
        data = resp.json()

        token_row.access_token  = data["access_token"]
        token_row.refresh_token = data.get("refresh_token", token_row.refresh_token)
        token_row.expires_at    = datetime.utcnow() + timedelta(seconds=data["expires_in"])
        db.commit()

    return {
        "Authorization": f"Bearer {token_row.access_token}",
        "Accept": "application/vnd.api+json",
    }

