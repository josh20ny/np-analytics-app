# app/planning_center/oauth_routes.py

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse
from urllib.parse import urlencode
from sqlalchemy.orm import Session
import requests
from datetime import datetime, timedelta

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
