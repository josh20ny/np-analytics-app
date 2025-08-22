# app/debug/routes.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db import get_db
from clickup_app.clickup_client import send_dm
import os

router = APIRouter(prefix="/debug", tags=["Debug"])

@router.post("/dm-test")
def dm_test(msg: str = "DM test via /debug ✅", db: Session = Depends(get_db)):
    ws = os.getenv("CLICKUP_WORKSPACE_ID")
    ids = [s.strip() for s in os.getenv("CLICKUP_DM_USER_IDS","").split(",") if s.strip()]
    if not (ws and ids):
        raise HTTPException(400, "Set CLICKUP_WORKSPACE_ID and CLICKUP_DM_USER_IDS")
    ch, m = send_dm(db, ws, ids, msg)
    return {"channel_id": ch, "message": m}
