# app/routes_ga_sanity.py
from fastapi import APIRouter
from app.services.ga4 import run_report
from app.utils.common import get_previous_week_dates_cst

router = APIRouter(prefix="/ga4", tags=["Google Analytics"])

@router.get("/ping")
def ping_ga4():
    start_iso, end_iso = get_previous_week_dates_cst()
    rows = run_report(
        dimensions=[],
        metrics=["activeUsers", "screenPageViews"],
        start_date=start_iso, end_date=end_iso,
    )
    return {"ok": True, "start": start_iso, "end": end_iso, "rows": rows}
