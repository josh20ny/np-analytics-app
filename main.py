# main.py
from fastapi import FastAPI

# App routers
from app.google_sheets import router as sheets_router
from app.attendance import router as attendance_router
from app.mailchimp import router as mailchimp_router

# Planning Center
from app.planning_center.checkins import router as pc_checkins_router
from app.planning_center.groups import router as pc_groups_router
from app.planning_center.giving import router as pc_giving_router
from app.planning_center.oauth_routes import router as pco_oauth_router

# ClickUp app
from clickup_app.webhooks import router as clickup_webhooks_router
from clickup_app.oauth_routes import router as cu_oauth_router

# YouTube
from app.youtube.routes import router as youtube_router

# (Optional) other routers if you have them:
# from app.assistant.routes import router as assistant_router

app = FastAPI(title="NP Analytics", version="1.0.0")

# Healthcheck
@app.get("/healthz")
def healthcheck():
    return {"ok": True}

# ── Routers ───────────────────────────────────────────────────────────────────
# Core data sources
app.include_router(sheets_router)
app.include_router(attendance_router)
app.include_router(mailchimp_router)

# Planning Center
app.include_router(pc_checkins_router)
app.include_router(pc_groups_router)
app.include_router(pc_giving_router)
app.include_router(pco_oauth_router)

# ClickUp
app.include_router(clickup_webhooks_router)
app.include_router(cu_oauth_router)

# YouTube
app.include_router(youtube_router)

# Optional extras if present
# app.include_router(assistant_router)
