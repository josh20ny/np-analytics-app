from fastapi import FastAPI
from app.google_sheets import router as gs_router
from app.attendance import router as attendance_router
from app.mailchimp import router as mailchimp_router
from app.planning_center.checkins import router as pc_check_router
from app.planning_center.groups import router as pc_groups_router
from app.youtube.routes import router as yt_router

app = FastAPI()

app.include_router(gs_router)
app.include_router(attendance_router)
app.include_router(mailchimp_router)
app.include_router(pc_check_router)
app.include_router(pc_groups_router)
app.include_router(yt_router)