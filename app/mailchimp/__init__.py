# app/mailchimp/__init__.py
from fastapi import APIRouter
from .weekly_summary import router as weekly_summary_router
from .sync import router as sync_router
from .read import router as read_router
from .link_text import router as link_router

router = APIRouter()
router.include_router(weekly_summary_router)
router.include_router(sync_router)
router.include_router(read_router)
router.include_router(link_router)
