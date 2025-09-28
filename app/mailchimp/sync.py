# app/mailchimp/sync.py
from fastapi import APIRouter, Depends
import os
from app.db import get_conn
from app.utils.common import get_previous_week_dates
from .api import mc_get, mc_paginate
from .dao import (
    upsert_campaigns, update_campaign_report, upsert_campaign_links,
    upsert_automations, upsert_automation_emails
)
from .link_text import resolve_link_text_for_campaign_internal  # <-- we'll add this in link_text.py

router = APIRouter(prefix="/mailchimp", tags=["Mailchimp"])

AUDIENCE_ENV_VARS = [
    "MAILCHIMP_AUDIENCE_NORTHPOINT",
    "MAILCHIMP_AUDIENCE_INSIDEOUT",
    "MAILCHIMP_AUDIENCE_TRANSIT",
    "MAILCHIMP_AUDIENCE_UPSTREET",
    "MAILCHIMP_AUDIENCE_WAUMBA",
]
def _list_ids():
    return [os.environ[k] for k in AUDIENCE_ENV_VARS if os.environ.get(k)]

FIELDS = ("campaigns.id,campaigns.web_id,campaigns.type,campaigns.send_time,campaigns.emails_sent,"
          "campaigns.settings.subject_line,campaigns.settings.preview_text,campaigns.settings.from_name,"
          "campaigns.settings.reply_to,campaigns.recipients.list_id,total_items")

def _sync_campaigns_window(conn, since_send_time: str | None, before_send_time: str | None) -> list[str]:
    """Sync campaigns (all audiences) for a window. Returns list of campaign IDs processed."""
    processed_ids: list[str] = []
    for lid in _list_ids():
        rows = []
        for c in mc_paginate("/campaigns", {
            "status":"sent","list_id":lid,
            "since_send_time": since_send_time,
            "before_send_time": before_send_time,
            "count":1000, "fields": FIELDS
        }, "campaigns"):
            rows.append({
                "id": c["id"],
                "web_id": c.get("web_id"),
                "type": c.get("type"),
                "send_time": c.get("send_time"),
                "emails_sent": c.get("emails_sent"),
                "list_id": (c.get("recipients") or {}).get("list_id"),
                "subject_line": (c.get("settings") or {}).get("subject_line"),
                "preview_text": (c.get("settings") or {}).get("preview_text"),
                "from_name": (c.get("settings") or {}).get("from_name"),
                "reply_to": (c.get("settings") or {}).get("reply_to"),
            })
        if rows:
            upsert_campaigns(conn, rows)
            for r in rows:
                cid = r["id"]
                rep = mc_get(f"/reports/{cid}")
                update_campaign_report(conn, cid, rep)
                clicks = mc_get(f"/reports/{cid}/click-details", params={"count": 1000})
                urls = clicks.get("urls_clicked") or []
                if urls:
                    upsert_campaign_links(conn, cid, urls)
                processed_ids.append(cid)
    conn.commit()
    return processed_ids

def _sync_automations(conn) -> tuple[int,int]:
    autos = []
    for wf in mc_paginate("/automations", {
        "count":1000,
        "fields":"automations.id,automations.status,automations.create_time,automations.start_time,automations.settings.title"
    }, "automations"):
        autos.append({
            "workflow_id": wf["id"],
            "name": (wf.get("settings") or {}).get("title") or None,
            "status": wf.get("status"),
            "create_time": wf.get("create_time") or None,
            "start_time": wf.get("start_time") or None,
        })
    if autos:
        upsert_automations(conn, autos)

    steps = []
    for a in autos:
        wid = a["workflow_id"]
        data = mc_get(f"/automations/{wid}/emails", params={
            "fields":"emails.id,emails.position,emails.status,emails.settings.subject_line,emails.settings.from_name,emails.settings.reply_to"
        })
        for e in data.get("emails") or []:
            steps.append({
                "workflow_id": wid,
                "workflow_email_id": e["id"],
                "position": e.get("position"),
                "status": e.get("status"),
                "subject_line": (e.get("settings") or {}).get("subject_line"),
                "from_name": (e.get("settings") or {}).get("from_name"),
                "reply_to": (e.get("settings") or {}).get("reply_to"),
            })
    if steps:
        upsert_automation_emails(conn, steps)
    conn.commit()
    return (len(autos), len(steps))

@router.post("/campaigns/sync")
def sync_campaigns(since_send_time: str | None = None, before_send_time: str | None = None, conn=Depends(get_conn)):
    ids = _sync_campaigns_window(conn, since_send_time, before_send_time)
    return {"ok": True, "campaigns_processed": len(ids)}

@router.post("/automations/sync")
def sync_automations(conn=Depends(get_conn)):
    a, e = _sync_automations(conn)
    return {"ok": True, "automations": a, "emails": e}

def _rfc3339_window_for_week(start_iso: str, end_iso: str) -> tuple[str, str]:
    return f"{start_iso}T00:00:00Z", f"{end_iso}T23:59:59Z"

@router.post("/weekly-refresh")
def weekly_refresh(conn=Depends(get_conn)):
    """
    1) Sync all 'sent' campaigns for the previous week (all audiences)
    2) Sync automations + steps
    3) Resolve link_text/domain/UTM for all campaigns that were sent in that week
    """
    week_start, week_end = get_previous_week_dates()  # returns YYYY-MM-DD, YYYY-MM-DD
    since_z, before_z = _rfc3339_window_for_week(week_start, week_end)

    # 1) campaigns + reports + click-details
    processed_ids = _sync_campaigns_window(conn, since_z, before_z)

    # 2) automations + steps
    autos_count, steps_count = _sync_automations(conn)

    # 3) resolve link text for those campaigns
    resolved = 0
    for cid in processed_ids:
        resolved += resolve_link_text_for_campaign_internal(conn, cid)

    return {
        "ok": True,
        "week_start": week_start,
        "week_end": week_end,
        "campaigns_processed": len(processed_ids),
        "link_sets_resolved": resolved,
        "automations": autos_count,
        "automation_emails": steps_count
    }
