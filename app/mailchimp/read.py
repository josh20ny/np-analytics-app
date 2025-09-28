from fastapi import APIRouter, Depends, Query
from typing import Optional
from app.db import get_conn

router = APIRouter(prefix="/mailchimp", tags=["Mailchimp Read"])

@router.get("/campaigns/recent")
def list_recent_campaigns(
    days: int = 90,
    list_id: Optional[str] = Query(default=None),
    limit: int = 500,
    conn = Depends(get_conn),
):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
              c.id, c.send_time, c.list_id, c.subject, c.emails_sent,
              ROUND((100 * COALESCE(c.open_rate_effective, 0))::numeric, 2)  AS open_rate_pct,
              ROUND((100 * COALESCE(c.click_rate_effective, 0))::numeric, 2) AS click_rate_pct,
              t.top_link_url, t.top_link_unique, t.top_link_total
            FROM v_mailchimp_campaigns_enriched c
            LEFT JOIN v_mailchimp_campaign_top_link t ON t.campaign_id = c.id
            WHERE c.send_time >= NOW() - (%s * INTERVAL '1 day')
              AND (%s IS NULL OR c.list_id = %s)
            ORDER BY c.send_time DESC
            LIMIT %s
        """, (days, list_id, list_id, limit))
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]

@router.get("/automations/emails")
def list_automation_emails(conn = Depends(get_conn)):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT workflow_id, flow_name, flow_status, workflow_email_id, position, email_status,
                   subject_line, from_name, reply_to
            FROM v_mailchimp_automation_emails_labeled
            ORDER BY flow_name, position
        """)
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]

@router.get("/campaigns/automation-sends")
def list_automation_sends(
    days: int = 90,
    list_id: Optional[str] = Query(default=None),
    limit: int = 500,
    conn = Depends(get_conn),
):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
              c.id, c.send_time, c.list_id, c.subject, c.emails_sent,
              ROUND((100 * COALESCE(c.open_rate_effective, 0))::numeric, 2)  AS open_rate_pct,
              ROUND((100 * COALESCE(c.click_rate_effective, 0))::numeric, 2) AS click_rate_pct
            FROM v_mailchimp_campaigns_enriched c
            WHERE c.type = 'automation'
              AND c.send_time >= NOW() - (%s * INTERVAL '1 day')
              AND (%s IS NULL OR c.list_id = %s)
            ORDER BY c.send_time DESC
            LIMIT %s
        """, (days, list_id, list_id, limit))
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]
