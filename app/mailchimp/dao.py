# app/mailchimp/dao.py
from typing import Dict, Any, List
from psycopg2.extras import execute_values

def upsert_campaigns(conn, rows: List[Dict[str, Any]]):
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO mailchimp_campaigns (
              id, web_id, type, send_time, emails_sent,
              list_id, subject_line, preview_text, from_name, reply_to, subject
            )
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
              web_id = EXCLUDED.web_id,
              type = EXCLUDED.type,
              send_time = EXCLUDED.send_time,
              emails_sent = EXCLUDED.emails_sent,
              list_id = EXCLUDED.list_id,
              subject_line = EXCLUDED.subject_line,
              preview_text = EXCLUDED.preview_text,
              from_name = EXCLUDED.from_name,
              reply_to = EXCLUDED.reply_to,
              subject = COALESCE(EXCLUDED.subject, mailchimp_campaigns.subject)
        """, [(
            r["id"],
            r.get("web_id"),
            r.get("type"),
            r.get("send_time"),
            r.get("emails_sent"),
            r.get("list_id"),
            r.get("subject_line"),
            r.get("preview_text"),
            r.get("from_name"),
            r.get("reply_to"),
            # keep legacy "subject" populated from subject_line when present
            (r.get("subject_line") or r.get("preview_text")),
        ) for r in rows])


def update_campaign_report(conn, cid: str, rep: Dict[str, Any]):
    opens = rep.get("opens") or {}
    clicks = rep.get("clicks") or {}
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE mailchimp_campaigns SET
              report_open_rate_proxy_excluded = %(proxy_excluded_open_rate)s,
              report_click_rate = %(click_rate)s,
              report_unique_opens = %(unique_opens)s,
              report_total_opens = %(opens_total)s,
              report_unique_clicks = %(unique_clicks)s,
              report_total_clicks = %(clicks_total)s,
              report_unsubscribed = %(unsubscribed)s,
              report_hard_bounces = %(hard_bounces)s,
              report_soft_bounces = %(soft_bounces)s,
              report_abuse_reports = %(abuse_reports)s
            WHERE id = %(cid)s
        """, dict(
            cid=cid,
            proxy_excluded_open_rate=opens.get("proxy_excluded_open_rate"),
            click_rate=clicks.get("click_rate"),
            unique_opens=opens.get("unique_opens"),
            opens_total=opens.get("opens_total") or opens.get("open_total"),
            unique_clicks=clicks.get("unique_clicks"),
            clicks_total=clicks.get("clicks_total"),
            unsubscribed=rep.get("unsubscribed"),
            hard_bounces=(rep.get("bounces") or {}).get("hard_bounces"),
            soft_bounces=(rep.get("bounces") or {}).get("soft_bounces"),
            abuse_reports=rep.get("abuse_reports"),
        ))

def upsert_campaign_links(conn, cid: str, links: List[Dict[str, Any]]):
    if not links:
        return
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO mailchimp_campaign_links (campaign_id, link_id, url, unique_clicks, total_clicks)
            VALUES %s
            ON CONFLICT (campaign_id, link_id) DO UPDATE SET
              url = EXCLUDED.url,
              unique_clicks = EXCLUDED.unique_clicks,
              total_clicks = EXCLUDED.total_clicks
        """, [(
            cid,
            l["id"],
            l.get("url"),
            l.get("unique_clicks", 0),
            l.get("total_clicks", 0),
        ) for l in links])

def upsert_automations(conn, rows: List[Dict[str, Any]]):
    if not rows:
        return

    def _ts(val):
        # Convert empty strings to None so Postgres timestamptz is happy
        return val or None

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO mailchimp_automations (workflow_id, name, status, create_time, start_time)
            VALUES %s
            ON CONFLICT (workflow_id) DO UPDATE SET
              name = EXCLUDED.name,
              status = EXCLUDED.status,
              create_time = EXCLUDED.create_time,
              start_time = EXCLUDED.start_time
        """, [(
            r["workflow_id"],
            r.get("name"),
            r.get("status"),
            _ts(r.get("create_time")),
            _ts(r.get("start_time")),
        ) for r in rows])

def upsert_automation_emails(conn, rows: List[Dict[str, Any]]):
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO mailchimp_automation_emails (
              workflow_id, workflow_email_id, position, subject_line, from_name, reply_to, status
            )
            VALUES %s
            ON CONFLICT (workflow_id, workflow_email_id) DO UPDATE SET
              position = EXCLUDED.position,
              subject_line = EXCLUDED.subject_line,
              from_name = EXCLUDED.from_name,
              reply_to = EXCLUDED.reply_to,
              status = EXCLUDED.status
        """, [(
            r["workflow_id"],
            r["workflow_email_id"],
            r.get("position"),
            r.get("subject_line"),
            r.get("from_name"),
            r.get("reply_to"),
            r.get("status"),
        ) for r in rows])
