# app/mailchimp/link_text.py
import re
from html import unescape
from urllib.parse import urlparse, parse_qs
from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, Optional, Tuple
from app.db import get_conn
from .api import mc_get

router = APIRouter(prefix="/mailchimp", tags=["Mailchimp Links"])

A_TAG_RE = re.compile(r'<a\b[^>]*?href=["\']([^"\']+)["\'][^>]*?>(.*?)</a>', re.IGNORECASE | re.DOTALL)

def _normalize_html_text(html: str) -> str:
    text = re.sub(r"<[^>]+>", "", html or "")
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()

def _parse_utm(url: str):
    try:
        u = urlparse(url)
        q = parse_qs(u.query)
        return (
            u.netloc or None,
            (q.get("utm_source") or [None])[0],
            (q.get("utm_medium") or [None])[0],
            (q.get("utm_campaign") or [None])[0],
        )
    except Exception:
        return (None, None, None, None)

def _anchor_map_from_content(html: str) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if not html:
        return mapping
    for href, inner in A_TAG_RE.findall(html):
        mapping[href] = _normalize_html_text(inner)
    return mapping

def resolve_link_text_for_campaign_internal(conn, campaign_id: str) -> int:
    """Returns number of link rows updated."""
    content = mc_get(f"/campaigns/{campaign_id}/content")
    html = (content.get("html") or "").strip()
    if not html:
        html = content.get("plain_text") or ""

    anchor_map = _anchor_map_from_content(html)

    with conn.cursor() as cur:
        cur.execute("SELECT link_id, url FROM mailchimp_campaign_links WHERE campaign_id = %s", (campaign_id,))
        links = cur.fetchall()

    updates = []
    for link_id, url in links:
        text = anchor_map.get(url)
        if not text:
            try:
                u = urlparse(url)
                if u.query:
                    stripped = f"{u.scheme}://{u.netloc}{u.path}"
                    text = anchor_map.get(stripped)
            except Exception:
                pass
        domain, utm_source, utm_medium, utm_campaign = _parse_utm(url)
        updates.append((text, domain, utm_source, utm_medium, utm_campaign, campaign_id, link_id))

    if updates:
        with conn.cursor() as cur:
            cur.executemany("""
                UPDATE mailchimp_campaign_links
                SET link_text = %s,
                    domain = %s,
                    utm_source = %s,
                    utm_medium = %s,
                    utm_campaign = %s
                WHERE campaign_id = %s AND link_id = %s
            """, updates)
        conn.commit()
    return len(updates)

@router.post("/campaigns/{campaign_id}/links/resolve")
def resolve_link_text_for_campaign(campaign_id: str, conn = Depends(get_conn)):
    try:
        updated = resolve_link_text_for_campaign_internal(conn, campaign_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Mailchimp content fetch failed: {e}")
    return {"ok": True, "links_updated": updated}
