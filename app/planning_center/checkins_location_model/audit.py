# app/planning_center/checkins_location_model/audit.py
from __future__ import annotations
from typing import Iterable, Dict, Any, List

async def write_skip_audit(conn, svc_date, rows: Iterable[Dict[str, Any]]) -> int:
    """
    Inserts skip rows into checkins_skip_audit with a safe schema:
      (svc_date date, reason text, payload jsonb)
    If your table differs, this function will catch and just return 0 (but logs are preserved).
    """
    rows = list(rows)
    if not rows:
        return 0
    try:
        await conn.executemany(
            """
            INSERT INTO checkins_skip_audit (svc_date, reason, payload)
            VALUES ($1, $2, $3::jsonb)
            """,
            [(svc_date, r["reason"], r) for r in rows],
        )
        return len(rows)
    except Exception:
        # don't fail the ingestion over audit schema diffs
        return 0
