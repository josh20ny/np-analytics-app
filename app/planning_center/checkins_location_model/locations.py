from __future__ import annotations
import logging
from typing import Any, Dict, Iterable, Optional
from datetime import datetime, timezone
import asyncpg

log = logging.getLogger(__name__)

def _ts(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

LOC_UPSERT = """
INSERT INTO pco_locations (
  location_id, event_id, parent_id, name, kind, child_or_adult,
  grade_min, grade_max, age_min_months, age_max_months, gender,
  position, updated_at_pco
) VALUES (
  $1,$2,$3,$4,$5,$6,
  $7,$8,$9,$10,$11,
  $12,$13
)
ON CONFLICT (location_id) DO UPDATE
SET event_id = EXCLUDED.event_id,
    parent_id = EXCLUDED.parent_id,
    name = EXCLUDED.name,
    kind = EXCLUDED.kind,
    child_or_adult = EXCLUDED.child_or_adult,
    grade_min = EXCLUDED.grade_min,
    grade_max = EXCLUDED.grade_max,
    age_min_months = EXCLUDED.age_min_months,
    age_max_months = EXCLUDED.age_max_months,
    gender = EXCLUDED.gender,
    position = EXCLUDED.position,
    updated_at_pco = EXCLUDED.updated_at_pco
"""

PATH_UPSERT = """
INSERT INTO pco_location_paths (ancestor_id, descendant_id, depth)
VALUES ($1,$2,$3)
ON CONFLICT (ancestor_id, descendant_id) DO UPDATE
SET depth = LEAST(pco_location_paths.depth, EXCLUDED.depth)
"""

async def upsert_locations_from_payload(conn: asyncpg.Connection, payload: Dict[str, Any]) -> None:
    included: Iterable[Dict[str, Any]] = payload.get("included") or []
    for obj in included:
        if (obj.get("type") or "").lower() != "location":
            continue
        a = obj.get("attributes") or {}
        rid = obj.get("id")
        rel = obj.get("relationships") or {}
        parent_id = ((rel.get("parent") or {}).get("data") or {}).get("id")
        raw_event_id = ((rel.get("event") or {}).get("data") or {}).get("id")

        # 👇 Ensure NOT NULL for event_id
        event_id = raw_event_id or "GLOBAL"

        updated_dt = _ts(a.get("updated_at")) or _ts(a.get("updated_at_pco"))
        if updated_dt is None:
            updated_dt = datetime.now(timezone.utc)

        await conn.execute(
            LOC_UPSERT,
            rid,
            event_id,                # <- now guaranteed non-null
            parent_id,
            a.get("name"),
            a.get("kind"),
            a.get("child_or_adult"),
            a.get("grade_min"),
            a.get("grade_max"),
            a.get("age_min_in_months") or a.get("age_min_months"),
            a.get("age_max_in_months") or a.get("age_max_months"),
            a.get("gender"),
            a.get("position"),
            updated_dt,
        )

    # self + 1-step parent path rows
    for obj in included:
        if (obj.get("type") or "").lower() != "location":
            continue
        rid = obj.get("id")
        rel = obj.get("relationships") or {}
        parent_id = ((rel.get("parent") or {}).get("data") or {}).get("id")
        await conn.execute(PATH_UPSERT, rid, rid, 0)
        if parent_id:
            await conn.execute(PATH_UPSERT, parent_id, rid, 1)