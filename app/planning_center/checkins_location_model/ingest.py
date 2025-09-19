# ============================
# app/planning_center/checkins_location_model/ingest.py
# ============================
from __future__ import annotations

from typing import Any, Dict, Tuple, Optional, List
from datetime import datetime
import logging
import json
import asyncpg

from .derive import _ts, derive_service_bucket, derive_ministry_key
from .locations import upsert_locations_from_payload  # keeps pco_locations fresh from each page

log = logging.getLogger(__name__)
_SQL_VERSION = "v13"

RAW_UPSERT = f"""
/* raw {_SQL_VERSION} */
INSERT INTO pco_checkins_raw (
  checkin_id, person_id, event_id, location_id,
  service_bucket, starts_at, created_at_pco,
  new_flag, ministry_key, location_name_at_checkin,
  person_created_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT (checkin_id) DO UPDATE SET
  person_id                   = EXCLUDED.person_id,
  event_id                    = EXCLUDED.event_id,
  location_id                 = EXCLUDED.location_id,
  service_bucket              = EXCLUDED.service_bucket,
  starts_at                   = EXCLUDED.starts_at,
  created_at_pco              = EXCLUDED.created_at_pco,
  new_flag                    = COALESCE(EXCLUDED.new_flag, pco_checkins_raw.new_flag),
  ministry_key                = COALESCE(EXCLUDED.ministry_key, pco_checkins_raw.ministry_key),
  location_name_at_checkin    = COALESCE(EXCLUDED.location_name_at_checkin, pco_checkins_raw.location_name_at_checkin),
  person_created_at           = COALESCE(EXCLUDED.person_created_at, pco_checkins_raw.person_created_at);
"""

UNPLACED_UPSERT = f"""
/* unplaced {_SQL_VERSION} */
INSERT INTO pco_checkins_unplaced (
  checkin_id, person_id, created_at_pco, reason_codes, details
)
VALUES ($1::text, $2::text, $3::timestamptz, $4::text[], $5::jsonb)
ON CONFLICT (checkin_id) DO UPDATE SET
  reason_codes = EXCLUDED.reason_codes,
  details = EXCLUDED.details
"""


def _build_included_index(payload: Dict[str, Any]) -> Dict[tuple, dict]:
    included: List[dict] = payload.get("included") or []
    return {((o.get("type") or ""), (o.get("id") or "")): o for o in included}


def _safe_rel_id(obj: dict, rel: str, plural: bool = False) -> Optional[str]:
    try:
        rels = obj.get("relationships") or {}
        if not plural:
            data = (rels.get(rel) or {}).get("data")
            if isinstance(data, dict):
                return data.get("id")
            return None
        else:
            data = (rels.get(rel) or {}).get("data") or []
            if data and isinstance(data, list):
                # choose first (primary) location/event_time per check-in
                first = data[0]
                if isinstance(first, dict):
                    return first.get("id")
            return None
    except Exception:
        return None


async def ingest_checkins_payload(
    conn: asyncpg.Connection,
    payload: Dict[str, Any],
    *,
    client,  # PCOCheckinsClient (unused here; kept for signature parity)
    skip_raw: bool = False,
) -> Tuple[int, int]:
    """Ingest a single page of Check-Ins (raw facts only). Returns (placed_count, unplaced_count)."""
    # Keep location dimension in sync from the page
    await upsert_locations_from_payload(conn, payload)

    idx = _build_included_index(payload)

    placed = 0
    unplaced = 0

    for row in payload.get("data") or []:
        try:
            if (row.get("type") or "") != "CheckIn":
                continue

            checkin_id = row.get("id")
            attrs = row.get("attributes") or {}
            created_at_pco = _ts(attrs.get("created_at"))
            person_id = _safe_rel_id(row, "person")
            loc_id = _safe_rel_id(row, "locations", plural=True)
            evt_time_id = _safe_rel_id(row, "event_times", plural=True)

            if not (checkin_id and person_id and created_at_pco and evt_time_id and loc_id):
                reason = []
                if not checkin_id: reason.append("missing_checkin_id")
                if not person_id: reason.append("missing_person")
                if not created_at_pco: reason.append("missing_created_at")
                if not evt_time_id: reason.append("missing_event_time")
                if not loc_id: reason.append("missing_location")
                details = {"row": {"id": checkin_id, "type": row.get("type")}}
                await conn.execute(
                    UNPLACED_UPSERT,
                    checkin_id or f"_MISSING_{datetime.utcnow().timestamp()}",
                    person_id or "",
                    created_at_pco or datetime.utcnow(),
                    reason or ["unknown"],
                    json.dumps(details),
                )
                unplaced += 1
                continue

            evt_time_obj = idx.get(("EventTime", evt_time_id)) or {}
            evt_time_attrs = (evt_time_obj.get("attributes") or {})
            starts_at = _ts(evt_time_attrs.get("starts_at")) or created_at_pco
            service_bucket = derive_service_bucket(evt_time_obj, created_at_pco)

            # event_id via EventTime.relationships.event if present
            event_id = None
            try:
                ev_rel = (evt_time_obj.get("relationships") or {}).get("event") or {}
                ev_data = ev_rel.get("data") or {}
                event_id = ev_data.get("id")
            except Exception:
                event_id = None
            event_id = event_id or "UNKNOWN"

            # Person created_at (for first-time heuristic)
            person_created_at = None
            pco_first = bool(attrs.get("first_time"))
            person_obj = idx.get(("Person", person_id)) or {}
            person_attrs = (person_obj.get("attributes") or {})
            if person_attrs:
                person_created_at = _ts(person_attrs.get("created_at"))

            # Resolve a friendly location name (best-effort)
            loc_name = None
            loc_obj = idx.get(("Location", loc_id)) or {}
            loc_name = (loc_obj.get("attributes") or {}).get("name")

            # Derive ministry_key from location name (nullable OK)
            ministry_key = derive_ministry_key(loc_name or "") or None

            if not skip_raw:
                await conn.execute(
                    RAW_UPSERT,
                    checkin_id,
                    person_id,
                    event_id,
                    loc_id,
                    service_bucket or "",
                    starts_at,
                    created_at_pco,
                    None,  # new_flag computed in rollup; keep NULL here
                    ministry_key,
                    loc_name,
                    person_created_at,
                )
                placed += 1

        except Exception as e:
            log.exception("ingest_checkins_payload error for checkin=%s", row.get("id"))
            try:
                err_details = {
                    "error": str(e),
                    "checkin_id": row.get("id"),
                    "person_id": _safe_rel_id(row, "person"),
                }
                await conn.execute(
                    UNPLACED_UPSERT,
                    row.get("id") or f"_ERR_{datetime.utcnow().timestamp()}",
                    _safe_rel_id(row, "person") or "",
                    _ts((row.get("attributes") or {}).get("created_at")) or datetime.utcnow(),
                    ["exception"],
                    json.dumps(err_details),
                )
                unplaced += 1
            except Exception:
                pass

    return placed, unplaced

