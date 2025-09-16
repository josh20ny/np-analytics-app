# app/planning_center/checkins_location_model/ingest.py
from typing import Any, Dict, Tuple, Optional, Sequence, List
from datetime import datetime, timezone
import logging
import asyncpg
import json

log = logging.getLogger(__name__)

# Bump to bust any prepared-statement caches after deploys
_SQL_VERSION = "v9"

# --- SQL ---------------------------------------------------------------------------

UNPLACED_UPSERT = f"""\
/* unplaced {_SQL_VERSION} */
INSERT INTO pco_checkins_unplaced (
  checkin_id, person_id, created_at_pco, reason_codes, details
)
VALUES ($1::text, $2::text, $3::timestamptz, $4::text[], $5::jsonb)
ON CONFLICT (checkin_id) DO UPDATE
SET reason_codes = EXCLUDED.reason_codes,
    details = EXCLUDED.details
"""

RAW_UPSERT = f"""\
/* raw {_SQL_VERSION} */
INSERT INTO pco_checkins_raw (
  checkin_id, person_id, event_id, location_id, service_bucket,
  starts_at, created_at_pco, new_flag, ministry_key, location_name_at_checkin
)
VALUES (
  $1::text, $2::text, $3::text, $4::text, $5::text,
  $6::timestamptz, $7::timestamptz, $8::bool, $9::text, $10::text
)
ON CONFLICT (checkin_id) DO UPDATE
SET person_id = EXCLUDED.person_id,
    event_id = EXCLUDED.event_id,
    location_id = EXCLUDED.location_id,
    service_bucket = EXCLUDED.service_bucket,
    starts_at = EXCLUDED.starts_at,
    created_at_pco = EXCLUDED.created_at_pco,
    new_flag = EXCLUDED.new_flag,
    ministry_key = EXCLUDED.ministry_key,
    location_name_at_checkin = EXCLUDED.location_name_at_checkin
"""

# --- helpers ----------------------------------------------------------------------

def _preview(v: object, maxlen: int = 240) -> str:
    try:
        if isinstance(v, (dict, list)):
            s = json.dumps(v)
        else:
            s = str(v)
        return s[:maxlen]
    except Exception:
        return f"<unrepr {type(v).__name__}>"

def _rel_id(rel: dict) -> Optional[str]:
    """Return id from a JSON:API relationship where data may be an object or list."""
    data = (rel or {}).get("data")
    if isinstance(data, dict):
        return data.get("id")
    if isinstance(data, list) and data:
        first = data[0]
        return first.get("id") if isinstance(first, dict) else None
    return None

async def exec_debug(conn: asyncpg.Connection, sql: str, *params: Sequence[object]):
    """
    Quiet execution helper:
      - On success: no console output.
      - On error: prints succinct details, then re-raises.
    """
    try:
        return await conn.execute(sql, *params)
    except Exception as e:
        print("=== EXECUTE FAILED ===")
        print("SQL:", sql.splitlines()[0], "...")  # first line only
        print("ERROR:", repr(e))
        print("PARAM_TYPES:", [type(p).__name__ for p in params])
        raise

def _s(v: Optional[Any], default: str = "") -> str:
    return v if isinstance(v, str) else default

# Optional schema probe (only emits on DEBUG level)
async def _probe_schema(conn: asyncpg.Connection):
    if not log.isEnabledFor(logging.DEBUG):
        return
    try:
        row = await conn.fetchrow("""
            SELECT
              current_database() AS db,
              current_schema()    AS schema,
              (SELECT data_type
                 FROM information_schema.columns
                WHERE table_name='pco_checkins_unplaced'
                  AND column_name='details') AS details_type
        """)
        log.debug("SCHEMA PROBE: %s", dict(row) if row else None)
    except Exception as e:
        log.debug("SCHEMA PROBE FAILED: %r", e)

# --- main -------------------------------------------------------------------------

async def ingest_checkins_payload(
    conn: asyncpg.Connection,
    payload: Dict[str, Any],
    *,
    client,                  # PCOCheckinsClient (passed from routes)
    skip_raw: bool = False,
) -> Tuple[int, int]:
    """
    Ingest a Planning Center Check-Ins page payload.
    Returns: (placed, unplaced)
    """
    await _probe_schema(conn)

    from .derive import _ts, choose_event_time_for_checkin, derive_service_bucket, derive_ministry_key
    from .locations import upsert_locations_from_payload

    placed = 0
    unplaced = 0

    # Keep locations dimension in sync from the page (as before)
    await upsert_locations_from_payload(conn, payload)

    # Reason counters (compact end-of-run summary only)
    reason_counts = {
        "missing_created_at": 0,
        "missing_location": 0,
        "invalid_service_bucket": 0,
        "param_build_error": 0,
    }

    included: List[dict] = payload.get("included") or []
    idx = {((o.get("type") or ""), (o.get("id") or "")): o for o in included}

    for row in (payload.get("data") or []):
        if (row.get("type") or "").lower() != "checkin":
            continue

        a = row.get("attributes") or {}
        r = row.get("relationships") or {}

        checkin_id = row.get("id")
        created_at = _ts(a.get("created_at")) or _ts(a.get("updated_at"))
        if not created_at:
            details = json.dumps({"row": {"id": checkin_id}, "note": "missing created_at"})
            await exec_debug(
                conn, UNPLACED_UPSERT,
                _s(checkin_id, ""), None, datetime.now(timezone.utc),
                ["missing_created_at"], details
            )
            reason_counts["missing_created_at"] += 1
            unplaced += 1
            continue

        # --- Relationship IDs (robust) -----------------------------------------
        person_id   = _rel_id(r.get("person")) or _rel_id(r.get("people"))
        # Some payloads expose event via event_period only; try both.
        event_id    = _rel_id(r.get("event")) or _rel_id(r.get("event_period"))
        location_id = _rel_id(r.get("location")) or _rel_id(r.get("locations"))
        evt_time_id = _rel_id(r.get("event_time")) or _rel_id(r.get("event_times"))

        # --- Resolve LOCATION via subresource BEFORE enforcing guards -----------
        fetched_locs: List[dict] = []
        if (not location_id) and checkin_id:
            try:
                location_id, fetched_locs = await client.get_checkin_locations(checkin_id)
            except Exception:
                # We'll unplace if still missing after this step
                pass

        # Pull location name from included or fetched
        loc_obj = idx.get(("Location", location_id)) if location_id else None
        location_name = ((loc_obj or {}).get("attributes") or {}).get("name")
        if (not location_name) and fetched_locs:
            try:
                location_name = (fetched_locs[0].get("attributes") or {}).get("name") or location_name
            except Exception:
                pass

        # --- EVENT TIME / SERVICE BUCKET (only call /check_in_times if needed) --
        # 1) Try included evt_time id first (rare), else choose from schedule
        evt_time = idx.get(("EventTime", evt_time_id)) if evt_time_id else None
        if not evt_time:
            evt_time = choose_event_time_for_checkin(idx, created_at)

        # 2) Derive from the best evt_time we have so far
        svc_label_val = derive_service_bucket(evt_time, created_at)
        svc_label = svc_label_val if isinstance(svc_label_val, str) else ""

        # 3) Only if we STILL don't have a bucket, call /check_in_times for this checkin
        if not svc_label and checkin_id:
            fetched_times: List[dict] = []
            try:
                fetched_times = await client.get_checkin_times(checkin_id)
            except Exception:
                pass

            if fetched_times:
                chosen = None
                for t in fetched_times:
                    attrs  = t.get("attributes") or {}
                    starts = attrs.get("starts_at")
                    ends   = attrs.get("ends_at") or attrs.get("expires_at")
                    if starts and ends:
                        sdt = _ts(starts)
                        edt = _ts(ends)
                        if sdt and edt and sdt <= created_at <= edt:
                            chosen = {"type": "EventTime", "attributes": {"starts_at": starts, "ends_at": ends}}
                            break
                if chosen:
                    evt_time = chosen
                    svc_label_val = derive_service_bucket(evt_time, created_at)
                    svc_label = svc_label_val if isinstance(svc_label_val, str) else ""

        # Compute ministry and new_flag after we resolved location_name
        ministry_key = derive_ministry_key(location_name or "") or "UNKNOWN"
        new_flag = bool(a.get("first_time")) or bool(a.get("new"))

        # --- Guards (AFTER resolution steps) -----------------------------------
        if (not isinstance(location_id, str)) or (not location_id):
            details = json.dumps({
                "row": {"id": checkin_id},
                "note": "no location after subresource resolution"
            })
            await exec_debug(
                conn, UNPLACED_UPSERT,
                _s(checkin_id, ""), _s(person_id, ""), created_at,
                ["missing_location"], details
            )
            reason_counts["missing_location"] += 1
            unplaced += 1
            continue

        if (not isinstance(svc_label, str)) or (not svc_label):
            details = json.dumps({"row": {"id": checkin_id}, "svc_label_val": _preview(svc_label, 120)})
            await exec_debug(
                conn, UNPLACED_UPSERT,
                _s(checkin_id, ""), _s(person_id, ""), created_at,
                ["invalid_service_bucket"], details
            )
            reason_counts["invalid_service_bucket"] += 1
            unplaced += 1
            continue

        # --- RAW upsert ---------------------------------------------------------
        params = [
            _s(checkin_id, ""),           # $1::text
            _s(person_id, ""),            # $2::text
            _s(event_id, ""),             # $3::text
            _s(location_id, "UNKNOWN"),   # $4::text
            svc_label,                    # $5::text
            created_at,                   # $6::timestamptz (starts_at per derive)
            created_at,                   # $7::timestamptz (created_at_pco)
            bool(new_flag),               # $8::bool
            _s(ministry_key, "UNKNOWN"),  # $9::text
            _s(location_name, ""),        # $10::text
        ]

        if not isinstance(params[4], str):
            details = json.dumps({"row": {"id": checkin_id}, "params_types": [type(p).__name__ for p in params]})
            await exec_debug(
                conn, UNPLACED_UPSERT,
                _s(checkin_id, ""), _s(person_id, ""), created_at,
                ["param_build_error"], details
            )
            reason_counts["param_build_error"] += 1
            unplaced += 1
            continue

        if skip_raw:
            placed += 1
            continue

        await exec_debug(conn, RAW_UPSERT, *params)
        placed += 1

    # Compact end-of-run summary (single line)
    print("=== UNPLACED SUMMARY ===", reason_counts)
    return placed, unplaced