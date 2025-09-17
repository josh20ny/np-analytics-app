# app/planning_center/checkins_location_model/ingest.py
from __future__ import annotations

from typing import Any, Dict, Tuple, Optional, Sequence, List, Set
from datetime import datetime, timezone, date
import logging
import asyncpg
import json
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)

# Bump to bust any prepared-statement caches after deploys
_SQL_VERSION = "v11"

LOCAL_TZ = ZoneInfo("America/Chicago")

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
    """Execute SQL and print succinct diagnostics on error."""
    try:
        return await conn.execute(sql, *params)
    except Exception as e:
        print("=== EXECUTE FAILED ===")
        print("SQL:", sql.splitlines()[0], "...")
        print("ERROR:", repr(e))
        print("PARAM_TYPES:", [type(p).__name__ for p in params])
        raise

def _s(v: Optional[Any], default: str = "") -> str:
    return v if isinstance(v, str) else default

def _normalize_service_bucket(label: Optional[str]) -> str:
    """
    Canonicalize to '9:30 AM' | '11:00 AM' | '4:30 PM'.
    Accept legacy numeric strings and 'unknown'.
    """
    if not isinstance(label, str):
        return ""
    x = label.strip()
    if x in ("930", "9:30", "9:30 AM"):
        return "9:30 AM"
    if x in ("1100", "11:00", "11:00 AM"):
        return "11:00 AM"
    if x in ("1630", "4:30", "4:30 PM"):
        return "4:30 PM"
    if x.lower() == "unknown":
        return ""
    return x

def _normalize_ministry_key(k: Optional[str]) -> str:
    if not isinstance(k, str):
        return "UNKNOWN"
    low = k.strip().lower()
    if low in ("insideout", "inside out", "io"): return "InsideOut"
    if low in ("upstreet", "up street"):          return "UpStreet"
    if low in ("waumbaland", "waumba", "waumba land"): return "WaumbaLand"
    if low == "transit":                           return "Transit"
    return k.strip() or "UNKNOWN"

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

def _person_created_dates_local(included: List[dict]) -> Dict[str, date]:
    """
    Map person_id -> America/Chicago creation DATE from Person.created_at in page 'included'.
    """
    from .derive import _ts
    out: Dict[str, date] = {}
    for item in included or []:
        if (item.get("type") or "") == "Person":
            pid = item.get("id")
            iso = ((item.get("attributes") or {}).get("created_at"))
            dt = _ts(iso)
            if pid and dt:
                out[pid] = dt.astimezone(LOCAL_TZ).date()
    return out

async def _already_marked_new_for_day(conn: asyncpg.Connection, pid: str, d: date) -> bool:
    """
    Avoid marking 'new' multiple times if we re-run or ingest in multiple passes.
    """
    q = """
    SELECT 1
    FROM pco_checkins_raw
    WHERE person_id = $1
      AND (starts_at AT TIME ZONE 'America/Chicago')::date = $2
      AND new_flag
    LIMIT 1
    """
    return (await conn.fetchval(q, pid, d)) is not None

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

    from .derive import (
        _ts,
        choose_event_time_for_checkin,
        derive_service_bucket,
        derive_ministry_key,
        derive_ministry_from_chain,
        service_from_location_chain,
    )
    from .locations import upsert_locations_from_payload

    placed = 0
    unplaced = 0

    # Keep locations dimension in sync from the page
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

    # Person.created_at map (local DATE) & a run-local de-dupe set
    person_created_local = _person_created_dates_local(included)
    run_new_seen: Set[tuple[str, date]] = set()

    # --- Helper: fetch ancestor chain for a location_id (root at the end)
    CHAIN_SQL = """
    WITH RECURSIVE chain AS (
      SELECT l.location_id, l.parent_id, l.name, l.location_id AS start_id, 0 AS depth
      FROM pco_locations l WHERE l.location_id = ANY($1::text[])
      UNION ALL
      SELECT p.location_id, p.parent_id, p.name, chain.start_id, chain.depth + 1
      FROM pco_locations p
      JOIN chain ON chain.parent_id = p.location_id
    )
    SELECT start_id, array_agg(name ORDER BY depth ASC) AS chain
    FROM chain
    GROUP BY start_id
    """

    async def chains_for(ids: List[str]) -> Dict[str, List[str]]:
        if not ids:
            return {}
        rows = await conn.fetch(CHAIN_SQL, ids)
        return {r["start_id"]: r["chain"] for r in rows}

    chain_cache: Dict[str, List[str]] = {}

    def chain_from_included(idx_map: Dict[tuple, dict], loc_id: Optional[str]) -> List[str]:
        """Build a name chain [root..leaf] by walking included Location.parent links."""
        if not loc_id:
            return []
        names: List[str] = []
        seen: set[str] = set()
        cur = loc_id
        steps = 0
        while isinstance(cur, str) and cur and cur not in seen and steps < 12:
            seen.add(cur); steps += 1
            loc = idx_map.get(("Location", cur))
            if not isinstance(loc, dict):
                break
            nm = ((loc.get("attributes") or {}).get("name") or "").strip()
            if nm:
                names.append(nm)
            rel = (loc.get("relationships") or {}).get("parent") or {}
            cur = ((rel.get("data") or {}) or {}).get("id")
        names.reverse()
        return names

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
                pass

        # Pull location name from included or fetched
        loc_obj = idx.get(("Location", location_id)) if location_id else None
        location_name = ((loc_obj or {}).get("attributes") or {}).get("name")
        if (not location_name) and fetched_locs:
            try:
                location_name = (fetched_locs[0].get("attributes") or {}).get("name") or location_name
            except Exception:
                pass

        # Resolve full ancestor chain (for ministry + Transit service)
        chain: List[str] = []
        if location_id:
            cached = chain_cache.get(location_id)
            if cached is not None:
                chain = cached
            else:
                try:
                    chain_map = await chains_for([location_id])
                    chain = chain_map.get(location_id) or []
                except Exception:
                    chain = []
                if not chain:
                    chain = chain_from_included(idx, location_id)
                if not chain and location_name:
                    chain = [location_name]
                chain_cache[location_id] = chain

        root_name = chain[-1] if chain else ""

        # --- EVENT TIME / SERVICE BUCKET (only call /check_in_times if needed) --
        # 1) Try included evt_time id first (rare), else choose from schedule
        evt_time = idx.get(("EventTime", evt_time_id)) if evt_time_id else None
        if not evt_time:
            evt_time = choose_event_time_for_checkin(idx, created_at)

        # 2) Derive from the best evt_time we have so far (normalized to AM/PM buckets)
        svc_label = derive_service_bucket(evt_time, created_at) or ""

        # 2b) Transit models service as a location (e.g., "9:30 Service") → prefer chain hint
        if not svc_label:
            chain_hint = service_from_location_chain(chain or [])
            if chain_hint:
                svc_label = chain_hint

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

        # Final normalized bucket label
        svc_label = _normalize_service_bucket(svc_label)

        # Compute ministry from the *chain root* then normalize
        ministry_key = (
            derive_ministry_from_chain(chain) or
            derive_ministry_key(" > ".join(chain) or root_name or location_name or "") or
            "UNKNOWN"
        )
        ministry_key = _normalize_ministry_key(ministry_key)

        # --- NEW FLAG (strict Person.created_at local-date equality)
        new_flag = False
        if person_id:
            svc_local_date = created_at.astimezone(LOCAL_TZ).date()
            p_created_date = person_created_local.get(person_id)
            if p_created_date == svc_local_date:
                # run-level de-dupe first
                key = (person_id, svc_local_date)
                if key not in run_new_seen:
                    # cross-run de-dupe (DB) to be safe
                    if not await _already_marked_new_for_day(conn, person_id, svc_local_date):
                        new_flag = True
                        run_new_seen.add(key)

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
            created_at,                   # $6::timestamptz (we use check-in timestamp as starts_at)
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
    print("=== UNPLACED SUMMARY ===", reason_counts,
          "| persons_included:", sum(1 for i in (payload.get("included") or []) if (i.get("type") or "") == "Person"))
    return placed, unplaced
