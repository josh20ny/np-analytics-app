# ============================
# app/planning_center/checkins_location_model/client.py
# ============================
from __future__ import annotations
import logging
from typing import Any, AsyncIterator, Dict, Optional, List, Tuple
from contextlib import asynccontextmanager

import asyncpg
import httpx

log = logging.getLogger(__name__)
API_BASE = "https://api.planningcenteronline.com/check-ins/v2"


# --- DB acquire helper ---
@asynccontextmanager
async def acquire(db_or_pool: Any):  # pool or connection
    if hasattr(db_or_pool, "acquire") and hasattr(db_or_pool, "release"):
        conn = await db_or_pool.acquire()
        try:
            yield conn
        finally:
            try:
                await db_or_pool.release(conn)
            except Exception:
                try:
                    await conn.close()
                except Exception:
                    pass
        return

    if isinstance(db_or_pool, asyncpg.Connection):
        yield db_or_pool
        return

    raise RuntimeError("Expected asyncpg Pool or Connection for DB access")


class PCOCheckinsClient:
    def __init__(self, get_bearer):
        self._get_bearer = get_bearer

    async def _auth_header(self) -> Dict[str, str]:
        v = await self._maybe_await(self._get_bearer())
        if isinstance(v, dict):
            if "Authorization" in v:  return v
            if "authorization" in v:  return {"Authorization": v["authorization"]}
            tok = v.get("access_token")
            if tok: return {"Authorization": f"Bearer {tok}"}
            raise RuntimeError("OAuth getter returned dict without Authorization/access_token")
        if isinstance(v, str):
            return {"Authorization": v if v.lower().startswith("bearer ") else f"Bearer {v}"}
        raise RuntimeError(f"OAuth getter returned unsupported type: {type(v).__name__}")

    async def _maybe_await(self, v):
        # await coroutines; otherwise pass through
        if callable(getattr(v, "__await__", None)):
            return await v
        return v

    async def _maybe_await(self, v):
        if callable(getattr(v, "__await__", None)):
            return await v
        return v

    async def paginate_check_ins(
        self,
        *args,  # tolerate accidental positional
        event_id: Optional[str] = None,
        created_at_gte: Optional[str] = None,
        created_at_lte: Optional[str] = None,
        per_page: int = 100,
        **kwargs,  # tolerate stray kwargs (e.g., future flags)
    ) -> AsyncIterator[Dict[str, Any]]:
        # If someone passed a single dict positionally (oops), merge it in
        if args:
            if len(args) == 1 and isinstance(args[0], dict):
                d = args[0]
                event_id       = d.get("event_id", event_id)
                created_at_gte = d.get("created_at_gte", created_at_gte)
                created_at_lte = d.get("created_at_lte", created_at_lte)
                per_page       = d.get("per_page", per_page)
            else:
                # ignore unexpected positional args to avoid TypeError
                pass

        params: Dict[str, Any] = {"per_page": per_page}
        if event_id:
            params["where[event_id]"] = event_id
        if created_at_gte:
            params["where[created_at][gte]"] = created_at_gte
        if created_at_lte:
            params["where[created_at][lte]"] = created_at_lte

        async with httpx.AsyncClient(base_url=API_BASE, timeout=30) as http:
            while True:
                hdrs = await self._auth_header()
                r = await http.get("/check_ins", headers=hdrs, params=params)
                r.raise_for_status()
                payload = r.json()
                yield payload
                nxt = (payload.get("meta") or {}).get("next") or {}
                if "offset" not in nxt:
                    break
                params["offset"] = nxt["offset"]

    async def paginate_locations(
        self,
        *args,
        event_id: Optional[str] = None,
        per_page: int = 100,
        include: str = "parent,event",
        **kwargs,
    ) -> AsyncIterator[Dict[str, Any]]:
        if args:
            if len(args) == 1 and isinstance(args[0], dict):
                d = args[0]
                event_id = d.get("event_id", event_id)
                per_page = d.get("per_page", per_page)
                include  = d.get("include", include)
            else:
                pass

        params: Dict[str, Any] = {"per_page": per_page, "include": include}
        if event_id:
            params["where[event_id]"] = event_id

        async with httpx.AsyncClient(base_url=API_BASE, timeout=30) as http:
            while True:
                hdrs = await self._auth_header()
                r = await http.get("/locations", headers=hdrs, params=params)
                r.raise_for_status()
                payload = r.json()
                yield payload
                nxt = (payload.get("meta") or {}).get("next") or {}
                if "offset" not in nxt:
                    break
                params["offset"] = nxt["offset"]

    async def get_checkin_locations(self, checkin_id: str) -> Tuple[Optional[str], List[Dict[str, Any]]]:
        """
        Return (first_location_id_or_none, full_locations_list) for a check-in.
        Uses /check_ins/{id}/locations (plural).
        """
        # If your class already keeps an AsyncClient, prefer it; otherwise open a short-lived one.
        try:
            http: httpx.AsyncClient = getattr(self, "http", None) or getattr(self, "_http", None)
        except Exception:
            http = None

        headers = await self._auth_header()  # you already have this
        if http:
            r = await http.get(f"{API_BASE}/check_ins/{checkin_id}/locations", headers=headers, timeout=30.0)
        else:
            async with httpx.AsyncClient(timeout=30.0) as tmp:
                r = await tmp.get(f"{API_BASE}/check_ins/{checkin_id}/locations", headers=headers)
        r.raise_for_status()
        js = r.json()
        locs: List[Dict[str, Any]] = js.get("data") or []
        loc_id = locs[0]["id"] if locs and isinstance(locs[0], dict) else None
        return loc_id, locs

    async def get_checkin_times(self, checkin_id: str) -> List[Dict[str, Any]]:
        """
        Return the list of check-in time objects for a check-in.
        Uses /check_ins/{id}/check_in_times (plural).
        """
        try:
            http: httpx.AsyncClient = getattr(self, "http", None) or getattr(self, "_http", None)
        except Exception:
            http = None

        headers = await self._auth_header()
        if http:
            r = await http.get(f"{API_BASE}/check_ins/{checkin_id}/check_in_times", headers=headers, timeout=30.0)
        else:
            async with httpx.AsyncClient(timeout=30.0) as tmp:
                r = await tmp.get(f"{API_BASE}/check_ins/{checkin_id}/check_in_times", headers=headers)
        r.raise_for_status()
        return r.json().get("data") or []