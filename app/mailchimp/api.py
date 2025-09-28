# app/mailchimp/api.py  (shared HTTP helpers)
import os, requests
from typing import Dict, Any, Iterable, Optional

MC_BASE = f"https://{os.environ['MAILCHIMP_SERVER_PREFIX']}.api.mailchimp.com/3.0"
AUTH = ("anystring", os.environ["MAILCHIMP_API_KEY"])

def mc_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    r = requests.get(f"{MC_BASE}{path}", auth=AUTH, params=params or {}, timeout=30)
    r.raise_for_status()
    return r.json() or {}

def mc_paginate(path: str, params: Dict[str, Any], array_key: str) -> Iterable[Dict[str, Any]]:
    offset = 0
    count = params.get("count", 1000)
    while True:
        page = mc_get(path, {**params, "offset": offset, "count": count})
        items = page.get(array_key, [])
        if not items: break
        for it in items: yield it
        offset += len(items)
        if len(items) < count: break
