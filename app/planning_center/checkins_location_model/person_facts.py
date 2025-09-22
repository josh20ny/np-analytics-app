# app/planning_center/checkins_location_model/person_facts.py
from __future__ import annotations
from typing import Iterable, Dict, Any, List, Tuple

def build_person_fact_rows(normalized_checkins: Iterable[Dict[str, Any]]) -> List[Tuple]:
    """
    Shape rows for f_checkins_person UPSERT.

    Expect each item to include keys (matching routes.py query):
      person_id (str)
      svc_date (date)
      ministry (str)
      service_time (str)
      event_id (str|None)
      campus_id (str|None)
      created_at_utc (naive UTC timestamp)

    Return tuples exactly in INSERT column order:
      (person_id, svc_date, service_time, ministry, event_id, campus_id, created_at_utc)
    """
    rows: List[Tuple] = []
    for c in normalized_checkins:
        rows.append((
            c["person_id"],
            c["svc_date"],
            c["service_time"],
            c["ministry"],
            c.get("event_id"),
            c.get("campus_id"),
            c["created_at_utc"],
        ))
    return rows
