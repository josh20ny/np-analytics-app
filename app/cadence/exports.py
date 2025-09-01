from __future__ import annotations
from datetime import date
from fastapi import Response
from typing import Iterable
from app.utils.common import get_last_sunday_cst
from . import dao

def export_downshifts_csv(week_end: str | None) -> Response:
    wk = date.fromisoformat(week_end) if week_end else get_last_sunday_cst()
    rows = dao.downshifts_rows(wk)
    lines = ["person_id,name,email,from_tier,to_tier,campus_id"]
    for r in rows:
        pid, first, last, email, from_tier, to_tier, campus_id = r
        name = f"{first or ''} {last or ''}".strip()
        parts = [str(pid), name, (email or ""), str(from_tier), str(to_tier), str(campus_id or "")]
        # naive CSV (fields are simple)
        lines.append(",".join(x.replace(",", " ") for x in parts))
    csv = "\n".join(lines)
    return Response(
        content=csv,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=downshifts_{wk}.csv"},
    )

def export_nla_csv(week_end: str | None) -> Response:
    wk = date.fromisoformat(week_end) if week_end else get_last_sunday_cst()
    rows = dao.nla_rows(wk)
    lines = ["person_id,name,email,first_seen_any,last_attend,last_give,last_serve,last_group,last_any"]
    for (pid, name, email, first_any, last_att, last_give, last_srv, last_grp, last_any) in rows:
        vals = [
            str(pid), name or "", email or "",
            (first_any or "") if isinstance(first_any, str) else (first_any.isoformat() if first_any else ""),
            (last_att or "") if isinstance(last_att, str) else (last_att.isoformat() if last_att else ""),
            (last_give or "") if isinstance(last_give, str) else (last_give.isoformat() if last_give else ""),
            (last_srv or "") if isinstance(last_srv, str) else (last_srv.isoformat() if last_srv else ""),
            (last_grp or "") if isinstance(last_grp, str) else (last_grp.isoformat() if last_grp else ""),
            (last_any or "") if isinstance(last_any, str) else (last_any.isoformat() if last_any else ""),
        ]
        lines.append(",".join(v.replace(",", " ") for v in vals))
    csv = "\n".join(lines)
    return Response(
        content=csv,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=nla_{wk}.csv"},
    )
