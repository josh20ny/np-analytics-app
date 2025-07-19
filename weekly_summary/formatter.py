# weekly_summary/formatter.py

from datetime import date

def calc_yoy(curr: int, prev: int) -> float | None:
    """
    Returns ((curr - prev) / prev * 100), or None if prev is zero or missing.
    """
    if prev and isinstance(prev, (int, float)):
        return (curr - prev) / prev * 100
    return None

def format_summary(latest: dict[str, dict[str, dict]]) -> str:
    lines = []

    # ── 1) Attendance tables with YoY ─────────────────────────────────────────────
    for key, display in [
        ("AdultAttendance",     "Adult Attendance"),
        ("WaumbaLandAttendance","WaumbaLand Attendance"),
        ("UpStreetAttendance",  "UpStreet Attendance"),
        ("TransitAttendance",   "Transit Attendance"),
    ]:
        data    = latest.get(key, {})
        cur     = data.get("current", {})
        pri     = data.get("prior", {})

        # date for all three rows (they share the same date)
        dt      = cur.get("date")
        date_str= dt.strftime("%b %d") if hasattr(dt, "strftime") else str(dt)

        # pull metrics
        a930    = cur.get("attendance_930",   0)
        p930    = pri.get("attendance_930",   0)
        a1100   = cur.get("attendance_1100",  0)
        p1100   = pri.get("attendance_1100",  0)
        tot     = cur.get("total_attendance", 0)
        ptot    = pri.get("total_attendance", 0)
        yoytot  = calc_yoy(tot, ptot)

        # build strings with YoY%
        def fmt(val, yoy):
            if yoy is None:
                return f"{val:,}"
            return f"{val:,} ({yoy:+.1f}% YoY)"

        lines.append(
            f"*{display}*: "
            f"9:30 = {fmt(a930,   None)}, "
            f"11:00 = {fmt(a1100, None)}, "
            f"Total = {fmt(tot,   yoytot)}"
        )

    # ── 2) Single-service InsideOut with YoY ────────────────────────────────────────
    io_data = latest.get("InsideOutAttendance", {})
    cur_io  = io_data.get("current", {})
    pri_io  = io_data.get("prior", {})

    dt_io   = cur_io.get("date")
    date_str= dt_io.strftime("%b %d") if hasattr(dt_io, "strftime") else str(dt_io)
    tot_io  = cur_io.get("total_attendance", 0)
    pt_io   = pri_io.get("total_attendance", 0)
    yoy_io  = calc_yoy(tot_io, pt_io)

    if yoy_io is None:
        io_str = f"{tot_io:,}"
    else:
        io_str = f"{tot_io:,} ({yoy_io:+.1f}% YoY)"

    lines.append(f"*InsideOut Attendance*: Total = {io_str}")

    # ── 3) The rest of your tables (unchanged) ────────────────────────────────────
    skip = {
      "AdultAttendance",
      "WaumbaLandAttendance",
      "UpStreetAttendance",
      "TransitAttendance",
      "InsideOutAttendance",
    }
    for label, data in latest.items():
        if label in skip or not data.get("current"):
            continue
        row = data["current"]

        if label == "Livestreams":
            ts       = row.get("published_at")
            ts_str   = ts.strftime("%b %d") if hasattr(ts, "strftime") else ts
            views    = row.get("initial_views", 0)
            lines.append(f"*Livestreams*: views = {views:,}")

        elif label == "GroupsSummary":
            d        = row.get("date")
            d_str    = d.strftime("%b %d") if hasattr(d, "strftime") else str(d)
            groups   = row.get("number_of_groups", 0)
            lines.append(f"*Groups Summary*: total groups = {groups:,}")

        else:
            # fallback: show all numeric fields
            dt = next((row[k] for k in ("date","week_end","published_at") if k in row), None)
            date_str = dt.strftime("%b %d") if hasattr(dt, "strftime") else str(dt)
            nums = {k:v for k,v in row.items() if isinstance(v,(int,float))}
            vals = ", ".join(f"{k}={v:,}" for k,v in nums.items())
            lines.append(f"*{label}*: {vals}")


    # ── 4) Wrap it all up ─────────────────────────────────────────────────────────
    header = f"📊 *Snapshot for* Sunday, {date_str}"
    body   = "\n".join(f"- {l}" for l in lines)
    footer = "\n\n🔗 Full dashboard: https://np-analytics-dashboard.onrender.com"

    return f"{header}\n{body}{footer}"
