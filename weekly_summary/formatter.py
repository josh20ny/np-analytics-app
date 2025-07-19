# weekly_summary/formatter.py

from datetime import date

def calc_yoy(curr: int, prev: int) -> float | None:
    if prev and isinstance(prev, (int, float)):
        return (curr - prev) / prev * 100
    return None

def format_summary(latest: dict[str, dict[str, dict]], mailchimp_rows: list[dict] = []) -> str:
    lines = []

    # ── 1) Attendance tables with YoY ─────────────────────────────
    for key, display in [
        ("AdultAttendance",     "Adult Attendance"),
        ("WaumbaLandAttendance","WaumbaLand Attendance"),
        ("UpStreetAttendance",  "UpStreet Attendance"),
        ("TransitAttendance",   "Transit Attendance"),
    ]:
        data    = latest.get(key, {})
        cur     = data.get("current", {})
        pri     = data.get("prior", {})
        dt      = cur.get("date")
        date_str= dt.strftime("%b %d") if hasattr(dt, "strftime") else str(dt)

        a930    = cur.get("attendance_930",   0)
        p930    = pri.get("attendance_930",   0)
        a1100   = cur.get("attendance_1100",  0)
        p1100   = pri.get("attendance_1100",  0)
        tot     = cur.get("total_attendance", 0)
        ptot    = pri.get("total_attendance", 0)
        yoytot  = calc_yoy(tot, ptot)

        def fmt(val, yoy):
            if yoy is None:
                return f"{val:,}"
            return f"{val:,} ({yoy:+.1f}% YoY)"

        lines.append(
            f"**{display}**: "
            f"9:30 = {fmt(a930, None)}, "
            f"11:00 = {fmt(a1100, None)}, "
            f"Total = {fmt(tot, yoytot)}"
        )

    # ── 2) InsideOut ─────────────────────────────────────────────
    io_data = latest.get("InsideOutAttendance", {})
    cur_io  = io_data.get("current", {})
    pri_io  = io_data.get("prior", {})
    dt_io   = cur_io.get("date")
    date_str= dt_io.strftime("%b %d") if hasattr(dt_io, "strftime") else str(dt_io)
    tot_io  = cur_io.get("total_attendance", 0)
    pt_io   = pri_io.get("total_attendance", 0)
    yoy_io  = calc_yoy(tot_io, pt_io)

    io_str = f"{tot_io:,} ({yoy_io:+.1f}% YoY)" if yoy_io is not None else f"{tot_io:,}"
    lines.append(f"**InsideOut Attendance**: Total = {io_str}")

    # ── 3) Mailchimp Summary (all 5 audiences) ─────────────
    if mailchimp_rows:
        row = mailchimp_rows[0]
        week_ending = row.get("week_end")
        week_str = week_ending.strftime("%b %d") if hasattr(week_ending, "strftime") else str(week_ending)
        lines.append(f"**Mailchimp Summary** (Week Ending {week_str}):")

        for r in mailchimp_rows:
            name = r["audience_name"]
            num  = r["email_count"]
            open_rate = round(r["avg_open_rate"], 2)
            click_rate = round(r["avg_click_rate"], 3)
            lines.append(f"  - {name}: {num} emails, {open_rate:.1f}% open, {click_rate:.1f}% click")


    # ── 4) The rest of the tables ─────────────────────────────────
    skip = {
      "AdultAttendance", "WaumbaLandAttendance", "UpStreetAttendance",
      "TransitAttendance", "InsideOutAttendance", "MailchimpSummary"
    }

    for label, data in latest.items():
        if label in skip or not data.get("current"):
            continue
        row = data["current"]
        dt = next((row.get(k) for k in ("date", "week_end", "published_at") if k in row), None)
        date_str = dt.strftime("%b %d") if hasattr(dt, "strftime") else str(dt)
        nums = {k:v for k,v in row.items() if isinstance(v,(int,float))}
        vals = ", ".join(f"{k}={v:,}" for k,v in nums.items())
        lines.append(f"**{label}**: {vals}")

    # ── 5) Final wrap-up ─────────────────────────────────────────
    header = f"# 📊 Snapshot for Sunday, {date_str}"
    body   = "\n".join(f"- {l}" for l in lines)
    footer = "\n\n## 🔗 Full dashboard: https://np-analytics-dashboard.onrender.com"

    return f"{header}\n{body}{footer}"
