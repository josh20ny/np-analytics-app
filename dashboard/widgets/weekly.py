from __future__ import annotations
import math
from datetime import timedelta
import pandas as pd
import streamlit as st

from data import engine
from widgets.core import format_display_dates

def _fmt_money(n) -> str:
    try:
        return f"${float(n):,.2f}"
    except Exception:
        return "—"

def _fmt_int(n) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return "—"

def _metric_with_yoy(col, label: str, cur_val, prev_val, *, as_money: bool = False):
    # delta text
    if prev_val in (None, 0) or cur_val in (None, float("nan")):
        delta_str = "–"
    else:
        try:
            delta = (float(cur_val) - float(prev_val)) / float(prev_val) * 100.0
            if abs(delta) < 1e-9:
                delta_str = "–"
            elif delta > 0:
                delta_str = f"▲ {delta:.1f}%"
            else:
                delta_str = f"▼ {abs(delta):.1f}%"
        except Exception:
            delta_str = "–"

    # value text
    if as_money:
        val_str = _fmt_money(cur_val) if cur_val is not None else "—"
    else:
        try:
            val_str = _fmt_int(cur_val)
        except Exception:
            val_str = "—"

    col.metric(label, val_str, delta_str)


def _one_row(sql: str, parse_dates=None):
    df = pd.read_sql(sql, engine, parse_dates=parse_dates or [])
    return (None if df.empty else df.iloc[0], df)

def _yoy_value(df: pd.DataFrame, date_col: str, value_col: str, cur_date) -> float | None:
    """
    Find the value ~52 weeks earlier (±3 days wiggle).
    """
    if df is None or df.empty or cur_date is None:
        return None
    lo = pd.to_datetime(cur_date) - pd.Timedelta(days=370)
    hi = pd.to_datetime(cur_date) - pd.Timedelta(days=358)
    prior = df[(df[date_col] >= lo) & (df[date_col] <= hi)].sort_values(date_col).tail(1)
    if prior.empty:
        return None
    v = prior.iloc[0].get(value_col)
    return float(v) if pd.notna(v) else None

def weekly_summary_view():
    st.title("Weekly Summary")

    # ── Adult Attendance (latest) ─────────────────────────────────────────────
    latest_att, att_df = _one_row(
        "SELECT date, total_attendance FROM adult_attendance ORDER BY date DESC LIMIT 1",
        parse_dates=["date"],
    )

    # ── Giving (latest week) ─────────────────────────────────────────────────
    latest_give, give_df = _one_row(
        "SELECT week_end, total_giving, giving_units FROM weekly_giving_summary ORDER BY week_end DESC LIMIT 1",
        parse_dates=["week_end"],
    )

    # ── Front Door (latest week) ─────────────────────────────────────────────
    latest_fd, fd_df = _one_row(
        "SELECT week_end, first_time_checkins FROM front_door_weekly ORDER BY week_end DESC LIMIT 1",
        parse_dates=["week_end"],
    )

    # ── Volunteers (latest) ─────────────────────────────────────────────────
    vol_latest_df = pd.read_sql(
        """
        SELECT week_end, total_volunteers, groups_volunteers, insideout_volunteers,
            transit_volunteers, upstreet_volunteers, waumba_land_volunteers, misc_volunteers
        FROM serving_volunteers_weekly
        ORDER BY week_end DESC
        LIMIT 1
        """,
        engine,
        parse_dates=["week_end"],
    )
    latest_vol = None if vol_latest_df.empty else vol_latest_df.iloc[0]

    # Total Groups (from groups_summary) — be flexible on column name
    groups_latest_df = pd.read_sql(
        "SELECT * FROM groups_summary ORDER BY date DESC LIMIT 1",
        engine, parse_dates=["date"]
    )
    gcol = None
    if not groups_latest_df.empty:
        candidates = ["total_groups", "groups_total", "number_of_groups", "group_count", "groups_count"]
        gcol = next((c for c in candidates if c in groups_latest_df.columns), None)

    groups_all_df = None
    latest_groups = None
    if gcol:
        latest_groups = groups_latest_df.iloc[0]
        groups_all_df = pd.read_sql(f"SELECT date, {gcol} AS total_groups FROM groups_summary", engine, parse_dates=["date"])

    # Preload whole tables for YoY lookups (lightweight)
    att_all = pd.read_sql("SELECT date, total_attendance FROM adult_attendance", engine, parse_dates=["date"])
    give_all = pd.read_sql("SELECT week_end, total_giving, giving_units FROM weekly_giving_summary", engine, parse_dates=["week_end"])
    fd_all  = pd.read_sql("SELECT week_end, first_time_checkins FROM front_door_weekly", engine, parse_dates=["week_end"])
    vol_all = pd.read_sql(
        """
        SELECT week_end, total_volunteers
        FROM serving_volunteers_weekly
        """,
        engine,
        parse_dates=["week_end"],
    )

    # ── KPI row with YoY deltas ──────────────────────────────────────────────
    role = (st.session_state.get("auth_user", {}).get("role") or "viewer").lower()
    viewer = (role == "viewer")

    cols = st.columns(4 if viewer else 6)
    i = 0

    # Adult Attendance
    if latest_att is not None:
        a_cur  = latest_att.get("total_attendance")
        a_prev = _yoy_value(att_all, "date", "total_attendance", latest_att["date"])
        _metric_with_yoy(cols[i], "Adult Attendance", a_cur, a_prev)
    else:
        cols[i].metric("Adult Attendance", "—", "–")
    i += 1

    # (Skip giving tiles entirely for viewer)
    if not viewer and latest_give is not None:
        # Total Giving ($)
        g_cur  = latest_give.get("total_giving")
        g_prev = _yoy_value(give_all, "week_end", "total_giving", latest_give["week_end"])
        _metric_with_yoy(cols[i], "Total Giving", g_cur, g_prev, as_money=True)
        i += 1

        # Giving Units
        u_cur  = latest_give.get("giving_units")
        u_prev = _yoy_value(give_all, "week_end", "giving_units", latest_give["week_end"])
        _metric_with_yoy(cols[i], "Giving Units", u_cur, u_prev)
        i += 1

    # Total Volunteers
    if latest_vol is not None:
        v_cur  = latest_vol.get("total_volunteers")
        v_prev = _yoy_value(vol_all, "week_end", "total_volunteers", latest_vol["week_end"])
        _metric_with_yoy(cols[i], "Total Volunteers", v_cur, v_prev)
    else:
        cols[i].metric("Total Volunteers", "—", "–")
    i += 1

    # Total Groups
    if latest_groups is not None and gcol:
        tg_cur  = latest_groups.get(gcol)
        tg_prev = _yoy_value(groups_all_df.rename(columns={"total_groups": "total_groups"}), "date", "total_groups", latest_groups["date"])
        _metric_with_yoy(cols[i], "Total Groups", tg_cur, tg_prev)
    else:
        cols[i].metric("Total Groups", "—", "–")
    i += 1

    # First-Time Check-ins
    if latest_fd is not None:
        f_cur  = latest_fd.get("first_time_checkins")
        f_prev = _yoy_value(fd_all, "week_end", "first_time_checkins", latest_fd["week_end"])
        _metric_with_yoy(cols[i], "First-Time Check-ins", f_cur, f_prev)
    else:
        cols[i].metric("First-Time Check-ins", "—", "–")


    st.divider()

    # ── Engaged numbers (tiers from snap_person_week, latest week_end) ───────
    spw = pd.read_sql(
        "SELECT week_end, engaged_tier FROM snap_person_week",
        engine,
        parse_dates=["week_end"],
    )
    if not spw.empty:
        latest_we = spw["week_end"].max()
        tiers = spw[spw["week_end"] == latest_we]["engaged_tier"].value_counts().sort_index()
        # Ensure 0..3 present
        data = {f"tier_{i}": int(tiers.get(i, 0)) for i in range(4)}
        data["engaged_1_3_total"] = data["tier_1"] + data["tier_2"] + data["tier_3"]
        st.subheader("Engaged (latest week)")
        st.write(pd.DataFrame([data]))
    else:
        st.info("No engaged rows found.")

    # ── Ministry check-ins + new kids (latest per ministry) ──────────────────
    st.subheader("Check-ins & New Kids (latest per ministry)")
    rows = []

    # InsideOut
    io = pd.read_sql("SELECT date, total_attendance, new_students FROM insideout_attendance ORDER BY date DESC LIMIT 1", engine, parse_dates=["date"])
    if not io.empty:
        rows.append({"ministry": "InsideOut", "total_checkins": int(io.iloc[0]["total_attendance"] or 0), "new_kids": int(io.iloc[0]["new_students"] or 0)})
    # Transit
    tr = pd.read_sql("SELECT date, total_attendance, total_new_kids FROM transit_attendance ORDER BY date DESC LIMIT 1", engine, parse_dates=["date"])
    if not tr.empty:
        rows.append({"ministry": "Transit", "total_checkins": int(tr.iloc[0]["total_attendance"] or 0), "new_kids": int(tr.iloc[0]["total_new_kids"] or 0)})
    # UpStreet
    us = pd.read_sql("SELECT date, total_attendance, total_new_kids FROM upstreet_attendance ORDER BY date DESC LIMIT 1", engine, parse_dates=["date"])
    if not us.empty:
        rows.append({"ministry": "UpStreet", "total_checkins": int(us.iloc[0]["total_attendance"] or 0), "new_kids": int(us.iloc[0]["total_new_kids"] or 0)})
    # Waumba Land
    wl = pd.read_sql("SELECT date, total_attendance, total_new_kids FROM waumbaland_attendance ORDER BY date DESC LIMIT 1", engine, parse_dates=["date"])
    if not wl.empty:
        rows.append({"ministry": "Waumba Land", "total_checkins": int(wl.iloc[0]["total_attendance"] or 0), "new_kids": int(wl.iloc[0]["total_new_kids"] or 0)})

    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True)
    else:
        st.info("No ministry attendance rows found.")

    # ── Volunteers by Ministry (latest week) ─────────────────────────────────────
    st.subheader("Volunteers by Ministry")
    if latest_vol is not None:
        # Nice caption with the week this snapshot represents
        _week_disp = format_display_dates(
            pd.DataFrame({"week_end": [latest_vol["week_end"]]})
        )["week_end"].iloc[0]
        st.caption(f"Week ending {_week_disp}")

        # Build the display table from serving_volunteers_weekly columns
        vol_map = {
            "Groups":      latest_vol.get("groups_volunteers", 0),
            "InsideOut":   latest_vol.get("insideout_volunteers", 0),
            "Transit":     latest_vol.get("transit_volunteers", 0),
            "UpStreet":    latest_vol.get("upstreet_volunteers", 0),
            "Waumba Land": latest_vol.get("waumba_land_volunteers", 0),
            "Misc":        latest_vol.get("misc_volunteers", 0),
        }
        vdf = pd.DataFrame(
            [{"ministry": k, "volunteers": int(v or 0)} for k, v in vol_map.items()]
        )

        # Add a total row (use the table’s total if present; otherwise sum)
        total_val = latest_vol.get("total_volunteers")
        if pd.isna(total_val) or total_val is None:
            total_val = vdf["volunteers"].sum()

        vdf = pd.concat(
            [vdf.sort_values("ministry"),
            pd.DataFrame([{"ministry": "Total", "volunteers": int(total_val)}])],
            ignore_index=True
        )

        st.dataframe(vdf, use_container_width=True)
    else:
        st.info("No serving_volunteers_weekly rows found.")

    # ── Livestreams (pretty cards) ───────────────────────────────────────────────
    st.subheader("Livestreams")
    # 1) Pull the last 5 livestream rows
    ls = pd.read_sql(
        """
        SELECT
        title,
        published_at,
        initial_views,
        views_1_week_later  AS views_1w,
        views_4_weeks_later AS views_4w
        FROM livestreams
        ORDER BY published_at DESC
        LIMIT 5
        """,
        engine,
        parse_dates=["published_at"],
    )
    def _pretty_date_str(d):
        # d can be a date or ISO string
        df = pd.DataFrame({"d": [pd.to_datetime(d, errors="coerce")]})
        return format_display_dates(df.rename(columns={"d": "date"}))["date"].iloc[0]

    cards = []
    if not ls.empty:
        # newest -> initial_views
        row0 = ls.iloc[0]
        cards.append({
            "label": "At Publish",
            "views": int(row0.get("initial_views") or 0),
            "title": str(row0.get("title") or ""),
            "date": row0.get("published_at"),
        })
        # previous -> views_1w
        if len(ls) >= 2:
            row1 = ls.iloc[1]
            cards.append({
                "label": "1 Week Later",
                "views": int(row1.get("views_1w") or 0),
                "title": str(row1.get("title") or ""),
                "date": row1.get("published_at"),
            })
        # ~4 videos back -> views_4w
        if len(ls) >= 5:
            row4 = ls.iloc[4]
            cards.append({
                "label": "4 Weeks Later",
                "views": int(row4.get("views_4w") or 0),
                "title": str(row4.get("title") or ""),
                "date": row4.get("published_at"),
            })

    if cards:
        cols = st.columns(len(cards))
        for i, c in enumerate(cards):
            with cols[i]:
                # Card header metric
                st.metric(c["label"], f"{c['views']:,}")
                # Title + date
                st.caption(c["title"])
                st.caption(_pretty_date_str(c["date"]))
    else:
        st.info("No livestreams found.")

