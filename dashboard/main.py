# dashboard/main.py
import os
from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import streamlit as st
from sqlalchemy import text

# Project imports
from data import load_table, engine
from config import TAB_CONFIG, TABLE_FILTERS
from widgets.core import ranged_table, format_display_dates
from widgets.legacy import (
    overlay_years_chart,
    per_service_location_bars,
    weekly_yoy_table,
    pie_chart,
    kpi_card,
)
from widgets.weekly import weekly_summary_view
from widgets.rolling import rolling_average_chart
from widgets.giving_ytd import giving_ytd_bar

from lib.auth import (
    login_gate,
    registration_panel,
    verification_panel,
    password_tools,
)
from lib.emailer import send_email

# Optional Admin panel (only shown for admins/owners if available)
from widgets.admin import admin_panel

# –– Maps ──────────────────────────────────────────────────────────────────────

ROLLING_MAP = {
    # Attendance tabs
    "Adult Attendance":            {"table": "adult_attendance",   "date_col": "date",     "value_col": "total_attendance", "currency": False, "title": "Adult Attendance"},
    "InsideOut Attendance":        {"table": "insideout_attendance","date_col": "date",     "value_col": "total_attendance", "currency": False, "title": "InsideOut Attendance"},
    "Transit Attendance":          {"table": "transit_attendance",  "date_col": "date",     "value_col": "total_attendance", "currency": False, "title": "Transit Attendance"},
    "UpStreet Attendance":         {"table": "upstreet_attendance", "date_col": "date",     "value_col": "total_attendance", "currency": False, "title": "UpStreet Attendance"},
    "Waumba Land Attendance":      {"table": "waumbaland_attendance","date_col": "date",    "value_col": "total_attendance", "currency": False, "title": "Waumba Land Attendance"},
    # Giving tab
    "Giving":           {"table": "weekly_giving_summary","date_col": "week_end","value_col": "total_giving",      "currency": True,  "title": "Total Giving"},
}


# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="NP Analytics", layout="wide", initial_sidebar_state="expanded")
st.title("📊 NP Analytics")

# Auth gate
authed = login_gate("NP Analytics Login", render_if_unauth=True)
if not authed:
    registration_panel()
    verification_panel()
    st.stop()

# Sidebar password tools (renders in sidebar in your auth.py)
password_tools()

# Determine role and tabs
user = st.session_state.get("auth_user", {})
is_admin = user.get("role") in {"admin"}

# Put Weekly Summary first, then the rest, plus Admin (if allowed)
base_tabs = ["Weekly Summary"]
tab_names = base_tabs + list(TAB_CONFIG.keys()) + (["Admin"] if is_admin and admin_panel else [])

ROLE_RULES = {
    "viewer":  {"deny": {"Giving", "Engagement", "Admin"}},
    "finance": {"deny": {"Engagement", "Admin"}},
    "people":  {"deny": {"Admin"}},
    "admin":   {"deny": set()},
}

def _effective_role(user):
    # adapt to whatever you store in session
    r = (st.session_state.get("auth_user", {}).get("role") or "").lower()
    return r if r in ROLE_RULES else "viewer"

role = _effective_role(st.session_state.get("auth_user", {}))
deny = ROLE_RULES[role]["deny"]

# include Weekly Summary by default + everything not denied
tab_names = ["Weekly Summary"] + [t for t in TAB_CONFIG.keys() if t not in deny]
if role != "admin":
    # don't add Admin tab unless admin
    pass
else:
    tab_names += ["Admin"]
tabs = st.tabs(tab_names)


for tab_obj, tab_name in zip(tabs, tab_names):
    with tab_obj:
        if tab_name == "Weekly Summary":
            weekly_summary_view()
            continue
        

    with tab_obj:
        # Admin tab
        if tab_name == "Admin":
            admin_panel()   # only present if imported and role-allowed
            continue

        widgets = TAB_CONFIG.get(tab_name, [])
        if not widgets:
            st.write(f"**{tab_name}** tab coming soon!")
            continue

        # Top “Filtered rows” table (only if first widget points to a real table)
        try:
            first_loader = widgets[0]["loader"]
            table_name, date_col_all, value_col_all = first_loader
        except Exception:
            table_name = date_col_all = value_col_all = None
        
        # ── Special case: Mailchimp tab shows per-audience tables ────────────────────
        if tab_name == "Mailchimp":
            try:
                # Pull once, then slice by audience + date range
                df_all = pd.read_sql(
                    "SELECT week_end, audience_name, email_count, avg_open_rate, avg_click_rate "
                    "FROM mailchimp_weekly_summary "
                    "ORDER BY week_end DESC",
                    engine,
                    parse_dates=["week_end"],
                )
                if df_all.empty:
                    st.info("No Mailchimp data.")
                    continue

                df_all = df_all.rename(columns={"week_end": "date"})

                # Date range picker (shared across all audiences on this tab)
                df_all["parsed_date"] = pd.to_datetime(df_all["date"], errors="coerce")
                df_all = df_all.sort_values("parsed_date", ascending=False)
                default_end = df_all["parsed_date"].iloc[0].date()
                default_start = df_all["parsed_date"].iloc[min(14, len(df_all) - 1)].date()
                start_date, end_date = st.date_input(
                    "Date range",
                    (default_start, default_end),
                    key=f"mailchimp_range",
                )
                # Normalize inputs (tuple vs list)
                if isinstance(start_date, (list, tuple)):
                    start_date, end_date = start_date[0], start_date[1]

                mask = (df_all["parsed_date"].dt.date >= start_date) & (df_all["parsed_date"].dt.date <= end_date)
                df_window = df_all.loc[mask].drop(columns=["parsed_date"])

                audiences = [
                    "Northpoint Church",
                    "InsideOut Parents",
                    "Transit Parents",
                    "Upstreet Parents",
                    "Waumba Land Parents",
                ]

                for aud in audiences:
                    sub = df_window[df_window["audience_name"] == aud].copy()
                    st.subheader(aud)

                    if sub.empty:
                        st.info("No rows in selected range.")
                        continue

                    # Pretty dates for display
                    display_df = sub[["date", "email_count", "avg_open_rate", "avg_click_rate"]].copy()
                    display_df = format_display_dates(display_df)

                    st.dataframe(display_df, use_container_width=True)

                    # Averages row (numeric cols only)
                    numeric_cols = display_df.select_dtypes(include="number").columns
                    if len(numeric_cols) > 0:
                        avg_row = display_df[numeric_cols].mean(numeric_only=True).to_frame().T
                        avg_row.index = ["Averages"]
                        st.dataframe(avg_row, use_container_width=True)
            except Exception as e:
                st.warning(f"Mailchimp audience view error: {e}")

            # Audience filter
            aud_map = {
                "625faaf650": "Northpoint Church",
                "5839e884af": "Inside Out Parents 2025-2026",
                "4d8c7861bc": "Transit Parents 2025-2026",
                "621229dee8": "Upstreet Parents 2025-2026",
                "5bfb241f04": "Waumba Land Parents 2025-2026",
            }
            aud_choices = ["All"] + list(aud_map.values())
            aud_pick = st.selectbox("Audience", aud_choices, index=0, key="mc_recent_audience")

            days = st.slider("Window (days)", min_value=30, max_value=365, value=90, step=15, key="mc_recent_days")

            where_extra = ""
            param = {}
            if aud_pick != "All":
                # invert map
                inv = {v: k for k, v in aud_map.items()}
                where_extra = "AND c.list_id = :lid"
                param["lid"] = inv.get(aud_pick)

            param["days"] = days

            sql_recent = text("""
                SELECT
                c.id,
                c.send_time,
                c.list_id,
                c.subject,
                c.emails_sent,
                ROUND((100 * COALESCE(c.open_rate_effective, 0))::numeric, 2)  AS open_rate_pct,
                ROUND((100 * COALESCE(c.click_rate_effective, 0))::numeric, 2) AS click_rate_pct,
                t.top_link_url,
                t.top_link_unique,
                t.top_link_total
                FROM v_mailchimp_campaigns_enriched c
                LEFT JOIN v_mailchimp_campaign_top_link t ON t.campaign_id = c.id
                WHERE c.send_time >= NOW() - (:days || ' days')::interval
                {where_extra}
                ORDER BY c.send_time DESC
                LIMIT 500
            """.replace("{where_extra}", where_extra))

            with engine.connect() as c:
                df_recent = pd.read_sql(sql_recent, c, params=param, parse_dates=["send_time"])

            if df_recent.empty:
                st.info("No campaigns in the selected window.")
            else:
                df_recent["audience"] = df_recent["list_id"].map(aud_map).fillna(df_recent["list_id"])
                df_recent_display = df_recent[[
                    "send_time","audience","subject","emails_sent","open_rate_pct","click_rate_pct",
                    "top_link_url","top_link_unique","top_link_total"
                ]].rename(columns={
                    "send_time":"Sent",
                    "audience":"Audience",
                    "subject":"Subject",
                    "emails_sent":"Sent To",
                    "open_rate_pct":"Open %",
                    "click_rate_pct":"Click %",
                    "top_link_url":"Top Link",
                    "top_link_unique":"Top Link Unique",
                    "top_link_total":"Top Link Total"
                })
                df_recent_display["Sent"] = df_recent_display["Sent"].dt.tz_localize(None)
                st.dataframe(df_recent_display, use_container_width=True, hide_index=True)

            # --- Top Clicks for a Campaign ------------------------------------------------
            st.markdown("### Top Clicks for a Campaign")
            if not df_recent.empty:
                camp_ids = df_recent[["id","subject","send_time"]].copy()
                camp_ids["label"] = camp_ids.apply(lambda r: f"{r['send_time'].strftime('%Y-%m-%d %H:%M')} — {r['subject']}", axis=1)
                pick = st.selectbox("Choose campaign", camp_ids["label"].tolist(), key="mc_pick_campaign")
                picked_id = camp_ids.loc[camp_ids["label"] == pick, "id"].iloc[0]

                sql_clicks = text("""
                    SELECT label, url, unique_clicks, total_clicks
                    FROM v_mailchimp_campaign_top_clicks
                    WHERE campaign_id = :cid
                    ORDER BY rn
                    LIMIT 20
                """)
                with engine.connect() as c:
                    df_clicks = pd.read_sql(sql_clicks, c, params={"cid": picked_id})

                if df_clicks.empty:
                    st.info("No click data for this campaign.")
                else:
                    st.dataframe(df_clicks, use_container_width=True, hide_index=True)
            
            # Skip the generic ranged_table for this tab
            continue

        elif tab_name in ("InsideOut Attendance", "Transit Attendance", "UpStreet Attendance", "Waumba Land Attendance"):
            # Map tab to ministry_key used in your locations view
            ministry_map = {
                "InsideOut Attendance": "InsideOut",
                "Transit Attendance": "Transit",
                "UpStreet Attendance": "UpStreet",
                "Waumba Land Attendance": "Waumba Land",
            }
            ministry = ministry_map[tab_name]

            # Pull the labeled daily location rows for this ministry
            sql = text("""
                SELECT date, ministry_key, service_bucket, location_name, total_attendance, total_new
                FROM attendance_by_location_daily_labeled
                WHERE ministry_key = :ministry
                ORDER BY date DESC
            """)
            with engine.connect() as c:
                df_loc = pd.read_sql(sql, c, params={"ministry": ministry}, parse_dates=["date"])

            if df_loc.empty:
                st.info(f"No location rows for {ministry}.")
            else:
                st.markdown("### Rooms / Groups — Most Recent Sunday")
                per_service_location_bars(df_loc, title_prefix=ministry)

        if table_name and table_name != "__service__" and date_col_all:
            flt = (TABLE_FILTERS or {}).get(tab_name, {})
            try:
                ranged_table(
                    table=table_name,
                    date_col=date_col_all,
                    key=tab_name,
                    metric_col=flt.get("metric_col"),
                    min_value=flt.get("min_value"),
                )
            except Exception as e:
                st.warning(f"Could not load data for `{table_name}`: {e}")
        
        # --- Special case: Giving YTD at the very top ---
        if tab_name == "Giving":
            giving_ytd_bar(years_back=5)

        # –– Rolling Average Table –––––––––––––––––––––––––––––––––––––––––––––
        if tab_name in ROLLING_MAP:
            cfg = ROLLING_MAP[tab_name]
            st.subheader(f"{cfg['title']} — Rolling Average (last 12 months)")
            rolling_average_chart(
                table=cfg["table"],
                date_col=cfg["date_col"],
                value_col=cfg["value_col"],
                title=cfg["title"],
                default_months=6,   # slider default
                last_days=365,
                currency=cfg["currency"],
                agg="mean",
                key_suffix=tab_name.replace(" ", "_").lower(),
            )

        # ── Widgets render loop ───────────────────────────────────────────────
        for meta in widgets:
            table, date_col, value_col = meta["loader"]
            widget_fn = meta["widget"]
            args = meta["args"].copy()

            # 1) Service-backed widgets: call directly with their provider
            if table == "__service__":
                try:
                    widget_fn(**args)
                except Exception as e:
                    st.warning(f"Widget error: {e}")
                continue

            # 2) Special-case: legacy pie charts (expect labels/values, not df)
            if widget_fn == pie_chart:
                title = args.get("title", "")

                # Service time distribution (pull latest 9:30/11:00)
                if title == "Service Time Distribution" and table in [
                    "adult_attendance", "waumbaland_attendance", "upstreet_attendance", "transit_attendance"
                ]:
                    try:
                        df_att = pd.read_sql(
                            f"SELECT date, attendance_930, attendance_1100 FROM {table}",
                            engine,
                            parse_dates=["date"],
                        )
                        if not df_att.empty:
                            latest = df_att.sort_values("date").iloc[-1]
                            labels = ["9:30 AM", "11:00 AM"]
                            values = [
                                int(latest.get("attendance_930") or 0),
                                int(latest.get("attendance_1100") or 0),
                            ]
                            if sum(values) > 0:
                                pie_chart(None, labels, values, title)
                    except Exception as e:
                        st.warning(f"Pie widget error ({title}): {e}")
                    continue

                # Gender distribution
                if title == "Gender Distribution":
                    try:
                        raw = pd.read_sql(f"SELECT * FROM {table}", engine, parse_dates=["date"])
                        if not raw.empty:
                            latest = raw.sort_values("date").iloc[-1]
                            male_cols = [c for c in latest.index if c.endswith("_male")]
                            female_cols = [c for c in latest.index if c.endswith("_female")]
                            male_sum = sum(int(latest[c] or 0) for c in male_cols if pd.notna(latest[c]))
                            female_sum = sum(int(latest[c] or 0) for c in female_cols if pd.notna(latest[c]))
                            if male_sum + female_sum > 0:
                                pie_chart(None, ["Male", "Female"], [male_sum, female_sum], title)
                    except Exception as e:
                        st.warning(f"Pie widget error ({title}): {e}")
                    continue

                # Age/Grade distribution
                if title in ["Age Distribution", "Grade Distribution"]:
                    try:
                        raw = pd.read_sql(f"SELECT * FROM {table}", engine, parse_dates=["date"])
                        if not raw.empty:
                            latest = raw.sort_values("date").iloc[-1]
                            groups = {}
                            for col in latest.index:
                                if (
                                    "_" in col
                                    and (col.endswith("_male") or col.endswith("_female"))
                                    and col not in ["attendance_930", "attendance_1100"]
                                ):
                                    key = col.rsplit("_", 2)[1]
                                    groups[key] = groups.get(key, 0) + (int(latest[col]) if pd.notna(latest[col]) else 0)
                            if sum(groups.values()) > 0:
                                pie_chart(None, list(groups.keys()), list(groups.values()), title)
                    except Exception as e:
                        st.warning(f"Pie widget error ({title}): {e}")
                    continue

                # If a pie widget didn't match any case, skip gracefully
                continue

            # 4) Default: load normalized df (date/value/year/week) for legacy widgets
            try:
                df = load_table(table, date_col, value_col) if table else None
                widget_fn(df, **args)
            except Exception as e:
                st.warning(f"Widget error for `{table}`: {e}")

            