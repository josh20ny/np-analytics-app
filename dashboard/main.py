# dashboard/main.py
import os
from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import streamlit as st

# Project imports
from data import load_table, engine
from config import TAB_CONFIG, TABLE_FILTERS
from widgets.core import ranged_table
from widgets.legacy import (
    overlay_years_chart,
    weekly_yoy_table,
    pie_chart,
    kpi_card,
)
from lib.auth import (
    login_gate,
    registration_panel,
    verification_panel,
    password_tools,
)
from lib.emailer import send_email

# Optional Admin panel (only shown for admins/owners if available)
from widgets.admin import admin_panel

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

# Determine tab list (include Admin if role allows and panel exists)
user = st.session_state.get("auth_user", {})
is_admin = user.get("role") in {"admin", "owner"}
tab_names = list(TAB_CONFIG.keys()) + (["Admin"] if is_admin and admin_panel else [])
tabs = st.tabs(tab_names)

for tab_obj, tab_name in zip(tabs, tab_names):
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

            # 3) Special-case: KPI card (groups_summary latest value)
            if widget_fn == kpi_card and table == "groups_summary":
                try:
                    gs = pd.read_sql(
                        "SELECT date, number_of_groups FROM groups_summary",
                        engine,
                        parse_dates=["date"],
                    )
                    if not gs.empty:
                        latest = int(gs.sort_values("date").iloc[-1]["number_of_groups"] or 0)
                        kpi_card(args["label"], latest)  # Note: no df passed
                except Exception as e:
                    st.warning(f"KPI widget error: {e}")
                continue

            # 4) Default: load normalized df (date/value/year/week) for legacy widgets
            try:
                df = load_table(table, date_col, value_col) if table else None
                widget_fn(df, **args)
            except Exception as e:
                st.warning(f"Widget error for `{table}`: {e}")
