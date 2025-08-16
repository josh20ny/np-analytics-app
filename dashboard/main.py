import os
from dotenv import load_dotenv
load_dotenv()

import streamlit as st
from data import load_table
from config import TAB_CONFIG

from widgets.core import ranged_table
from lib.auth import login_gate, registration_panel, verification_panel, password_tools
from lib.emailer import send_email
from widgets.legacy import overlay_years_chart, weekly_yoy_table, pie_chart, kpi_card, date_range_table, filter_meaningful_rows
import pandas as pd
from data import load_table, engine

st.set_page_config(page_title="NP Analytics", layout="wide", initial_sidebar_state="expanded")
st.title("📊 NP Analytics")

authed = login_gate("NP Analytics Login", render_if_unauth=True)
if not authed:
    registration_panel()
    verification_panel()
    st.stop()

# ✅ From here down, user is authenticated
password_tools()

with st.expander("✉️ Send a test email"):
    to = st.text_input("To", value=os.getenv("TEST_EMAIL", "you@personal.com"))
    if st.button("Send test"):
        try:
            send_email(to, "NP Analytics test", "If you got this, SendGrid is wired.")
            st.success(f"Sent to {to}")
        except Exception as e:
            st.error(f"Failed: {e}")

tabs = st.tabs(list(TAB_CONFIG.keys()))

for tab_obj, tab_name in zip(tabs, TAB_CONFIG):
    with tab_obj:
        widgets = TAB_CONFIG[tab_name]
        if not widgets:
            st.write(f"**{tab_name}** tab coming soon!")
            continue

        # Optional “raw table + date range” viewer using the FIRST widget’s loader,
        # if it points to a real table (not a service).
        first_loader = widgets[0]["loader"]
        table_name, date_col, _ = first_loader
        if table_name != "__service__" and date_col:
            ranged_table(table_name, date_col, key=f"range_{tab_name}")

        # Render widgets
        for meta in widgets:
            table, date_col, value_col = meta["loader"]
            widget_fn = meta["widget"]
            args = meta["args"].copy()

            # 1) Service-backed widgets use their provider directly
            if table == "__service__":
                widget_fn(**args)
                continue

            # 2) Special-case legacy pie charts (they need labels & values, not a df)
            if widget_fn == pie_chart:
                title = args.get("title", "")

                # Service time distribution (latest row’s two service counts)
                if title == "Service Time Distribution" and table in [
                    "adult_attendance", "waumbaland_attendance", "upstreet_attendance", "transit_attendance"
                ]:
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
                elif title == "Gender Distribution":
                    raw = pd.read_sql(f"SELECT * FROM {table}", engine, parse_dates=["date"])
                    if not raw.empty:
                        latest = raw.sort_values("date").iloc[-1]
                        male_cols = [c for c in latest.index if c.endswith("_male")]
                        female_cols = [c for c in latest.index if c.endswith("_female")]
                        male_sum = sum(int(latest[c] or 0) for c in male_cols if pd.notna(latest[c]))
                        female_sum = sum(int(latest[c] or 0) for c in female_cols if pd.notna(latest[c]))
                        if male_sum + female_sum > 0:
                            pie_chart(None, ["Male", "Female"], [male_sum, female_sum], title)
                elif title in ["Age Distribution", "Grade Distribution"]:
                    raw = pd.read_sql(f"SELECT * FROM {table}", engine, parse_dates=["date"])
                    if not raw.empty:
                        latest = raw.sort_values("date").iloc[-1]
                        groups = {}
                        for col in latest.index:
                            if "_" in col and (col.endswith("_male") or col.endswith("_female")) and col not in ["attendance_930", "attendance_1100"]:
                                key = col.rsplit("_", 2)[1]
                                groups[key] = groups.get(key, 0) + (int(latest[col]) if pd.notna(latest[col]) else 0)
                        if sum(groups.values()) > 0:
                            pie_chart(None, list(groups.keys()), list(groups.values()), title)
                # done with pie widgets
                continue

            # 3) Special-case KPI card (compute latest value, then call by label/value)
            if widget_fn == kpi_card and table == "groups_summary":
                gs = pd.read_sql(
                    "SELECT date, number_of_groups FROM groups_summary",
                    engine,
                    parse_dates=["date"],
                )
                if not gs.empty:
                    latest = int(gs.sort_values("date").iloc[-1]["number_of_groups"] or 0)
                    kpi_card(args["label"], latest)   # 👈 no df passed here
                continue

            # 4) All other widgets get the normalized df (date/value/year/week)
            df = load_table(table, date_col, value_col) if table else None
            widget_fn(df, **args)
