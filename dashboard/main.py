import streamlit as st
from data import load_table, engine
from config import TAB_CONFIG
from widgets import pie_chart, kpi_card
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="NP Analytics", layout="wide")
st.title("📊 NP Analytics")

tabs = st.tabs(list(TAB_CONFIG.keys()))
for tab_obj, tab_name in zip(tabs, TAB_CONFIG):
    with tab_obj:
        widgets = TAB_CONFIG[tab_name]

        # ─── Full Data Table (last 10 rows) ───────────────────────────────────
        if widgets:
            first_loader = widgets[0]['loader']
            table_all, date_col_all, _ = first_loader
            try:
                # Special handling for Mailchimp: split by audience
                if tab_name == "Mailchimp":
                    audiences = [
                        "Northpoint Church",
                        "InsideOut Parents",
                        "Transit Parents",
                        "Upstreet Parents",
                        "Waumba Land Parents",
                    ]
                    for aud in audiences:
                        df_aud = pd.read_sql(
                            f"SELECT * FROM {table_all} WHERE audience_name = %s ORDER BY {date_col_all} DESC LIMIT 10",
                            engine,
                            params=(aud,),
                            parse_dates=[date_col_all]
                        )
                        # Format date column
                        if date_col_all in df_aud.columns:
                            df_aud[date_col_all] = df_aud[date_col_all].dt.strftime('%B %d, %Y')
                        st.subheader(f"Last 10 rows for {aud}")
                        st.dataframe(df_aud, use_container_width=True)
                else:
                    df_all = pd.read_sql(
                        f"SELECT * FROM {table_all}", engine,
                        parse_dates=[date_col_all]
                    )
                    # limit to last 10 by date
                    if date_col_all in df_all.columns:
                        df_recent = df_all.sort_values(
                            date_col_all, ascending=False
                        ).head(10)
                        df_recent[date_col_all] = df_recent[date_col_all].dt.strftime('%B %d, %Y')
                    else:
                        df_recent = df_all.tail(10)
                    st.subheader(f"Last 10 rows from `{table_all}` table")
                    st.dataframe(df_recent, use_container_width=True)
            except Exception as e:
                st.warning(f"Could not load data for `{table_all}`: {e}")

        if not widgets:
            st.write(f"**{tab_name}** tab coming soon!")
            continue

        # ─── Widgets ──────────────────────────────────────────────────────────
        for meta in widgets:
            table, date_col, value_col = meta['loader']
            widget_fn = meta['widget']
            args = meta['args'].copy()
            df = load_table(table, date_col, value_col) if value_col else None

            if widget_fn == pie_chart:
                title = args.pop('title')
                # Service time pie
                if title == 'Service Time Distribution' and table in [
                    'adult_attendance','waumbaland_attendance',
                    'upstreet_attendance','transit_attendance'
                ]:
                    df_att = pd.read_sql(
                        f"SELECT date, attendance_930, attendance_1100 FROM {table}",
                        engine, parse_dates=['date']
                    )
                    latest = df_att.sort_values('date').iloc[-1]
                    labels = ['9:30 AM', '11:00 AM']
                    values = [latest['attendance_930'], latest['attendance_1100']]
                    if sum(values) > 0:
                        pie_chart(None, labels, values, title)
                # Gender pie
                elif title == 'Gender Distribution':
                    raw = pd.read_sql(f"SELECT * FROM {table}", engine, parse_dates=['date'])
                    latest = raw.sort_values('date').iloc[-1]
                    male_cols = [c for c in latest.index if c.endswith('_male')]
                    female_cols = [c for c in latest.index if c.endswith('_female')]
                    male_sum = sum((0 if pd.isna(latest[c]) else latest[c]) for c in male_cols)
                    female_sum = sum((0 if pd.isna(latest[c]) else latest[c]) for c in female_cols)
                    if male_sum + female_sum > 0:
                        pie_chart(None, ['Male', 'Female'], [male_sum, female_sum], title)
                # Age/Grade pie
                elif title in ['Age Distribution', 'Grade Distribution']:
                    raw = pd.read_sql(f"SELECT * FROM {table}", engine, parse_dates=['date'])
                    latest = raw.sort_values('date').iloc[-1]
                    groups = {}
                    for col in latest.index:
                        if '_' in col and (col.endswith('_male') or col.endswith('_female')) and col not in ['attendance_930','attendance_1100']:
                            key = col.rsplit('_',2)[1]
                            groups[key] = groups.get(key, 0) + (0 if pd.isna(latest[col]) else latest[col])
                    if sum(groups.values()) > 0:
                        pie_chart(None, list(groups.keys()), list(groups.values()), title)
                continue

            # KPI card for groups
            if widget_fn == kpi_card and table == 'groups_summary':
                gs = pd.read_sql(
                    "SELECT date, number_of_groups FROM groups_summary",
                    engine, parse_dates=['date']
                )
                latest = gs.sort_values('date').iloc[-1]['number_of_groups']
                kpi_card(args['label'], latest)
                continue

            # Default: overlay or YoY table
            widget_fn(df, **args)

