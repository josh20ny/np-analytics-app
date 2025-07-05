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
        if not widgets:
            st.write(f"**{tab_name}** tab coming soon!")
            continue
        for meta in widgets:
            table, date_col, value_col = meta['loader']
            widget_fn = meta['widget']
            args = meta['args'].copy()
            # Load data if numeric metric
            df = load_table(table, date_col, value_col) if value_col else None
            # Pie charts: service time, gender, age/grade
            if widget_fn == pie_chart:
                title = args.pop('title')
                # Debug entry
                st.write(f"Processing pie chart '{title}' for table '{table}'")
                # Service Time
                if title == 'Service Time Distribution' and table in ['adult_attendance','waumbaland_attendance','upstreet_attendance','transit_attendance']:
                    df_att = pd.read_sql(f"SELECT date, attendance_930, attendance_1100 FROM {table}", engine, parse_dates=['date'])
                    st.write("Service time raw df:", df_att.tail(3))
                    latest = df_att.sort_values('date').iloc[-1]
                    labels = ['9:30 AM', '11:00 AM']
                    values = [latest['attendance_930'], latest['attendance_1100']]
                    st.write(f"Service Time raw values for {table}:", dict(zip(labels, values)))
                    if sum(values) > 0:
                        pie_chart(None, labels, values, title)
                # Gender
                elif title == 'Gender Distribution':
                    raw = pd.read_sql(f"SELECT * FROM {table}", engine, parse_dates=['date'])
                    st.write("Gender raw df columns:", list(raw.columns))
                    st.write("Gender raw tail:", raw.tail(3))
                    latest = raw.sort_values('date').iloc[-1]
                    male_cols = [c for c in latest.index if c.endswith('_male')]
                    female_cols = [c for c in latest.index if c.endswith('_female')]
                    labels = ['Male', 'Female']
                    male_sum = sum((0 if pd.isna(latest[c]) else latest[c]) for c in male_cols)
                    female_sum = sum((0 if pd.isna(latest[c]) else latest[c]) for c in female_cols)
                    values = [male_sum, female_sum]
                    st.write(f"Gender raw values for {table}:", dict(zip(labels, values)))
                    if sum(values) > 0:
                        pie_chart(None, labels, values, title)
                # Age or Grade
                elif title in ['Age Distribution', 'Grade Distribution']:
                    raw = pd.read_sql(f"SELECT * FROM {table}", engine, parse_dates=['date'])
                    st.write("Age/Grade raw df columns:", list(raw.columns))
                    st.write("Age/Grade raw tail:", raw.tail(3))
                    latest = raw.sort_values('date').iloc[-1]
                    groups = {}
                    for col in latest.index:
                        if '_' in col and (col.endswith('_male') or col.endswith('_female')) and col not in ['attendance_930','attendance_1100']:
                            key = col.rsplit('_',2)[1]
                            groups.setdefault(key, 0)
                            groups[key] += (0 if pd.isna(latest[col]) else latest[col])
                    labels = list(groups.keys())
                    values = list(groups.values())
                    st.write(f"{title} raw values for {table}:", groups)
                    if sum(values) > 0:
                        pie_chart(None, labels, values, title)
                # Skip default for pie charts
                continue

            # KPI card for groups
            if widget_fn == kpi_card and table == 'groups_summary':
                gs = pd.read_sql("SELECT date, number_of_groups FROM groups_summary", engine, parse_dates=['date'])
                latest = gs.sort_values('date').iloc[-1]['number_of_groups']
                kpi_card(args['label'], latest)
                continue
            # Default: overlay or YoY table
            widget_fn(df, **args)