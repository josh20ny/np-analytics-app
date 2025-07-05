# dashboard.py
import os
from datetime import datetime

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from dotenv import load_dotenv
import matplotlib.pyplot as plt

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"))

st.set_page_config(page_title="NP Analytics Dashboard", layout="wide")
st.title("📊 NP Analytics")

# ─── LOADER (now takes a value column and renames it to “value”) ──────────────
@st.cache_data(ttl=3600)
def load_table(table_name: str, date_col: str, value_col: str) -> pd.DataFrame:
    df = pd.read_sql(f"SELECT * FROM {table_name}", engine, parse_dates=[date_col])
    df = df.sort_values(date_col)
    df["year"] = df[date_col].dt.year
    df["week"] = df[date_col].dt.isocalendar().week
    # isolate & rename the one metric column you want to chart
    return df[[date_col, value_col, "year", "week"]].rename(
        columns={date_col: "date", value_col: "value"}
    )

# ─── YOUR TABLE‐TO‐METRIC MAPPING ─────────────────────────────────────────────
# friendly name → ( table_name, date_column, value_column )
queries = {
    "Livestream Views":       ("livestreams",             "published_at",      "initial_views"),
    "Adult Attendance":       ("adult_attendance",        "date",              "total_attendance"),
    "Waumba Land Attendance": ("waumbaland_attendance",   "date",              "total_attendance"),
    "UpStreet Attendance":    ("upstreet_attendance",     "date",              "total_attendance"),
    "Transit Attendance":     ("transit_attendance",      "date",              "total_attendance"),
    "Insideout Attendance":   ("insideout_attendance",    "date",              "total_attendance"),
    "Weekly YouTube Summary": ("weekly_youtube_summary",  "week_start",        "total_views"),
    "Mailchimp Summary":      ("mailchimp_weekly_summary","week_start",        "email_count"),
}

# ─── LOAD THEM ALL ────────────────────────────────────────────────────────────
data = {
    name: load_table(tbl, dt, val)
    for name, (tbl, dt, val) in queries.items()
}

# ─── SIDEBAR SELECTION ────────────────────────────────────────────────────────
metric = st.sidebar.selectbox("Metric", list(data))
df     = data[metric]

st.write(f"### {metric} — {len(df):,} weeks of data")

# ─── MULTI‐YEAR LINE CHART ────────────────────────────────────────────────────
years = sorted(df.year.unique())
pick  = st.sidebar.multiselect("Years to compare", years, default=years[-2:])
agg   = df.groupby(["year","week"])["value"].sum().unstack("year").fillna(0)
plot  = agg[pick]
st.subheader(f"{metric} by ISO Week")
st.line_chart(plot, use_container_width=True)

# ─── CURRENT YEAR TREND ──────────────────────────────────────────────────────
cy = datetime.now().year
st.subheader(f"This Year ({cy})")
if cy in agg.columns:
    st.line_chart(agg[cy], use_container_width=True)
else:
    st.write("No data for this year yet.")

# ─── WEEKLY YoY TABLE ────────────────────────────────────────────────────────
st.subheader("Weekly YoY Comparison")
last, now = cy-1, cy
weekly = df.groupby(["week","year"])["value"].sum().unstack("year").fillna(0)
if last in weekly.columns and now in weekly.columns:
    comp = weekly[[last, now]].copy()
    comp["YoY %"] = (comp[now] - comp[last]) / comp[last] * 100

    def hl(v): return "background-color: #40e060" if v>0 else "background-color: #d9374a"
    styled = (comp.style
                 .format({last:"{:,}", now:"{:,}", "YoY %":"{:+.1f}%"})
                 .applymap(hl, subset=["YoY %"]))
    st.dataframe(styled, use_container_width=True)
else:
    st.write(f"Insufficient data for {last} vs {now}")

# ─── CONDITIONAL PIE CHARTS FOR ATTENDANCE ──────────────────────────────────
attendance_tables = {
    "Adult Attendance":      "adult_attendance",
    "Waumba Land Attendance":"waumbaland_attendance",
    "UpStreet Attendance":   "upstreet_attendance",
    "Transit Attendance":    "transit_attendance",
}

@st.cache_data(ttl=3600)
def load_attendance(table_name: str):
    df = pd.read_sql(
        f"SELECT date, attendance_930, attendance_1100 FROM {table_name}",
        engine,
        parse_dates=["date"],
    )
    # add ISO week column
    df["week"] = df["date"].dt.isocalendar().week
    return df

# then later, where you want to draw pies:
current_week = datetime.now().isocalendar().week

if metric in attendance_tables:
    table_name = attendance_tables[metric]
    df_att = load_attendance(table_name)
    current_week = datetime.now().isocalendar().week
    df_wk = df_att[df_att["week"] == current_week]
    if df_wk.empty:
        st.warning(f"No data for week {current_week} in **{metric}**.")
    else:
        # pick the most recent date if you have multiples
        row = df_wk.sort_values("date").iloc[-1]
        a9  = float(row["attendance_930"])
        a11 = float(row["attendance_1100"])
        if a9 + a11 == 0:
            st.warning(f"Zero attendance in **{metric}** for {row['date'].date()}.")
        else:
            fig, ax = plt.subplots()
            ax.pie(
                [a9, a11],
                labels=["9:30 AM", "11:00 AM"],
                autopct="%1.1f%%",
                startangle=90,
                wedgeprops={"edgecolor": "white"},
            )
            ax.set_title(f"{metric} breakdown for week of {row['date'].date()}")
            ax.axis("equal")  # keep it a circle
            st.pyplot(fig)