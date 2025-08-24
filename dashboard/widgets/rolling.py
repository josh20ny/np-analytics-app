# dashboard/widgets/rolling.py
from __future__ import annotations
import math
import pandas as pd
import streamlit as st
import altair as alt

from data import engine
from widgets.core import format_display_dates

def rolling_average_chart(
    *,
    table: str,
    date_col: str,
    value_col: str,
    title: str | None = None,
    default_months: int = 6,
    last_days: int = 365,
    currency: bool = False,
    agg: str = "mean",   # "mean" or "sum"
    key_suffix: str = "",
    smooth: bool = True
):
    """
    Draw a rolling average chart over the last `last_days` (default: 1 year).
    Rolling window is set in MONTHS via a slider (1–12).
    """
    df = pd.read_sql(
        f"SELECT {date_col} AS d, {value_col} AS v FROM {table} ORDER BY {date_col}",
        engine,
        parse_dates=["d"],
    )
    if df.empty:
        st.info("No data to chart.")
        return

    # Filter to last year
    cutoff = df["d"].max() - pd.Timedelta(days=last_days)
    df = df[df["d"] >= cutoff].copy()
    df = df.dropna(subset=["d", "v"]).sort_values("d")

    # UI controls
    months = st.slider(
        "Rolling window (months)",
        min_value=1, max_value=12, value=default_months,
        key=f"roll_months_{table}_{value_col}_{key_suffix}"
    )

    # Compute rolling average using a time-based window (in days)
    days = int(round(months * 30.44))  # calendar-ish months
    ts = df.set_index("d").sort_index()
    if agg == "sum":
        ts["rolling"] = ts["v"].rolling(f"{days}D").sum()
    else:
        ts["rolling"] = ts["v"].rolling(f"{days}D").mean()
    ts["raw"] = ts["v"]
    ts = ts.reset_index().rename(columns={"index": "d"})

    # Pretty date labels for tooltips
    disp = format_display_dates(ts[["d"]].rename(columns={"d": "date"}))["date"]
    ts["date_label"] = disp


    # Axis/tooltip formats — force non-scientific labels
    if currency:
        y_title   = title or value_col.replace("_", " ").title()
        y_axis    = alt.Axis(format="$,.2f", tickCount=6)  # dollars
        tip_raw   = alt.Tooltip("raw:Q",     title=y_title, format=",.2f")
        tip_roll  = alt.Tooltip("rolling:Q", title=f"{months}-mo avg", format=",.2f")
    else:
        y_title   = title or value_col.replace("_", " ").title()
        y_axis    = alt.Axis(format=",d", tickCount=6)     # integers, no 2e+3
        tip_raw   = alt.Tooltip("raw:Q",     title=y_title, format=",d")
        tip_roll  = alt.Tooltip("rolling:Q", title=f"{months}-mo avg", format=",d")

    base = alt.Chart(ts).encode(
        x=alt.X("d:T", title=None),
    )

    # Raw series (smoothed curve + explicitly formatted y-axis)
    raw_line = base.mark_line(opacity=0.35,
                            interpolate=("monotone" if smooth else "linear")
                            ).encode(
        y=alt.Y("raw:Q", title=y_title,
                axis=y_axis,
                scale=alt.Scale(zero=True, nice=True)),
        tooltip=[alt.Tooltip("date_label:N", title="Date"), tip_raw],
    )

    # Rolling series (thicker line; share the same scale; no extra axis)
    roll_line = base.mark_line(strokeWidth=3,
                            interpolate=("monotone" if smooth else "linear")
                            ).encode(
        y=alt.Y("rolling:Q", title=y_title,
                axis=None,
                scale=alt.Scale(zero=True, nice=True)),
        
        tooltip=[alt.Tooltip("date_label:N", title="Date"), tip_roll],
        color=alt.value("#ffffff"),
    )

    st.altair_chart((raw_line + roll_line).interactive(), use_container_width=True)
