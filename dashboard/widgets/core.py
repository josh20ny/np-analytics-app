# dashboard/widgets/core.py
from __future__ import annotations
import re
import pandas as pd
import streamlit as st
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_datetime64tz_dtype,
    is_object_dtype,
    is_numeric_dtype,
)
from data import read_sql

DATEISH_COLUMNS = {
    "date", "week_start", "week_end",
    "published_at", "last_seen", "observed_none_since", "expected_by",
    "created_at", "updated_at", "verified_at",
}

# Regex: "YYYY-MM-DD" optionally followed by time and tz
ISO_DT_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)?$"
)

def _fmt_month_day_year(s: pd.Series) -> pd.Series:
    # Convert *anything* parseable to "August 10, 2025"
    s = pd.to_datetime(s, errors="coerce", utc=False)
    return (s.dt.strftime("%B ") + s.dt.day.astype("Int64").astype("string") + s.dt.strftime(", %Y")).astype("string")

def _looks_like_iso(series: pd.Series) -> bool:
    sample = series.dropna().astype(str).head(30)
    if sample.empty:
        return False
    hits = sample.map(lambda x: bool(ISO_DT_RE.match(x))).sum()
    return (hits / len(sample)) >= 0.6

def format_date_series(s: pd.Series) -> pd.Series:
    """
    Convert anything parseable to 'Month D, YYYY' strings.
    Avoids platform issues with %-d by composing the day manually.
    """
    s = pd.to_datetime(s, errors="coerce", utc=False)
    return (s.dt.strftime("%B ")
            + s.dt.day.astype("Int64").astype("string")
            + s.dt.strftime(", %Y")).astype("string")

def format_display_dates(df: pd.DataFrame, exclude=("parsed_date",)) -> pd.DataFrame:
    """
    Force all columns that are datetimes or look like datetimes
    into 'Month D, YYYY' STRINGS for display (non-destructive to the original df).
    """
    out = df.copy()
    for col in out.columns:
        if col in exclude:
            continue
        series = out[col]
        if is_datetime64_any_dtype(series) or is_datetime64tz_dtype(series):
            out[col] = format_date_series(series)
        elif (col in DATEISH_COLUMNS or is_object_dtype(series)) and _looks_like_iso(series):
            out[col] = format_date_series(series)
    return out

def ranged_table(
    table: str,
    date_col: str,
    key: str,
    metric_col: str | None = None,
    min_value: float | None = None,
):
    """Raw data slice with date-range picker + optional numeric threshold."""
    df = read_sql(f"SELECT * FROM {table}", parse_dates=[date_col])
    if df.empty:
        st.info("No data.")
        return

    # Keep a working timestamp for filtering only (never shown)
    df["parsed_date"] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.sort_values("parsed_date", ascending=False)

    default_end = df["parsed_date"].iloc[0]
    default_start = df["parsed_date"].iloc[min(9, len(df) - 1)]

    start_date, end_date = st.date_input(
        "Select date range",
        value=(default_start.date(), default_end.date()),
        min_value=df["parsed_date"].min().date(),
        max_value=default_end.date(),
        key=f"range_{key}",
    )
    start_dt = pd.Timestamp.combine(pd.Timestamp(start_date), pd.Timestamp.min.time())
    end_dt   = pd.Timestamp.combine(pd.Timestamp(end_date),   pd.Timestamp.max.time())

    df_filtered = df[(df["parsed_date"] >= start_dt) & (df["parsed_date"] <= end_dt)].copy()

    # Optional threshold filter (e.g., InsideOut > 50)
    if metric_col and metric_col in df_filtered.columns and min_value is not None:
        df_filtered = df_filtered[
            pd.to_numeric(df_filtered[metric_col], errors="coerce").fillna(0) >= min_value
        ]

    # 👉 Convert ALL date-ish columns to strings for display
    display_df = format_display_dates(df_filtered, exclude=("parsed_date",))

    # Always hide the helper column from the table
    display_df = display_df.drop(columns=["parsed_date"], errors="ignore")

    if display_df.empty:
        st.warning("No rows in selected range.")
    else:
        st.dataframe(display_df, use_container_width=True)

        # Averages row (numeric columns only)
        numeric_cols = display_df.select_dtypes(include="number").columns
        if len(numeric_cols) > 0:
            avg_row = display_df[numeric_cols].mean(numeric_only=True).to_frame().T
            avg_row.index = ["Averages"]
            st.dataframe(avg_row, use_container_width=True)
