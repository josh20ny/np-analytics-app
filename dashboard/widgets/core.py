# dashboard/widgets/core.py
from __future__ import annotations
import io
import pandas as pd
import streamlit as st
from datetime import datetime, time
from pandas.api.types import is_numeric_dtype, is_datetime64_any_dtype, is_object_dtype
from data import read_sql

def _format_date_series(s: pd.Series) -> pd.Series:
    s = pd.to_datetime(s, errors="coerce")
    return s.dt.strftime("%B %d, %Y").fillna("")

def _format_display_dates(df: pd.DataFrame, exclude=("parsed_date",)) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if c in exclude: 
            continue
        col = out[c]
        if is_numeric_dtype(col):
            continue
        if is_datetime64_any_dtype(col):
            out.loc[:, c] = _format_date_series(col)
            continue
        if is_object_dtype(col):
            non_null = col.notna().sum()
            if non_null == 0:
                continue
            parsed = pd.to_datetime(col, errors="coerce", infer_datetime_format=True)
            good_ratio = parsed.notna().sum() / non_null
            if good_ratio >= 0.8:
                years = parsed.dt.year.dropna()
                if not years.empty and years.median() >= 1990:
                    out.loc[:, c] = _format_date_series(parsed)
    return out

def _download_button(df: pd.DataFrame, label: str, filename: str):
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    st.download_button(label, buf.getvalue(), file_name=filename, mime="text/csv")

def ranged_table(
    table: str,
    date_col: str,
    *,
    title: str,
    metric_col: str | None = None,
    min_value: int | None = None,
    key: str = "range",
):
    """Standardized “raw table + date range + averages (+ optional metric threshold)” renderer."""
    df = read_sql(f"SELECT * FROM {table}", parse_dates=[date_col])

    st.subheader(title)

    if df.empty:
        st.info("No data.")
        return

    df["parsed_date"] = pd.to_datetime(df[date_col])
    df = df.sort_values("parsed_date", ascending=False)

    default_end = df["parsed_date"].iloc[0]
    default_start = df["parsed_date"].iloc[min(9, len(df)-1)]

    start_date, end_date = st.date_input(
        "Select date range",
        value=(default_start.date(), default_end.date()),
        min_value=df["parsed_date"].min().date(),
        max_value=default_end.date(),
        key=f"{key}_date",
    )
    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date, time.max)

    df_filtered = df[(df["parsed_date"] >= start_dt) & (df["parsed_date"] <= end_dt)].copy()
    if df_filtered.empty:
        st.warning("No rows in selected range.")
        return

    # Optional threshold filtering
    display_df = df_filtered
    if metric_col and metric_col in display_df.columns:
        slider_max = int(max(pd.to_numeric(display_df[metric_col], errors="coerce").max(skipna=True) or 0, min_value or 1))
        ui_min = st.slider(
            f"Minimum rows to show (filter by '{metric_col}')",
            min_value=0,
            max_value=max(slider_max, 1),
            value=int(min_value or 1),
            key=f"{key}_thresh",
        )
        display_df = display_df[pd.to_numeric(display_df[metric_col], errors="coerce").fillna(0) >= ui_min]

    # Pretty date formatting
    display_df = _format_display_dates(display_df)

    # Toggle parsed_date visibility
    show_parsed = st.checkbox("Show parsed_date", value=False, key=f"{key}_show_parsed")
    if not show_parsed:
        display_df = display_df.drop(columns=["parsed_date"], errors="ignore")

    if display_df.empty:
        st.warning("No rows with meaningful data in selected range.")
        return

    st.dataframe(display_df, use_container_width=True)

    # Averages row (numeric columns only)
    numeric_cols = [c for c in display_df.columns if is_numeric_dtype(display_df[c])]
    if numeric_cols:
        avg_row = (display_df[numeric_cols].mean(numeric_only=True).to_frame().T)
        avg_row.index = ["Averages"]
        st.dataframe(avg_row, use_container_width=True)

    # CSV export
    _download_button(display_df, "⬇️ Download CSV", f"{table}_{start_date}_{end_date}.csv")
