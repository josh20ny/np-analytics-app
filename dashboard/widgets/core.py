import io
import pandas as pd
import streamlit as st
from pandas.api.types import is_numeric_dtype, is_datetime64_any_dtype, is_object_dtype
from datetime import datetime, time
from data import read_sql

def _download_button(df: pd.DataFrame, label: str, filename: str):
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    st.download_button(label, buf.getvalue(), file_name=filename, mime="text/csv")

def _format_date_series(s: pd.Series) -> pd.Series:
    s = pd.to_datetime(s, errors="coerce")
    return s.dt.strftime("%B ") + s.dt.day.astype(str) + s.dt.strftime(", %Y")

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

def ranged_table(table: str, date_col: str, key: str):
    """Reusable raw-data table with a date range picker and CSV export."""
    df_all = read_sql(f"SELECT * FROM {table}", parse_dates=[date_col])
    st.subheader(f"Filtered rows from `{table}`")

    if df_all.empty:
        st.info("No data.")
        return

    df_all["parsed_date"] = pd.to_datetime(df_all[date_col])
    df_all = df_all.sort_values("parsed_date", ascending=False)

    default_end = df_all["parsed_date"].iloc[0]
    default_start = (
        df_all["parsed_date"].iloc[9]
        if len(df_all) >= 10 else df_all["parsed_date"].min()
    )

    start_date, end_date = st.date_input(
        f"Select date range",
        value=(default_start.date(), default_end.date()),
        min_value=df_all["parsed_date"].min().date(),
        max_value=default_end.date(),
        key=f"{key}_date_range",
    )
    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date, time.max)

    display_df = (
        df_all[(df_all["parsed_date"] >= start_dt) & (df_all["parsed_date"] <= end_dt)]
        .sort_values("parsed_date", ascending=False)
        .copy()
    )

    # Format visible date-like columns for readability
    display_df = _format_display_dates(display_df)

    # Optional: show parsed_date
    show_parsed = st.checkbox("Show parsed_date", value=False, key=f"{key}_show_parsed")
    if not show_parsed:
        display_df = display_df.drop(columns=["parsed_date"], errors="ignore")

    if display_df.empty:
        st.warning("No rows with meaningful data in selected range.")
        return

    st.dataframe(display_df, use_container_width=True)

    # Averages row for numeric columns
    numeric_cols = [c for c in display_df.columns if is_numeric_dtype(display_df[c])]
    if numeric_cols:
        avg_row = (display_df[numeric_cols].mean(numeric_only=True).to_frame().T)
        avg_row.index = ["Averages"]
        st.dataframe(avg_row, use_container_width=True)

    _download_button(display_df, "⬇️ Download CSV", f"{table}_{start_date}_{end_date}.csv")
