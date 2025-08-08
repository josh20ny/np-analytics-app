import streamlit as st
from data import load_table, engine
from config import TAB_CONFIG
# Optional per-tab filters (e.g., InsideOut: keep rows where total_attendance >= 5)
try:
    from config import TABLE_FILTERS  # optional override
except Exception:
    TABLE_FILTERS = {}

from widgets import (
    pie_chart,
    kpi_card,
    overlay_years_chart,
    filter_meaningful_rows,  # signature: (df, metric_col=None, min_value=1)
)
import pandas as pd
from datetime import datetime, time
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_object_dtype,
    is_numeric_dtype,
)

# ──────────────────────────────────────────────────────────────────────────────
# Default per-tab filters (override by defining TABLE_FILTERS in config.py)
DEFAULT_TABLE_FILTERS = {
    "InsideOut": {"metric_col": "total_attendance", "min_value": 5},
}
TABLE_FILTERS = {**DEFAULT_TABLE_FILTERS, **TABLE_FILTERS}

def format_date_series(s: pd.Series) -> pd.Series:
    s = pd.to_datetime(s, errors="coerce")
    return s.dt.strftime("%B ") + s.dt.day.astype(str) + s.dt.strftime(", %Y")

def format_display_dates(df: pd.DataFrame, exclude=("parsed_date",)) -> pd.DataFrame:
    """
    Convert only actual date-like columns to 'Month D, YYYY'.
    - Skip numeric columns entirely (keeps counts/ratios numeric).
    - For object columns, only treat as dates if ≥80% parse AND median year >= 1990.
    - Always format true datetime64 columns.
    """
    out = df.copy()
    for c in out.columns:
        if c in exclude:
            continue
        col = out[c]

        # 1) Never touch numeric columns
        if is_numeric_dtype(col):
            continue

        # 2) True datetimes → format
        if is_datetime64_any_dtype(col):
            out.loc[:, c] = format_date_series(col)
            continue

        # 3) Object columns → cautiously parse
        if is_object_dtype(col):
            non_null = col.notna().sum()
            if non_null == 0:
                continue
            parsed = pd.to_datetime(col, errors="coerce", infer_datetime_format=True)
            good_ratio = parsed.notna().sum() / non_null
            if good_ratio >= 0.8:
                years = parsed.dt.year.dropna()
                if not years.empty and years.median() >= 1990:
                    out.loc[:, c] = format_date_series(parsed)

    return out
# ──────────────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="NP Analytics", layout="wide")
st.title("📊 NP Analytics")

tabs = st.tabs(list(TAB_CONFIG.keys()))
for tab_obj, tab_name in zip(tabs, TAB_CONFIG):
    with tab_obj:
        widgets = TAB_CONFIG[tab_name]

        if not widgets:
            st.write(f"**{tab_name}** tab coming soon!")
            continue

        # ─── Dynamic Raw Data Table with Date Range ─────────────────────────────
        first_loader = widgets[0]["loader"]
        table_all, date_col_all, value_col_all = first_loader

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
                        f"SELECT * FROM {table_all} WHERE audience_name = %s",
                        engine,
                        params=(aud,),
                        parse_dates=[date_col_all],
                    )
                    st.subheader(f"Filtered rows for {aud}")

                    if df_aud.empty:
                        st.info("No data.")
                        continue

                    df_aud["parsed_date"] = pd.to_datetime(df_aud[date_col_all])
                    df_aud = df_aud.sort_values("parsed_date", ascending=False)

                    default_end = df_aud["parsed_date"].iloc[0]
                    default_start = (
                        df_aud["parsed_date"].iloc[9]
                        if len(df_aud) >= 10
                        else df_aud["parsed_date"].min()
                    )

                    start_date, end_date = st.date_input(
                        f"Select date range for {aud}",
                        value=(default_start.date(), default_end.date()),
                        min_value=df_aud["parsed_date"].min().date(),
                        max_value=default_end.date(),
                        key=f"aud_range_{aud}",
                    )
                    start_dt = datetime.combine(start_date, time.min)
                    end_dt = datetime.combine(end_date, time.max)

                    df_filtered = (
                        df_aud[
                            (df_aud["parsed_date"] >= start_dt)
                            & (df_aud["parsed_date"] <= end_dt)
                        ]
                        .sort_values("parsed_date", ascending=False)
                        .copy()
                    )

                    # Reformat visible date-ish columns
                    display_df = format_display_dates(df_filtered)

                    # Optional: show/hide parsed_date
                    show_parsed = st.checkbox(
                        "Show parsed_date", value=False, key=f"show_parsed_{tab_name}_{aud}"
                    )
                    if not show_parsed:
                        display_df = display_df.drop(columns=["parsed_date"], errors="ignore")

                    # Force-format common date columns
                    for col in ("date", "week_start", "week_end"):
                        if col in display_df.columns:
                            display_df[col] = format_date_series(display_df[col])

                    if display_df.empty:
                        st.warning("No rows in selected range.")
                    else:
                        st.dataframe(display_df, use_container_width=True)

                        # Averages row (numeric columns only)
                        numeric_cols = display_df.select_dtypes(include="number").columns
                        if len(numeric_cols) > 0:
                            avg_row = (
                                display_df[numeric_cols].mean(numeric_only=True).to_frame().T
                            )
                            avg_row.index = ["Averages"]
                            st.dataframe(avg_row, use_container_width=True)

            else:
                # Standard date‐range table for all other tabs
                df_all = pd.read_sql(
                    f"SELECT * FROM {table_all}", engine, parse_dates=[date_col_all]
                )

                st.subheader(f"Filtered rows from `{table_all}` table")

                if df_all.empty:
                    st.info("No data.")
                else:
                    df_all["parsed_date"] = pd.to_datetime(df_all[date_col_all])
                    df_all = df_all.sort_values("parsed_date", ascending=False)

                    default_end = df_all["parsed_date"].iloc[0]
                    default_start = (
                        df_all["parsed_date"].iloc[9]
                        if len(df_all) >= 10
                        else df_all["parsed_date"].min()
                    )

                    start_date, end_date = st.date_input(
                        f"Select date range for {tab_name}",
                        value=(default_start.date(), default_end.date()),
                        min_value=df_all["parsed_date"].min().date(),
                        max_value=default_end.date(),
                        key=f"range_{tab_name}",
                    )
                    start_dt = datetime.combine(start_date, time.min)
                    end_dt = datetime.combine(end_date, time.max)

                    df_filtered = (
                        df_all[
                            (df_all["parsed_date"] >= start_dt)
                            & (df_all["parsed_date"] <= end_dt)
                        ]
                        .sort_values("parsed_date", ascending=False)
                        .copy()
                    )

                    # Live threshold slider if this tab has a configured metric filter
                    cfg = TABLE_FILTERS.get(tab_name)
                    display_df = df_filtered
                    if cfg and cfg.get("metric_col") in df_filtered.columns:
                        metric_col = cfg["metric_col"]
                        metric_series = pd.to_numeric(df_filtered[metric_col], errors="coerce")
                        # choose a reasonable slider max
                        slider_max = int(max(metric_series.max(skipna=True) or 0, cfg.get("min_value", 1)))
                        ui_min = st.slider(
                            f"Minimum rows to show (filter by '{metric_col}')",
                            min_value=0,
                            max_value=max(slider_max, 1),
                            value=int(cfg.get("min_value", 1)),
                            key=f"thresh_{tab_name}",
                        )
                        display_df = filter_meaningful_rows(df_filtered, metric_col=metric_col, min_value=ui_min)
                    elif cfg:
                        # fall back to static config if metric column missing
                        display_df = filter_meaningful_rows(df_filtered, **cfg)

                    # Reformat visible date-ish columns
                    display_df = format_display_dates(display_df)

                    # Optional: show/hide parsed_date
                    show_parsed = st.checkbox(
                        "Show parsed_date", value=False, key=f"show_parsed_{tab_name}"
                    )
                    if not show_parsed:
                        display_df = display_df.drop(columns=["parsed_date"], errors="ignore")

                    # Force-format common date columns
                    for col in ("date", "week_start", "week_end"):
                        if col in display_df.columns:
                            display_df[col] = format_date_series(display_df[col])

                    if display_df.empty:
                        st.warning("No rows with meaningful data in selected range.")
                    else:
                        st.dataframe(display_df, use_container_width=True)

                        # Averages row (numeric columns only)
                        numeric_cols = display_df.select_dtypes(include="number").columns
                        if len(numeric_cols) > 0:
                            avg_row = (
                                display_df[numeric_cols].mean(numeric_only=True).to_frame().T
                            )
                            avg_row.index = ["Averages"]
                            st.dataframe(avg_row, use_container_width=True)

        except Exception as e:
            st.warning(f"Could not load data for `{table_all}`: {e}")

        # ─── Widgets ─────────────────────────────────────────
        for meta in widgets:
            table, date_col, value_col = meta["loader"]
            widget_fn = meta["widget"]
            args = meta["args"].copy()
            df = load_table(table, date_col, value_col) if value_col else None

            if widget_fn == pie_chart:
                title = args.pop("title")
                # Service time pie
                if (
                    title == "Service Time Distribution"
                    and table
                    in [
                        "adult_attendance",
                        "waumbaland_attendance",
                        "upstreet_attendance",
                        "transit_attendance",
                    ]
                ):
                    df_att = pd.read_sql(
                        f"SELECT date, attendance_930, attendance_1100 FROM {table}",
                        engine,
                        parse_dates=["date"],
                    )
                    if df_att.empty:
                        continue
                    latest = df_att.sort_values("date").iloc[-1]
                    labels = ["9:30 AM", "11:00 AM"]
                    values = [latest["attendance_930"], latest["attendance_1100"]]
                    if sum(values) > 0:
                        pie_chart(None, labels, values, title)

                # Gender pie
                elif title == "Gender Distribution":
                    raw = pd.read_sql(
                        f"SELECT * FROM {table}", engine, parse_dates=["date"]
                    )
                    if raw.empty:
                        continue
                    latest = raw.sort_values("date").iloc[-1]
                    male_cols = [c for c in latest.index if c.endswith("_male")]
                    female_cols = [c for c in latest.index if c.endswith("_female")]
                    male_sum = sum((0 if pd.isna(latest[c]) else latest[c]) for c in male_cols)
                    female_sum = sum((0 if pd.isna(latest[c]) else latest[c]) for c in female_cols)
                    if male_sum + female_sum > 0:
                        pie_chart(None, ["Male", "Female"], [male_sum, female_sum], title)

                # Age/Grade pie
                elif title in ["Age Distribution", "Grade Distribution"]:
                    raw = pd.read_sql(
                        f"SELECT * FROM {table}", engine, parse_dates=["date"]
                    )
                    if raw.empty:
                        continue
                    latest = raw.sort_values("date").iloc[-1]
                    groups = {}
                    for col in latest.index:
                        if (
                            "_" in col
                            and (col.endswith("_male") or col.endswith("_female"))
                            and col not in ["attendance_930", "attendance_1100"]
                        ):
                            key = col.rsplit("_", 2)[1]
                            groups[key] = groups.get(key, 0) + (0 if pd.isna(latest[col]) else latest[col])
                    if sum(groups.values()) > 0:
                        pie_chart(None, list(groups.keys()), list(groups.values()), title)

                continue

            # KPI card for groups
            if widget_fn == kpi_card and table == "groups_summary":
                gs = pd.read_sql(
                    "SELECT date, number_of_groups FROM groups_summary",
                    engine,
                    parse_dates=["date"],
                )
                if not gs.empty:
                    latest = gs.sort_values("date").iloc[-1]["number_of_groups"]
                    kpi_card(args["label"], latest)
                continue

            # Line chart
            if widget_fn == overlay_years_chart:
                widget_fn(df, **args)
                continue

            # Default
            widget_fn(df, **args)
