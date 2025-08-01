import streamlit as st
from data import load_table, engine
from config import TAB_CONFIG
from widgets import (
    pie_chart,
    kpi_card,
    overlay_years_chart,
    filter_meaningful_rows,
)
import pandas as pd
from datetime import datetime, time

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
                    )

                    # reformat the original date column for display
                    if date_col_all in df_filtered.columns:
                        df_filtered[date_col_all] = df_filtered[
                            date_col_all
                        ].dt.strftime("%B %d, %Y")

                    st.subheader(f"Filtered rows for {aud}")
                    display_df = filter_meaningful_rows(
                        df_filtered, date_col="parsed_date", min_value=1
                    )
                    if display_df.empty:
                        st.warning("No rows with meaningful data in selected range.")
                    else:
                        st.dataframe(display_df, use_container_width=True)
                        # Averages row on the cleaned display_df
                        numeric_cols = display_df.select_dtypes(
                            include="number"
                        ).columns
                        if numeric_cols.any():
                            avg_row = (
                                display_df[numeric_cols]
                                .mean(numeric_only=True)
                                .to_frame()
                                .T
                            )
                            avg_row.index = ["Averages"]
                            st.dataframe(avg_row, use_container_width=True)

            else:
                # Standard date‐range table for all other tabs
                df_all = pd.read_sql(
                    f"SELECT * FROM {table_all}", engine, parse_dates=[date_col_all]
                )
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
                )

                # reformat the original date column for display
                if date_col_all in df_filtered.columns:
                    df_filtered[date_col_all] = df_filtered[
                        date_col_all
                    ].dt.strftime("%B %d, %Y")

                st.subheader(f"Filtered rows from `{table_all}` table")
                display_df = filter_meaningful_rows(
                    df_filtered, date_col="parsed_date", min_value=1
                )
                if display_df.empty:
                    st.warning("No rows with meaningful data in selected range.")
                else:
                    st.dataframe(display_df, use_container_width=True)
                    # Averages row on the cleaned display_df
                    numeric_cols = display_df.select_dtypes(
                        include="number"
                    ).columns
                    if numeric_cols.any():
                        avg_row = (
                            display_df[numeric_cols]
                            .mean(numeric_only=True)
                            .to_frame()
                            .T
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
                    latest = raw.sort_values("date").iloc[-1]
                    male_cols = [c for c in latest.index if c.endswith("_male")]
                    female_cols = [c for c in latest.index if c.endswith("_female")]
                    male_sum = sum(
                        (0 if pd.isna(latest[c]) else latest[c]) for c in male_cols
                    )
                    female_sum = sum(
                        (0 if pd.isna(latest[c]) else latest[c]) for c in female_cols
                    )
                    if male_sum + female_sum > 0:
                        pie_chart(None, ["Male", "Female"], [male_sum, female_sum], title)

                # Age/Grade pie
                elif title in ["Age Distribution", "Grade Distribution"]:
                    raw = pd.read_sql(
                        f"SELECT * FROM {table}", engine, parse_dates=["date"]
                    )
                    latest = raw.sort_values("date").iloc[-1]
                    groups = {}
                    for col in latest.index:
                        if (
                            "_" in col
                            and (col.endswith("_male") or col.endswith("_female"))
                            and col not in ["attendance_930", "attendance_1100"]
                        ):
                            key = col.rsplit("_", 2)[1]
                            groups[key] = groups.get(key, 0) + (
                                0 if pd.isna(latest[col]) else latest[col]
                            )
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
                latest = gs.sort_values("date").iloc[-1]["number_of_groups"]
                kpi_card(args["label"], latest)
                continue

            # Line chart
            if widget_fn == overlay_years_chart:
                widget_fn(df, **args)
                continue

            # Default
            widget_fn(df, **args)

