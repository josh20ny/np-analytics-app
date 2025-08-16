import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime


def overlay_years_chart(df: pd.DataFrame, title: str):
    """
    Interactive line chart overlaying each year for the metric 'value'.
    Uses a unique key for the multiselect to avoid duplicate IDs.
    """
    st.subheader(title)

    # 1) pick which years to overlay
    years = sorted(df['year'].unique())
    pick = st.multiselect(
        'Years to compare',
        years,
        default=years[-2:],
        key=f"years_compare_{title.replace(' ', '_')}"
    )
    if not pick:
        st.warning("Pick at least one year to compare.")
        return

    # 2) pivot into a year × week table
    weekly = (
        df
        .groupby(['year','week'])['value']
        .sum()
        .unstack('year')
        .fillna(0)
    )
    plot_df = weekly[pick]

    # 3) ensure we have a real datetime column to map from
    if 'parsed_date' not in df.columns:
        df['parsed_date'] = pd.to_datetime(df['date'], format='%B %d, %Y')

    # 4) build a week → date map *only* from your baseline year
    baseline = pick[-1]
    date_map = (
        df[df['year'] == baseline]
          .groupby('week')['parsed_date']
          .max()
    )

    # 5) convert ISO week index → real datetime index
    dt_index = date_map.reindex(plot_df.index)
    plot_df.index = dt_index

    # 6) drop any weeks without a match, then sort chronologically
    plot_df = plot_df[~plot_df.index.isna()].sort_index()

    # 7) hand it off to Streamlit (DatetimeIndex → proper time axis)
    st.line_chart(plot_df, use_container_width=True)




def weekly_yoy_table(df: pd.DataFrame, title: str):
    """
    Displays a table with Date, Last Year value, This Year value, and YoY %.
    Hides future weeks (based on current-year dates) and orders the most recent at the top.
    """
    st.subheader(title)
    # Determine years present
    years = sorted(df['year'].unique())
    if len(years) < 2:
        st.write("Not enough years of data for YoY.")
        return
    last, now = years[-2], years[-1]

    # Aggregate values by ISO week and year
    weekly = df.groupby(['week','year'])['value'].sum().unstack('year').fillna(0)
    comp = weekly[[last, now]].copy()
    # Compute YoY percentage
    comp['YoY %'] = (comp[now] - comp[last]).div(comp[last].replace(0, pd.NA)).mul(100)

    dates = df[df['year'] == now].groupby('week')['date'].max()
    # Map ISO week to the actual most recent date in current year
    dates = df[df['year'] == now].groupby('week')['date'].max()
    # Reset index to bring week into column, map to dates, parse dates
    comp = comp.reset_index().rename(columns={'week': 'ISO Week'})
    comp['Date'] = comp['ISO Week'].map(dates)
    comp['Date_parsed'] = pd.to_datetime(comp['Date'], format='%B %d, %Y')

    # Filter out any future dates beyond today
    today = datetime.today()
    comp = comp[comp['Date_parsed'] <= today]

    # Prepare display table sorted most recent first
    display = comp[['Date', last, now, 'YoY %']].copy()
    display = display.set_index('Date')
    # Sort index as parsed dates in descending order
    display.index = pd.to_datetime(display.index, format='%B %d, %Y')
    display = display.sort_index(ascending=False)
    # Reformat index back to string
    display.index = display.index.strftime('%B %d, %Y')
    display = display.reset_index().rename(columns={'index': 'Date'})

    # Style and render
    styled = (
        display.style
               .format({last: '{:,.0f}', now: '{:,.0f}', 'YoY %': '{:+.1f}%'} )
               .applymap(lambda v: 'background-color: #40e060' if isinstance(v, (int, float)) and v > 0 else 'background-color: #d9374a', subset=['YoY %'])
    )
    st.dataframe(styled, use_container_width=True)


def pie_chart(_df, labels: list, values: list, title: str):
    """
    Pie chart from lists of labels and values, sanitizing NaNs.
    """
    clean_values = [0 if pd.isna(v) else int(v) for v in values]
    total = sum(clean_values)
    if total == 0:
        st.write(f"No data to display for {title}")
        return
    fig, ax = plt.subplots()
    ax.pie(
        clean_values,
        labels=labels,
        autopct='%1.1f%%',
        startangle=90,
        wedgeprops={'edgecolor':'white'}
    )
    ax.set_title(title)
    ax.axis('equal')
    st.pyplot(fig)


def kpi_card(label: str, value, delta=None):
    """
    Displays a single KPI metric. Optionally show delta.
    """
    if delta is not None:
        st.metric(label, value, delta)
    else:
        st.metric(label, value)


def date_range_table(df: pd.DataFrame, title: str):
    """
    Displays a table filtered by a selected date range.
    Works with any table that has a formatted 'date' column.
    """
    st.subheader(title + " (Filtered Table)")

    df['parsed_date'] = pd.to_datetime(df['date'], format='%B %d, %Y')
    df = df.sort_values('parsed_date')

    # Default to last 10 dates
    default_end = df['parsed_date'].max()
    default_start = df['parsed_date'].iloc[-10] if len(df) >= 10 else df['parsed_date'].min()

    start_date, end_date = st.date_input(
        "Select date range",
        value=(default_start, default_end),
        min_value=df['parsed_date'].min(),
        max_value=default_end,
        key=f"date_range_table_{title.replace(' ', '_')}"
    )

    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())
    filtered = df[(df['parsed_date'] >= start_dt) & (df['parsed_date'] <= end_dt)]

    if filtered.empty:
        st.warning("No data in selected range.")
        return

    # ——— hide rows where ALL metrics are ≤ 1 ———
    display_df = filtered.drop(columns='parsed_date')

    # 1) treat every column except 'date' as a metric
    metric_cols = [c for c in display_df.columns if c != 'date']

    # 2) coerce everything to numeric (None/invalid → NaN → 0)
    metrics_num = (
        display_df[metric_cols]
        .apply(pd.to_numeric, errors='coerce')
        .fillna(0)
    )

    # 3) keep only rows where at least one metric > 1
    mask = metrics_num.gt(1).any(axis=1)
    display_df = display_df.loc[mask]

    if display_df.empty:
        st.warning("No rows with meaningful data in selected range.")
        return

    st.dataframe(display_df, use_container_width=True)


def filter_meaningful_rows(df: pd.DataFrame, metric_col: str | None = None, min_value: float = 1):
    """
    Return df with only rows where metric_col >= min_value.
    Only the metric column is coerced to numeric; all other columns keep their original dtype.
    If metric_col is None, we try to infer a sensible one.
    """
    df = df.copy()

    # Try to infer if not provided
    if metric_col is None:
        for c in ["total_attendance", "value", "total_giving", "giving_units"]:
            if c in df.columns:
                metric_col = c
                break

    # If we still don't have a metric col, just return as-is
    if not metric_col or metric_col not in df.columns:
        return df

    # Coerce ONLY the metric column
    df[metric_col] = pd.to_numeric(df[metric_col], errors="coerce")

    # Filter by threshold (treat NaN as 0)
    return df[df[metric_col].fillna(0) >= min_value]


