# dashboard/widgets.py
import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd


def overlay_years_chart(df: pd.DataFrame, title: str):
    """
    Interactive line chart overlaying each year for the metric 'value'.
    Uses a unique key for the multiselect to avoid duplicate IDs.
    """
    st.subheader(title)
    years = sorted(df['year'].unique())
    # Use title in key to ensure uniqueness
    pick = st.multiselect(
        'Years to compare',
        years,
        default=years[-2:],
        key=f"years_compare_{title.replace(' ', '_')}"
    )
    data = df.groupby(['year','week'])['value'].sum().unstack('year').fillna(0)
    plot = data[pick]
    st.line_chart(plot, use_container_width=True)


def weekly_yoy_table(df: pd.DataFrame, title: str):
    """
    Displays a table of last year vs this year and YoY % by ISO week.
    """
    st.subheader(title)
    tbl = df.groupby(['week', 'year'])['value'].sum().unstack('year').fillna(0)
    years = sorted(tbl.columns)
    if len(years) < 2:
        st.write("Not enough years of data for YoY.")
        return
    last, now = years[-2], years[-1]
    comp = tbl[[last, now]].copy()
    comp['YoY %'] = (comp[now] - comp[last]) / comp[last] * 100
    styled = (
        comp.style
            .format({last: '{:,.0f}', now: '{:,.0f}', 'YoY %': '{:+.1f}%'} )
            .applymap(lambda v: 'background-color: #40e060' if v > 0 else 'background-color: #d9374a', subset=['YoY %'])
    )
    st.dataframe(styled, use_container_width=True)


def pie_chart(_df, labels: list, values: list, title: str):
    """
    Pie chart from lists of labels and values, sanitizing NaNs.
    """
    # Replace NaN with zero and convert to int
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