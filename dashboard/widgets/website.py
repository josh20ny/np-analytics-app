# dashboard/widgets/website.py
import streamlit as st
import pandas as pd
from data import read_sql

def website_tab():
    st.title("🌐 Website")

    # ========== Weekly trend ==========
    trend = read_sql("""
        SELECT week_end, week_start, users, page_views, avg_engagement_time_sec
        FROM website_weekly_summary
        ORDER BY week_end
    """, parse_dates=["week_end", "week_start"])

    if trend.empty:
        st.info("No website data yet. Run the GA4 sync and refresh.")
        return

    # Latest week KPIs
    latest = trend.iloc[-1]
    c1, c2, c3 = st.columns(3)
    c1.metric("Users (last wk)", f"{int(latest['users']):,}")
    c2.metric("Page Views (last wk)", f"{int(latest['page_views']):,}")
    c3.metric("Avg Engagement / User (s)", f"{latest['avg_engagement_time_sec']:.0f}")

    # Line chart: users & page_views over time
    chart_df = trend.rename(columns={"week_end": "date"}).set_index("date")[["users", "page_views"]]
    st.line_chart(chart_df)

    # ========== Top pages (last full week) ==========
    st.subheader("Top Pages (last full week)")
    pages = read_sql("""
        WITH last_week AS (SELECT MAX(week_end) AS we FROM website_page_views_weekly)
        SELECT page_key AS page, views
        FROM website_page_views_weekly, last_week
        WHERE week_end = we
        ORDER BY views DESC
        LIMIT 15
    """)
    if pages.empty:
        st.caption("No page data for last week.")
    else:
        st.bar_chart(pages.set_index("page")["views"])

    col1, col2 = st.columns(2)

    # ========== Channel group ==========
    with col1:
        st.subheader("Channel Group (last full week)")
        channel = read_sql("""
            WITH last_week AS (SELECT MAX(week_end) AS we FROM website_channel_group_weekly)
            SELECT channel_group, users, page_views
            FROM website_channel_group_weekly, last_week
            WHERE week_end = we
            ORDER BY page_views DESC
        """)
        if channel.empty:
            st.caption("No channel data for last week.")
        else:
            st.dataframe(channel, use_container_width=True, hide_index=True)

    # ========== Device mix ==========
    with col2:
        st.subheader("Device Mix (last full week)")
        devices = read_sql("""
            WITH last_week AS (SELECT MAX(week_end) AS we FROM website_device_weekly)
            SELECT device_category, sessions, users, page_views
            FROM website_device_weekly, last_week
            WHERE week_end = we
            ORDER BY page_views DESC
        """)
        if devices.empty:
            st.caption("No device data for last week.")
        else:
            st.bar_chart(devices.set_index("device_category")[["page_views"]])

    # ========== Conversions ==========
    st.subheader("Conversions (last full week)")
    conv = read_sql("""
        WITH last_week AS (SELECT MAX(week_end) AS we FROM website_conversions_weekly)
        SELECT conversion_type, event_count
        FROM website_conversions_weekly, last_week
        WHERE week_end = we
        ORDER BY conversion_type
    """)
    if conv.empty:
        st.caption("No conversion events for last week.")
    else:
        st.dataframe(conv, use_container_width=True, hide_index=True)
