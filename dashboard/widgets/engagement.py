import pandas as pd
import streamlit as st
import altair as alt

# 1) Metric strip for “This Week”
def stat_row(title: str, provider, **kwargs):
    st.subheader(title)
    df = provider(**kwargs)
    if df is None or df.empty:
        st.info("No recent engagement snapshot yet.")
        return
    # Expect columns: label, value
    cols = st.columns(min(6, len(df)))
    for i, (_, r) in enumerate(df.iterrows()):
        with cols[i % len(cols)]:
            st.metric(r["label"], f"{int(r['value']):,}")

# 2) Cadence buckets bar chart
def cadence_bars(title: str, provider, **kwargs):
    st.subheader(title)
    df = provider(**kwargs)
    if df is None or df.empty:
        st.info("No cadence data yet.")
        return
    # Expect columns: signal, bucket, count
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("bucket:N", title="Bucket"),
            y=alt.Y("count:Q", title="People"),
            color=alt.Color("signal:N", title="Signal"),
            column=alt.Column("signal:N", title=None)
        )
        .properties(width=220, height=240)
    )
    st.altair_chart(chart, use_container_width=True)

# 3) People table (e.g., newly lapsed)
def people_table(title: str, provider, limit: int = 100, **kwargs):
    st.subheader(title)
    df = provider(limit=limit, **kwargs)
    if df is None or df.empty:
        st.info("No people to show.")
        return
    st.dataframe(df, use_container_width=True)
