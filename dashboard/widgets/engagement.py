from __future__ import annotations
import pandas as pd
import streamlit as st
import altair as alt
import math

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
def _as_df(data) -> pd.DataFrame:
    # Accept dict {'0–7d': 12, ...}, list[dict], or DataFrame
    if isinstance(data, pd.DataFrame):
        df = data.copy()
    elif isinstance(data, dict):
        df = pd.DataFrame([{"bucket": k, "count": v} for k, v in data.items()])
    elif isinstance(data, (list, tuple)):
        df = pd.DataFrame(data)
    else:
        df = pd.DataFrame()
    # normalize column names
    rename_map = {c.lower(): c for c in df.columns}
    cols = {c: c.lower() for c in df.columns}
    df = df.rename(columns=cols)
    # ensure required columns
    if "bucket" not in df.columns and "label" in df.columns:
        df = df.rename(columns={"label": "bucket"})
    if "count" not in df.columns and "value" in df.columns:
        df = df.rename(columns={"value": "count"})
    # optional delta for vs last week
    if "delta" not in df.columns and "change" in df.columns:
        df = df.rename(columns={"change": "delta"})
    return df

# cadence_bars_v2: accept kwargs + enforce bucket order + nice labels
def cadence_bars_v2(*_ignored, title: str = "Current Cadence Buckets", provider=None,
                    order=None, **kwargs):

    if provider is None:
        st.info("No provider configured.")
        return

    # forward filters (e.g., signals=("attend",) or ("give",))
    raw = provider(**kwargs)
    df = _as_df(raw)
    if df.empty:
        st.info("No cadence data.")
        return

 # ── Order + labels ───────────────────────────────────────────────
    default_order = ["weekly", "biweekly", "monthly", "6weekly", "irregular", "one_off"]
    wanted = order or default_order
    seen = list(df["bucket"].astype(str).unique())
    ordered_buckets = [b for b in wanted if b in seen] + [b for b in seen if b not in wanted]

    label_map = {"biweekly": "bi-weekly", "6weekly": "6-weekly", "one_off": "one-off"}
    df["bucket_label"] = df["bucket"].map(lambda b: label_map.get(b, b))
    ordered_labels = [label_map.get(b, b) for b in ordered_buckets]

    # ── Axis: no scientific notation + room for labels ───────────────
    max_count = int(df["count"].max())
    domain_max = int(math.ceil(max_count * 1.08))  # small right margin for the text labels

    base = alt.Chart(df)

    bars = base.mark_bar(size=20).encode(  # thinner bar -> visible spacing
        y=alt.Y("bucket_label:N", title=None, sort=ordered_labels),
        x=alt.X(
            "count:Q",
            title="People",
            scale=alt.Scale(domain=[0, domain_max], nice=False, zero=True),
            axis=alt.Axis(format=",.0f")  # force 1,000 style (no 1e+3)
        ),
        color=alt.value("#2563eb"),
        tooltip=[
            alt.Tooltip("bucket_label:N", title="Bucket"),
            alt.Tooltip("count:Q", title="People", format=",.0f"),
        ] + ([alt.Tooltip("delta:Q", title="Δ vs last wk", format="+,.0f")]
             if "delta" in df.columns else []),
    )

    # value labels at the end of bars (outside the bar)
    text = base.mark_text(
        align="left", dx=8, baseline="middle", fontWeight="bold", fill="#e5e7eb"
    ).encode(
        y=alt.Y("bucket_label:N", sort=ordered_labels),
        x="count:Q",
        text=alt.Text("count:Q", format=",.0f"),
    )

    st.altair_chart(
        (bars + text).properties(
            title=title,
            height=max(210, 50 * len(ordered_labels)),  # extra height = spacing between bars
        ),
        use_container_width=True,
    )


# 3) People table (e.g., newly lapsed)
def people_table(title: str, provider, limit: int = 100, **kwargs):
    st.subheader(title)
    df = provider(limit=limit, **kwargs)
    if df is None or df.empty:
        st.info("No people to show.")
        return
    st.dataframe(df, use_container_width=True)
