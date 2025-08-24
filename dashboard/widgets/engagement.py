from __future__ import annotations
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

def cadence_bars_v2(*_ignored, title: str = "Current Cadence Buckets", provider=None):
    """
    Drop-in replacement for `cadence_bars`:
    - provider(): returns dict / list[dict] / DataFrame with columns:
        bucket (str), count (int), [delta (int or float, optional)]
    - renders horizontal bars with big count labels and color-coded deltas
    """
    if provider is None:
        st.info("No provider configured.")
        return

    raw = provider()
    df = _as_df(raw)
    if df.empty:
        st.info("No cadence data.")
        return

    # nice display order if buckets look like recency ranges
    # normalize dash type so "0–7d" and "0-7d" both match
    bk = df["bucket"].astype(str).str.replace("–", "-", regex=False).str.strip()
    df["bucket"] = bk

    order_hint = ["0-7d","8-30d","31-60d","61-90d","91-180d","181-365d",">365d"]
    cats = [b for b in order_hint if b in set(bk)]

    # fallback: natural-ish order by first number if we don't recognize the labels
    if not cats:
        cats = sorted(bk.unique(), key=lambda s: int("".join(ch for ch in s if ch.isdigit()) or "0"))

    df["bucket"] = pd.Categorical(df["bucket"], categories=cats, ordered=True)
    df = df.sort_values("bucket")


    base = alt.Chart(df)

    bars = base.mark_bar(size=28).encode(
        y=alt.Y("bucket:N", title=None, sort=cats),
        x=alt.X("count:Q", title="People", axis=alt.Axis(format=",")),
        color=alt.value("#2563eb"),
        tooltip=[
            alt.Tooltip("bucket:N", title="Bucket"),
            alt.Tooltip("count:Q",  title="People", format=","),
            alt.Tooltip("delta:Q",  title="Δ vs last wk", format="+,"),
        ],
    )

    # Large count labels
    count_labels = base.mark_text(
        align="left", dx=6, fontSize=16, fontWeight="bold", color="#e5e7eb"
    ).encode(
        y="bucket:N",
        x="count:Q",
        text=alt.Text("count:Q", format=","),
    )

    # Optional Δ labels (colored)
    if "delta" in df.columns:
        df["delta_str"] = df["delta"].apply(lambda v: "" if pd.isna(v) else f"{v:+,}")
        df["delta_sign"] = df["delta"].apply(lambda v: "pos" if (pd.notna(v) and v > 0) else ("neg" if pd.notna(v) and v < 0 else "zero"))
        delta_labels = base.mark_text(
            align="left", dx=80, fontSize=14, fontWeight="bold"
        ).encode(
            y="bucket:N",
            x="count:Q",
            text="delta_str:N",
            color=alt.Color("delta_sign:N", legend=None,
                            scale=alt.Scale(domain=["pos","neg","zero"],
                                            range=["#16a34a","#ef4444","#9ca3af"])),
        )
        chart = bars + count_labels + delta_labels
    else:
        chart = bars + count_labels

    st.subheader(title)
    st.altair_chart(chart.properties(height=280).configure_axis(grid=True), use_container_width=True)


# 3) People table (e.g., newly lapsed)
def people_table(title: str, provider, limit: int = 100, **kwargs):
    st.subheader(title)
    df = provider(limit=limit, **kwargs)
    if df is None or df.empty:
        st.info("No people to show.")
        return
    st.dataframe(df, use_container_width=True)
