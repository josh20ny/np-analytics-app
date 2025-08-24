# dashboard/widgets/giving_ytd.py
from __future__ import annotations
import pandas as pd
import streamlit as st
import altair as alt
from data import engine

def giving_ytd_bar(years_back: int = 5, title: str = "YTD Giving") -> None:
    """
    Compare this year's YTD giving with prior years at the same week index.
    Adds %YoY labels (each year vs prior year).
    Source: weekly_giving_summary.total_giving
    """
    df = pd.read_sql(
        "SELECT week_end, total_giving FROM weekly_giving_summary ORDER BY week_end",
        engine,
        parse_dates=["week_end"],
    )
    if df.empty:
        st.info("No giving data found.")
        return

    df = df.sort_values("week_end").copy()
    df["year"] = df["week_end"].dt.year
    # 1-based week sequence within each year (based on Monday→Sunday weeks)
    df["week_seq"] = df.groupby("year").cumcount() + 1

    latest_year = int(df["year"].max())
    cutoff_seq = int(df.loc[df["year"] == latest_year, "week_seq"].max())

    ytd = (
        df[df["week_seq"] <= cutoff_seq]
        .groupby("year", as_index=False)["total_giving"]
        .sum()
        .rename(columns={"total_giving": "ytd"})
        .sort_values("year")
        .reset_index(drop=True)
    )

    # keep last N years
    min_year = max(latest_year - (years_back - 1), int(ytd["year"].min()))
    ytd = ytd[ytd["year"].between(min_year, latest_year)].copy()

    if ytd.empty:
        st.info("Not enough data to compute YTD comparison.")
        return

    # %YoY vs prior year (for label color/text)
    ytd["prev_ytd"] = ytd["ytd"].shift(1)
    ytd["yoy_pct"] = ((ytd["ytd"] - ytd["prev_ytd"]) / ytd["prev_ytd"] * 100.0).where(ytd["prev_ytd"] > 0)
    def _yoy_label(v):
        if pd.isna(v):
            return ""
        r = round(v)
        if r == 0:
            return "0%"
        sign = "" if r < 0 else ""  # we’ll add symbols in caption; label stays compact
        return f"{sign}{r:d}%"
    ytd["yoy_label"] = ytd["yoy_pct"].apply(_yoy_label)

    # sign bucket for coloring the % label
    def _sign(v):
        if pd.isna(v): 
            return "zero"
        return "pos" if v > 0 else ("neg" if v < 0 else "zero")
    ytd["yoy_sign"] = ytd["yoy_pct"].apply(_sign)

    # Caption delta (current vs last year)
    cur_val = float(ytd.loc[ytd["year"] == latest_year, "ytd"].iloc[0])
    prev_year = latest_year - 1
    if (ytd["year"] == prev_year).any():
        prev_val = float(ytd.loc[ytd["year"] == prev_year, "ytd"].iloc[0])
        if prev_val != 0:
            pct = (cur_val - prev_val) / prev_val * 100.0
            cap_delta = "▲ {:.1f}%".format(pct) if pct > 0 else ("▼ {:.1f}%".format(-pct) if pct < 0 else "–")
        else:
            cap_delta = "–"
    else:
        cap_delta = "–"

    chart_df = ytd.assign(Year=lambda d: d["year"].astype(str))
    chart_df["is_current"] = chart_df["year"] == latest_year

       # Axis formatting (clean dollars)
    y_axis = alt.Axis(format="$,.0f", tickCount=6)

    base = alt.Chart(chart_df)

    # ---- slimmer bars + spacing (Altair v5) ----
    BAR_SIZE = 65  # try 32–44 to taste

    X = alt.X(
        "Year:O",
        title=None,
        sort=None,
        # control spacing between bars (no 'band' in v5)
        scale=alt.Scale(paddingInner=0.55, paddingOuter=0.25),
    )

    bars = base.mark_bar(size=BAR_SIZE).encode(
        x=X,
        y=alt.Y("ytd:Q", title="YTD Giving", axis=y_axis),
        color=alt.Color(
            "is_current:N",
            legend=None,
            scale=alt.Scale(domain=[True, False], range=["#2563eb", "#94a3b8"])  # current=blue
        ),
        tooltip=[
            alt.Tooltip("Year:N", title="Year"),
            alt.Tooltip("ytd:Q", title="YTD", format="$,.2f"),
            alt.Tooltip("yoy_pct:Q", title="% YoY", format=".1f"),
        ],
    )

    # ---- big $ labels above bars (light text for dark theme) ----
    dollar_labels = base.mark_text(
        dy=-12,
        fontSize=18,
        fontWeight="bold",
        color="#e5e7eb",  # light gray/near-white
    ).encode(
        x=X,
        y="ytd:Q",
        text=alt.Text("ytd:Q", format="$,d"),
    )

    # ---- big %YoY labels inside/near top of each bar ----
    # yoy_label / yoy_sign already computed earlier; add arrows if you want:
    def _arrow_label(v):
        if pd.isna(v): return ""
        r = int(round(v))
        if r > 0:  return f"▲ {r}%"
        if r < 0:  return f"▼ {abs(r)}%"
        return "0%"
    chart_df["yoy_arrow"] = chart_df["yoy_pct"].apply(_arrow_label)

    pct_labels = base.mark_text(
        dy=14,
        fontSize=18,
        fontWeight="bold",
    ).encode(
        x=X,
        y="ytd:Q",
        text="yoy_arrow:N",
        color=alt.Color(
            "yoy_sign:N",
            legend=None,
            scale=alt.Scale(
                domain=["pos", "neg", "zero"],
                range=["#16a34a", "#ef4444", "#9ca3af"],  # green / red / gray
            ),
        ),
    )

    st.subheader(title)
    st.caption(f"Aligned to week #{cutoff_seq} of {latest_year}.  Δ vs {prev_year}: {cap_delta}")

    chart = (bars + dollar_labels + pct_labels).properties(height=360)
    st.altair_chart(chart.configure_axis(grid=True), use_container_width=True)
