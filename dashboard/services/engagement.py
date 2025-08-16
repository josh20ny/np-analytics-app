# dashboard/widgets/engagement.py
from typing import Callable, Optional
import streamlit as st
import pandas as pd

__all__ = ["stat_row", "cadence_bars", "people_table"]

def _safe_df(x, cols: list[str] | None = None) -> pd.DataFrame:
    if isinstance(x, pd.DataFrame):
        return x
    return pd.DataFrame(columns=cols or [])

def stat_row(_df_unused: Optional[pd.DataFrame], title: str, provider: Callable[[], pd.DataFrame]):
    st.subheader(title)
    df = _safe_df(provider(), ["label", "value"])
    if df.empty or not {"label", "value"}.issubset(df.columns):
        st.info("No engagement snapshot yet — this will populate once your ETL writes the new view.")
        return
    cols = st.columns(len(df))
    for col, (_, r) in zip(cols, df.iterrows()):
        label = str(r.get("label", "—"))
        v = r.get("value", 0)
        try:
            val = int(v if v is not None else 0)
        except Exception:
            val = 0
        col.metric(label, f"{val:,}")

def cadence_bars(_df_unused: Optional[pd.DataFrame], title: str, provider: Callable[[], pd.DataFrame]):
    st.subheader(title)
    df = _safe_df(provider(), ["bucket", "people"])
    if df.empty or not {"bucket", "people"}.issubset(df.columns):
        st.info("No cadence data yet.")
        return
    df = df.copy()
    df["people"] = pd.to_numeric(df["people"], errors="coerce").fillna(0).astype(int)
    st.bar_chart(df.set_index("bucket")["people"])

def people_table(_df_unused: Optional[pd.DataFrame], title: str, provider: Callable[..., pd.DataFrame], limit: int = 100, height: int = 420):
    st.subheader(title)
    try:
        df = provider(limit=limit)
    except TypeError:
        df = provider()
    df = _safe_df(df)
    if df.empty:
        st.info("No rows to show.")
        return
    st.dataframe(df, use_container_width=True, height=height)
