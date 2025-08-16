import os
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

# Build engine from DATABASE_URL or discrete vars
DB_URL = os.getenv("DATABASE_URL") or (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)

def read_sql(sql, params=None, parse_dates=None):
    """Simple pass-through to pandas.read_sql using the shared engine."""
    return pd.read_sql(sql, engine, params=params, parse_dates=parse_dates)

# dashboard/data.py (only the load_table function needs to change)

@st.cache_data(ttl=300, show_spinner=False)
def load_table(table: str, date_col: str | None, value_col: str | None) -> pd.DataFrame:
    """
    Legacy-friendly loader:
      - If value_col is provided, select (date, value) with standard names.
      - If value_col is None, return full table but rename date_col -> 'date' if present.
      - Always derive year/week/month from 'date' for the legacy widgets.
    """
    if date_col and value_col:
        # Standardize to 'date' and 'value' for charts
        df = read_sql(
            f"SELECT {date_col} AS date, {value_col} AS value FROM {table}",
            parse_dates=["date"],
        )
    else:
        parse = [date_col] if date_col else None
        df = read_sql(f"SELECT * FROM {table}", parse_dates=parse)
        if date_col and date_col in df.columns and date_col != "date":
            df = df.rename(columns={date_col: "date"})

    # Derive time parts the legacy widgets expect
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        iso = df["date"].dt.isocalendar()
        df["year"] = iso.year.astype("Int64")
        df["week"] = iso.week.astype("Int64")
        df["month"] = df["date"].dt.month.astype("Int64")

    return df

