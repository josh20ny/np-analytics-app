# dashboard/data.py
import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load env for local/dev; harmless in prod
load_dotenv()

DB_URL = os.getenv("DATABASE_URL") or (
    "postgresql+psycopg2://"
    f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT','5432')}/{os.getenv('DB_NAME')}"
)

@st.cache_resource
def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True, future=True)

# ✅ This is what main.py imports
engine = get_engine()

@st.cache_data(ttl=300, show_spinner=False)
def read_sql(query: str, params=None, parse_dates=None) -> pd.DataFrame:
    return pd.read_sql(query, engine, params=params, parse_dates=parse_dates)

@st.cache_data(ttl=300, show_spinner=False)
def load_table(table: str, date_col: str | None, value_col: str | None) -> pd.DataFrame:
    """
    Legacy-friendly loader:
    - If value_col is provided: returns columns (date, value)
    - Adds year/week/month derived from date for legacy widgets
    - If value_col is None: returns full table, renaming date_col->date if needed
    """
    if value_col:
        df = read_sql(
            f"SELECT {date_col} AS date, {value_col} AS value FROM {table}",
            parse_dates=["date"],
        )
    else:
        parse = [date_col] if date_col else None
        df = read_sql(f"SELECT * FROM {table}", parse_dates=parse)
        if date_col and date_col != "date" and date_col in df.columns:
            df = df.rename(columns={date_col: "date"})

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        iso = df["date"].dt.isocalendar()
        df["year"] = iso.year.astype("Int64")
        df["week"] = iso.week.astype("Int64")
        df["month"] = df["date"].dt.month.astype("Int64")

    return df
