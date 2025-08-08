import os
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import streamlit as st

# Load environment and create DB engine
load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"))

@st.cache_data(ttl=3600)
def load_table(table_name: str, date_col: str, value_col: str) -> pd.DataFrame:
    """
    Generic loader: fetches full table, parses date_col, sorts, and adds year/week columns.
    Also formats the date into a human-readable string "Month dd, yyyy" in the 'date' column for display.
    Isolates the value_col and renames columns to date/value/year/week.
    """
    # read full table, parse the date column
    df = pd.read_sql(
        f"SELECT * FROM {table_name}",
        engine,
        parse_dates=[date_col]
    )
    df = df.sort_values(date_col)
    
    if value_col is not None and value_col in df.columns:
        df[value_col] = pd.to_numeric(df[value_col], errors='coerce').fillna(0)

    df['year'] = df[date_col].dt.year
    df['week'] = df[date_col].dt.isocalendar().week
    df = df[[date_col, value_col, 'year', 'week']].rename(
        columns={date_col: 'date', value_col: 'value'}
    )
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%B %d, %Y')
    return df