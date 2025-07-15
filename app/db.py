# app/db.py

import psycopg2
from .config import settings

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

def get_conn():
    """
    Raw psycopg2 connection (unchanged).
    """
    return psycopg2.connect(
        dbname   = settings.DB_NAME,
        user     = settings.DB_USER,
        password = settings.DB_PASSWORD,
        host     = settings.DB_HOST,
        port     = settings.DB_PORT,
    )

# ──────────────────────────────────────────────────────────────────────────────────────────
#                  SQLAlchemy ORM setup (for your OAuth app and any future models)
# ──────────────────────────────────────────────────────────────────────────────────────────

# Use the same DATABASE_URL your app.config defines
DATABASE_URL = settings.DATABASE_URL

# Create the engine and session factory
engine       = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# Declarative base for all ORM models
Base = declarative_base()

def get_db():
    """
    FastAPI dependency: yields a SQLAlchemy Session and closes it after use.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
