# dashboard/lib/db.py
import os
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()  # safe even if already loaded

DB_URL = os.getenv("DATABASE_URL") or (
    "postgresql+psycopg2://"
    f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME')}"
)

engine = create_engine(DB_URL, pool_pre_ping=True, future=True)

@contextmanager
def connect():
    # begin() = transaction w/ auto-commit on exit (OK for writes & reads)
    with engine.begin() as conn:
        yield conn

# ── Users ────────────────────────────────────────────────────────────────────
def fetch_active_users():  # only verified users show up in the login creds
    with connect() as c:
        rows = c.execute(text("""
            SELECT id, email, username, name, role, password_hash
            FROM users
            WHERE is_active = TRUE
              AND is_verified = TRUE
        """)).mappings().all()
        return [dict(r) for r in rows]

def get_user_by_email(email: str):
    with connect() as c:
        row = c.execute(text("SELECT * FROM users WHERE email = :e"), {"e": email}).mappings().first()
        return dict(row) if row else None

def insert_user(email, username, name, role, password_hash):
    with connect() as c:
        c.execute(text("""
            INSERT INTO users (email, username, name, role, password_hash)
            VALUES (:e, :u, :n, :r, :p)
            ON CONFLICT (email) DO NOTHING
        """), {"e": email, "u": username, "n": name, "r": role, "p": password_hash})

def update_password(email, new_hash):
    with connect() as c:
        c.execute(text("""
            UPDATE users
            SET password_hash = :p, updated_at = NOW()
            WHERE email = :e AND is_active = TRUE
        """), {"p": new_hash, "e": email})

# ── Email verification ───────────────────────────────────────────────────────
def set_verification(email: str, code_hash: str, minutes: int | None = None):
    minutes = minutes or int(os.getenv("VERIFICATION_MINUTES", "15"))
    with connect() as c:
        c.execute(text("""
            UPDATE users
            SET verification_code_hash = :h,
                verification_expires_at =
                    (NOW() AT TIME ZONE 'UTC') + (:mins || ' minutes')::INTERVAL
            WHERE email = :e
        """), {"h": code_hash, "mins": minutes, "e": email})

def get_verification(email: str):
    with connect() as c:
        row = c.execute(text("""
            SELECT verification_code_hash AS hash,
                   verification_expires_at AS expires_at
            FROM users
            WHERE email = :e
        """), {"e": email}).mappings().first()
        return dict(row) if row else None

def mark_verified(email: str):
    with connect() as c:
        c.execute(text("""
            UPDATE users
            SET is_verified = TRUE,
                verified_at = NOW(),
                verification_code_hash = NULL,
                verification_expires_at = NULL
            WHERE email = :e
        """), {"e": email})

def fetch_users_all():
    with connect() as c:
        rows = c.execute(text("""
            SELECT id, email, username, name, role, is_active, is_verified,
                   created_at, verified_at, updated_at
            FROM users
            ORDER BY created_at DESC
        """)).mappings().all()
        return [dict(r) for r in rows]

def set_user_role(email: str, role: str):
    with connect() as c:
        c.execute(text("UPDATE users SET role=:r, updated_at=NOW() WHERE email=:e"),
                  {"r": role, "e": email})

def set_user_active(email: str, active: bool):
    with connect() as c:
        c.execute(text("UPDATE users SET is_active=:a, updated_at=NOW() WHERE email=:e"),
                  {"a": active, "e": email})

def approve_user(email: str):
    with connect() as c:
        c.execute(text("""
            UPDATE users
            SET is_verified = TRUE, verified_at = NOW(), updated_at = NOW()
            WHERE email=:e
        """), {"e": email})
