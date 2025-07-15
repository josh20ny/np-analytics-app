# clickup_app/database.py

from app.db import engine, Base

def init_db():
    """
    Ensure the clickup_tokens table (and any other ORM tables) exists.
    """
    Base.metadata.create_all(bind=engine)

