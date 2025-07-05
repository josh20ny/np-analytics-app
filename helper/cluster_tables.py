# cluster_tables.py

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ─── Load config ───────────────────────────────────────────────────────────────
load_dotenv()
engine = create_engine(os.environ["DATABASE_URL"], echo=True)

# ─── Table → date‐column mapping ────────────────────────────────────────────────
tables = {
    "livestreams":           "published_at",
    "adult_attendance":      "date",
    "waumbaland_attendance": "date",
    "upstreet_attendance":   "date",
    "transit_attendance":    "date",
    "insideout_attendance":  "date",
}

# ─── Cluster each table ─────────────────────────────────────────────────────────
def cluster_all():
    with engine.begin() as conn:
        for table, col in tables.items():
            idx_name = f"{table}_{col}_idx"
            # 1) ensure index exists
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({col});"
            ))
            print(f"✔ ensured index {idx_name}")
            # 2) cluster table on that index
            conn.execute(text(
                f"CLUSTER {table} USING {idx_name};"
            ))
            print(f"✔ clustered {table} on {col}")
            # 3) refresh planner statistics
            conn.execute(text(f"ANALYZE {table};"))
            print(f"✔ analyzed {table}")
    print("🎉 All tables clustered!")

if __name__ == "__main__":
    cluster_all()
