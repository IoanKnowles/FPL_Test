# src/add_features_to_db.py

import sqlite3
import pandas as pd
from pathlib import Path

# 1. Paths
DB_PATH = Path("data/fpl.db")
FEATURES_PQ = Path("data/processed/features_2025.parquet")

def main():
    # 2. Load your newly baked Features parquet
    df = pd.read_parquet(FEATURES_PQ)

    # 3. Open a sqlite3 connection (will create the DB if it doesn’t exist)
    conn = sqlite3.connect(DB_PATH)

    # 4. Write the table. If it already exists, replace it.
    df.to_sql(
        name="features_2025",
        con=conn,
        if_exists="replace",
        index=False,
    )
    print(f"Wrote {len(df):,} rows into table 'features_2025' in {DB_PATH}")

    conn.close()

if __name__ == "__main__":
    main()