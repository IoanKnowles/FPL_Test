import pandas as pd
import sqlite3
from pathlib import Path

DATA_DIR = Path("data/processed")
DB_PATH  = Path("data/fpl.db")

# define your position files & desired table names
pos_files = {
    "GK":  DATA_DIR / "features_2025_GK.parquet",
    "DEF": DATA_DIR / "features_2025_DEF.parquet",
    "MID": DATA_DIR / "features_2025_MID.parquet",
    "FWD": DATA_DIR / "features_2025_FWD.parquet",
}

with sqlite3.connect(DB_PATH) as conn:
    for pos, path in pos_files.items():
        # read the parquet
        df = pd.read_parquet(path)
        # write to a table named e.g. features_2025_GK
        tbl = f"features_2025_{pos}"
        df.to_sql(tbl, conn, if_exists="replace", index=False)
        print(f"Wrote {len(df)} rows to {tbl}")