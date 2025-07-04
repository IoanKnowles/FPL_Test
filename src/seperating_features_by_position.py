import pandas as pd
from pathlib import Path

DATA_DIR = Path("data/processed")
MASTER_FEAT = DATA_DIR / "features_2025.parquet"

# Load master features
df = pd.read_parquet(MASTER_FEAT)

# Map element_type to position names (adjust if yours differ)
pos_map = {
    1: "GK",
    2: "DEF",
    3: "MID",
    4: "FWD"
}

for etype, pos_name in pos_map.items():
    sub = df[df["element_type"] == etype].copy()
    out_path = DATA_DIR / f"features_2025_{pos_name}.parquet"
    sub.to_parquet(out_path, index=False)
    print(f"Wrote {pos_name}: {sub.shape[0]} rows → {out_path}")