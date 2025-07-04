# scripts/prune_features.py

import pandas as pd
from pathlib import Path

DATA_DIR = Path("data/processed")

# 1) Prune GK
GK_DROP = [
    "full_match_4", "red_propensity", "penalties_missed_38",
    "threat_4", "xg_4", "xg_10", "team_goals_scored_10", "blank_gw"
]
p = DATA_DIR / "features_2025_GK.parquet"
df = pd.read_parquet(p)
df = df.drop(columns=GK_DROP, errors="ignore")
df.to_parquet(p, index=False)
print(f"Updated {p.name}, now {df.shape[1]} cols")


OTHER_DROP1 = ["blank_gw", "saves_4", "saves_10", "penalties_saved_38", 'penalties_missed_38']
# 2) Prune DEF
for pos in ["DEF"]:
    p = DATA_DIR / f"features_2025_{pos}.parquet"
    df = pd.read_parquet(p)
    df = df.drop(columns=OTHER_DROP1, errors="ignore")
    df.to_parquet(p, index=False)
    print(f"Updated {p.name}, now {df.shape[1]} cols")

# 3) Prune MID/FWD
OTHER_DROP = ["blank_gw", "saves_4", "saves_10", "penalties_saved_38"]
for pos in ["MID", "FWD"]:
    p = DATA_DIR / f"features_2025_{pos}.parquet"
    df = pd.read_parquet(p)
    df = df.drop(columns=OTHER_DROP, errors="ignore")
    df.to_parquet(p, index=False)
    print(f"Updated {p.name}, now {df.shape[1]} cols")
