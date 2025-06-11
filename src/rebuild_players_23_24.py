# src/rebuild_players_23_24.py

import pandas as pd
from pathlib import Path

# 1) Paths
RAW_CSV    = Path("data/raw/cleaned_merged_seasons_team_aggregated.csv")
PROC_DIR   = Path("data/processed")
PLAY25_PQ  = PROC_DIR / "players_2025.parquet"

# 2) Load the 2025 master schema so we know which columns to end up with
players25 = pd.read_parquet(PLAY25_PQ)
full_cols = players25.columns.tolist()

# 3) Read the cleaned CSV
df = pd.read_csv(RAW_CSV)

# 4) Map season strings to numeric suffixes
df = df.rename(columns={"season_x": "season_raw"})
df["season"] = df["season_raw"].map({"2022-23":"2023","2023-24":"2024"})
assert not df["season"].isna().any()

# 5) Static fields we want from the CSV
STATIC = ["element","name","team_x","position","value","selected"]

for season in ["2023","2024"]:
    # 5A) Build minimal players_<season>.parquet
    df_se = df[df["season"] == season]
    # drop duplicates to get one row per player
    players = df_se[STATIC].drop_duplicates(subset=["element"]).copy()
    players["season"] = season
    rename_map = {
                    "value":                "now_cost",
                    "selected":             "selected_by_percent",
                    "team_x":               "team",
                    "name":                 "first_name"
                }
    players = players.rename(columns=rename_map)

    # 5B) Aggregate history sums and merge in
    hist = pd.read_parquet(PROC_DIR / f"history_{season}.parquet")
    # ensure starts exists
    if "starts" not in hist.columns:
        hist["starts"] = (hist["minutes"] > 45).astype(int)
    # sum the per-GW fields
    to_sum = [
      "minutes","goals_scored","assists","goals_conceded","clean_sheets",
      "own_goals","penalties_missed","penalties_saved","yellow_cards",
      "red_cards","saves","bonus","bps","influence","creativity",
      "threat","ict_index","transfers_in","transfers_out",
      "total_points","starts"
    ]
    for c in to_sum:
        if c not in hist.columns:
            hist[c] = 0
    agg = hist.groupby("element")[to_sum].sum().add_prefix("sum_").reset_index()

    # merge sums onto minimal players
    merged = players.merge(agg, on="element", how="left")
    # fill any missing sums with zero
    sum_cols = [c for c in merged.columns if c.startswith("sum_")]
    merged[sum_cols] = merged[sum_cols].fillna(0)
    

    pos_map = {"GK":1,"GKP":1, "DEF":2, "MID":3, "FWD":4}
    merged["element_type"] = merged["position"].map(pos_map).fillna(0).astype(int)
    merged = merged.rename(columns={"element": "id"})
    final_cols = full_cols + sum_cols
    merged = merged.reindex(columns=final_cols, fill_value=0)
    # 5C) Add any missing columns from 2025 schema
    #for col in full_cols:
        #if col not in merged.columns:
            #merged[col] = 0  

    # 5E) Write out
    out_pq = PROC_DIR / f"players_{season}.parquet"
    merged.to_parquet(out_pq, index=False)
    print(f"Wrote players_{season}.parquet (shape {merged.shape})")