# src/aggregate_history_to_players.py

import pandas as pd
from pathlib import Path

# seasons you’ve got history_<season>.parquet for
SEASONS = ["2023", "2024", "2025"]

# the history columns you want to sum per player
HIST_COLS = [
    "minutes",
    "goals_scored",
    "assists",
    "goals_conceded",
    "clean_sheets",
    "own_goals",
    "penalties_missed",
    "penalties_saved",
    "yellow_cards",
    "red_cards",
    "saves",
    "bonus",
    "bps",
    "influence",
    "creativity",
    "threat",
    "ict_index",
    "transfers_in",
    "transfers_out",
    "total_points",
    "starts",
]

DATA_DIR = Path("data/processed")

for season in SEASONS:
    # --- 1) aggregate history_<season>.parquet ---
    hist_path = DATA_DIR / f"history_{season}.parquet"
    if not hist_path.exists():
        print(f"⚠️  {hist_path.name} not found, skipping season {season}.")
        continue

    df_hist = pd.read_parquet(hist_path)
    # ensure all needed columns are present
    for c in HIST_COLS:
        if c not in df_hist.columns:
            df_hist[c] = 0
    # group & sum
    agg = (
        df_hist
        .groupby("element")[HIST_COLS]
        .sum()
        .add_prefix("sum_")   # sums will be sum_minutes, sum_goals_scored, …
        .reset_index()
    )
    print(f"  • season {season}: aggregated history shape → {agg.shape}")

    # --- 2) merge into players_<season>.parquet ---
    players_path = DATA_DIR / f"players_{season}.parquet"
    if not players_path.exists():
        print(f"⚠️  {players_path.name} not found, skipping season {season}.")
        continue

    df_players = pd.read_parquet(players_path)
    # merge on element
    merged = df_players.merge(agg, on="element", how="left")
    # fill any NaNs in the new sum_ columns with 0
    sum_cols = [col for col in merged.columns if col.startswith("sum_")]
    merged[sum_cols] = merged[sum_cols].fillna(0)

    # overwrite parquet
    merged.to_parquet(players_path, index=False)
    print(f"✅ players_{season}.parquet updated; new shape {merged.shape}\n")
