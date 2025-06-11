import pandas as pd
from pathlib import Path

PROC = Path("data/processed")
FULL_COLS = pd.read_parquet(PROC/"players_2025.parquet").columns.tolist()



# Every other FULL_COLS entry should be dropped from 2023/24 before we re-unify schemas
to_drop = [
      "minutes","goals_scored","assists","goals_conceded","clean_sheets",
      "own_goals","penalties_missed","penalties_saved","yellow_cards",
      "red_cards","saves","bonus","bps","influence","creativity",
      "threat","ict_index","transfers_in","transfers_out",
      "total_points","starts"
    ]

rename_map = {"sum_minutes": "minutes",
              "sum_goals_scored": "goals_scored",
              "sum_assists": "assists",
              "sum_goals_conceded": "goals_conceded",
              "sum_clean_sheets": "clean_sheets",
              "sum_own_goals": "own_goals",
              "sum_penalties_missed": "penalties_missed",
              "sum_penalties_saved": "penalties_saved",
              "sum_yellow_cards": "yellow_cards",
              "sum_red_cards": "red_cards",
              "sum_saves": "saves",
              "sum_bonus": "bonus",
              "sum_bps": "bps",
              "sum_influence": "influence",
              "sum_creativity": "creativity",
              "sum_threat": "threat",
              "sum_ict_index": "ict_index",
              "sum_transfers_in": "transfers_in",
              "sum_transfers_out": "transfers_out",
              "sum_total_points": "total_points",
              "sum_starts": "starts"}

for season in ("2023","2024"):
    p = PROC / f"players_{season}.parquet"
    df = pd.read_parquet(p)
    #df = df.drop(columns=to_drop, errors="ignore")
    df = df.rename(columns=rename_map)
    df.to_parquet(p, index=False)

    #print(f"Dropped {len(to_drop)} zero‐filled columns from players_{season}.parquet → now {df.shape[1]} cols")
    print(f"renamed files in season {season}")
