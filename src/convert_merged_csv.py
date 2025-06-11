# convert_merged_csv.py

import pandas as pd
from pathlib import Path

def main():
    raw_dir = Path("data/raw")
    proc_dir = Path("data/processed")
    proc_dir.mkdir(parents=True, exist_ok=True)

    csv_path = raw_dir / "cleaned_merged_seasons_team_aggregated.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Could not find {csv_path}. Place the CSV under data/raw/")

    # 1) Read the CSV
    df = pd.read_csv(csv_path)

    # 2) Rename season_x → season, mapping "2022-23"→"2023" and "2023-24"→"2024" and rename points column
    df = df.rename(columns={"season_x": "season_raw"})
    df["season"] = df["season_raw"].map({
        "2022-23": "2023",
        "2023-24": "2024"
    })

    #df = df.rename(columns={"points": "total_points"})

    if df["season"].isna().any():
        missing = df[df["season"].isna()]["season_raw"].unique().tolist()
        raise KeyError(f"Found unexpected season_raw values: {missing}")

    # 3) List out unique seasons in the CSV (should be ["2023","2024"])
    seasons = sorted(df["season"].unique())

    for season in seasons:
        df_se = df[df["season"] == season].copy()

        # --- A) Build history_<season>.parquet ---
        #
        # We want one row per (element, round) with all GW stats. From your CSV columns:
        #   element, round, minutes, goals_scored, assists, goals_conceded, clean_sheets,
        #   own_goals, penalties_missed, penalties_saved, yellow_cards, red_cards, saves,
        #   bonus, bps, influence, creativity, threat, ict_index,
        #   transfers_balance, transfers_in, transfers_out, value, total_points, was_home,
        #   team_h_score, team_a_score
        #
        history_cols = [
            "element",
            "round",
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
            "transfers_balance",
            "transfers_in",
            "transfers_out",
            "value",
            "total_points",
            "was_home",
            "team_h_score",
            "team_a_score",
            "kickoff_time",
            'selected',
            'opponent_team'
        ]
        # Keep only those that actually exist
        #history_cols_pres = [c for c in history_cols if c in df_se.columns]

        # Make sure 'element' and 'round' exist
        #if "element" not in history_cols_pres or "round" not in history_cols_pres:
         #   raise KeyError(f"CSV for season {season} is missing 'element' or 'round' columns.")

        #hist_df = df_se[["element", "round"] + history_cols_pres[2:]].copy()
        #hist_df["season"] = season

        #out_hist = proc_dir / f"history_{season}.parquet"
        #hist_df.to_parquet(out_hist, index=False)
        #print(f"Wrote {out_hist} (shape {hist_df.shape})")

        # --- B) Build players_<season>.parquet ---
        #
        # We want one row per element (player) containing only static info:
        #   element, name, position, team_x, value
        player_cols = ["element", "name", "position", "team_x", "value"]
        player_cols_pres = [c for c in player_cols if c in df_se.columns]

        # Deduplicate by element
        players_df = df_se[player_cols_pres].drop_duplicates(subset=["element"]).copy()
        players_df["season"] = season

        out_players = proc_dir / f"players_{season}.parquet"
        players_df.to_parquet(out_players, index=False)
        print(f"Wrote {out_players} (shape {players_df.shape})")

    print("✅ Conversion complete. Now you have:")
    for season in seasons:
        print(f"   • data/processed/players_{season}.parquet")
        print(f"   • data/processed/history_{season}.parquet")


if __name__ == "__main__":
    main()
