import pandas as pd
from pathlib import Path

def build_fixtures_2025():
    RAW  = Path("data/raw/matches_2025.csv")
    OUT  = Path("data/processed/fixtures_2025.parquet")

    # 1) load
    df = pd.read_csv(RAW)

    # 2) drop unneeded cols
    df = df.drop(columns=["Match Number", "Location", "Date"])

    # 3) rename to canonical names
    df = df.rename(columns={
        "Round Number": "round",
        "Home Team":    "home_team",
        "Away Team":    "away_team",
        "Result":       "score"
    })

    # 4) parse score into two integer columns
    #    expect format "X - Y"
    score_split = df["score"].str.split(" - ", expand=True)
    df["home_goals"] = score_split[0].astype(int)
    df["away_goals"] = score_split[1].astype(int)

    # 5) map team names → team_id (fill in your own mapping)
    team_map = {
        "Arsenal":        1,
        "Aston Villa":    2,
        "Bournemouth":    3,
        "Brentford":      4,
        "Brighton":       5,
        "Chelsea":        6,
        "Crystal Palace": 7,
        "Everton":        8,
        "Fulham":         9,
        "Ipswich":        10,
        "Leicester":      11,
        "Liverpool":      12,
        "Man City":       13,
        "Man Utd":        14,
        "Newcastle":      15,
        "Nott'm Forest":  16,
        "Southampton":    17,
        "Spurs":          18,
        "West Ham":       19,
        "Wolves" :        20}
    df["home_team_id"] = df["home_team"].map(team_map)
    df["away_team_id"] = df["away_team"].map(team_map)

    # 6) drop the old name + score columns
    df = df.drop(columns=["home_team", "away_team", "score"])

    # 7) reorder columns
    df = df[["round", "home_team_id", "away_team_id", "home_goals", "away_goals"]]

    # 8) write out
    df.to_parquet(OUT, index=False)
    print(f"Wrote {OUT} ({len(df)} rows)")

if __name__ == "__main__":
    build_fixtures_2025()
