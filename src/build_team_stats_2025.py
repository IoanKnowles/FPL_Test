import pandas as pd
from pathlib import Path

def build_team_stats_2025():
    # paths
    FIXTURES = Path("data/processed/fixtures_2025.parquet")
    OUT       = Path("data/processed/team_stats_2025.parquet")

    # 1) load fixtures
    fx = pd.read_parquet(FIXTURES)

    # 2) build home‐side records
    home = fx.rename(columns={
        "home_team_id":  "team_id",
        "home_goals":    "goals_scored",
        "away_goals":    "goals_conceded"
    })
    home["points"] = home.apply(
        lambda r: 3 if r.goals_scored > r.goals_conceded
                  else 1 if r.goals_scored == r.goals_conceded
                  else 0,
        axis=1
    )
    home = home[["team_id","round","points","goals_scored","goals_conceded"]]

    # 3) build away‐side records
    away = fx.rename(columns={
        "away_team_id":  "team_id",
        "away_goals":    "goals_scored",
        "home_goals":    "goals_conceded"
    })
    away["points"] = away.apply(
        lambda r: 3 if r.goals_scored > r.goals_conceded
                  else 1 if r.goals_scored == r.goals_conceded
                  else 0,
        axis=1
    )
    away = away[["team_id","round","points","goals_scored","goals_conceded"]]

    # 4) concatenate and sort
    team_stats = pd.concat([home, away], ignore_index=True)
    team_stats = team_stats.sort_values(["team_id","round"])

    # 5) save
    team_stats.to_parquet(OUT, index=False)
    print(f"Wrote {OUT} with {len(team_stats)} rows")

if __name__ == "__main__":
    build_team_stats_2025()