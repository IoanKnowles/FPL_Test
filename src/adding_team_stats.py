import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from contextlib import closing

DB_PATH    = Path("data/fpl.db")
FIXTURES   = Path("data/processed/fixtures_2025.parquet")
TEAM_STATS = Path("data/processed/team_stats_2025.parquet")

def build_team_stats_df():
    fx = pd.read_parquet(FIXTURES)

    # home side
    home = fx.rename(columns={
        "home_team_id": "team_id",
        "home_goals":   "goals_scored",
        "away_goals":   "goals_conceded"
    })
    home["points"] = home.apply(
        lambda r: 3 if r.goals_scored > r.goals_conceded
                  else 1 if r.goals_scored == r.goals_conceded
                  else 0,
        axis=1
    )
    home = home[["team_id","round","points","goals_scored","goals_conceded"]]

    # away side
    away = fx.rename(columns={
        "away_team_id": "team_id",
        "away_goals":   "goals_scored",
        "home_goals":   "goals_conceded"
    })
    away["points"] = away.apply(
        lambda r: 3 if r.goals_scored > r.goals_conceded
                  else 1 if r.goals_scored == r.goals_conceded
                  else 0,
        axis=1
    )
    away = away[["team_id","round","points","goals_scored","goals_conceded"]]

    df = pd.concat([home, away], ignore_index=True)
    return df.sort_values(["team_id","round"])

def main():
    # 1) build DataFrame
    df = build_team_stats_df()

    # 2) write out the parquet for quick reload later
    df.to_parquet(TEAM_STATS, index=False)
    print(f"Wrote {TEAM_STATS} ({len(df)} rows)")

    # 3) create engine with WAL & timeout
    engine = create_engine(
        f"sqlite:///{DB_PATH!s}",
        connect_args={
            "timeout": 30,
            "check_same_thread": False
        }
    )

    # 4) enable WAL & sane sync
    with closing(engine.connect()) as conn:
        conn.execute(text("PRAGMA journal_mode=WAL;"))
        conn.execute(text("PRAGMA synchronous=NORMAL;"))

    # 5) load into SQLite
    with closing(engine.connect()) as conn:
        df.to_sql(
            "team_stats_2025",
            conn,
            if_exists="replace",
            index=False
        )
        print(f"✅ Loaded {len(df)} rows into table 'team_stats_2025'")

    engine.dispose()

if __name__ == "__main__":
    main()