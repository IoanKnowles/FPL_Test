# src/build_features.py

import numpy as np
import pandas as pd
import sqlite3
from pathlib import Path

DATA_DIR = Path("data/processed")
DB_PATH  = Path("data/fpl.db")
SEASONS  = ["2025"]   # only doing 2025

def build_features_for(season: str):
    # 1) load raw data
    hist    = pd.read_parquet(DATA_DIR / f"history_{season}.parquet")
    players = pd.read_parquet(DATA_DIR / f"players_{season}.parquet")

    # standardize opponent column
    hist = hist.rename(columns={"opponent_team":"opp_team_id"})
    players = players.rename(columns={"id": "element"})

    # 2) merge in player‐static + name/price/ownership/position
    df = (hist.merge(players[["element","team","first_name","second_name","now_cost","selected_by_percent","element_type"]],on="element", how="left").rename(columns={
            "team":                  "team_id",
            "now_cost":              "price",
            "selected_by_percent":   "ownership",
            "total_points":          "gw_points"
        })
    )
    df["ownership"] = pd.to_numeric(df["ownership"], errors="coerce").fillna(0.0)
    if season=="2025":
        df["name"] = df["first_name"] + " " + df["second_name"]

    # 3) pull in team_stats_{season} from SQLite
    with sqlite3.connect(DB_PATH) as conn:
        ts = pd.read_sql(f"SELECT * FROM team_stats_{season}", conn)
    # add a clean_sheet_match flag
    ts["clean_sheet_match"] = (ts["goals_conceded"]==0).astype(int)

    # merge team match‐level stats
    df = (
        df
        .merge(
            ts.rename(columns={
                "points":             "team_points",
                "goals_scored":       "team_goals_scored_match",
                "goals_conceded":     "team_goals_conceded_match",
                "clean_sheet_match":  "team_clean_sheet_match"
            }),
            on=["team_id","round"], how="left"
        )
        .merge(
            ts.rename(columns={
                "team_id":            "opp_team_id",
                "points":             "opp_points",
                "goals_scored":       "opp_goals_scored_match",
                "goals_conceded":     "opp_goals_conceded_match",
                "clean_sheet_match":  "opp_clean_sheet_match"
            }),
            on=["opp_team_id","round"], how="left"
        )
    )
    # fill any NaNs (e.g. blanks) with zero
    df[[
        "team_points","team_goals_scored_match","team_goals_conceded_match",
        "team_clean_sheet_match","opp_points","opp_goals_scored_match",
        "opp_goals_conceded_match","opp_clean_sheet_match"
    ]] = df[[
        "team_points","team_goals_scored_match","team_goals_conceded_match",
        "team_clean_sheet_match","opp_points","opp_goals_scored_match",
        "opp_goals_conceded_match","opp_clean_sheet_match"
    ]].fillna(0)

    # 4) basic start/full flags
    df["start_flag"]      = (df["minutes"] > 45).astype(int)
    df["full_match_flag"] = (df["minutes"] == 90).astype(int)

    # 5) rolling features by element, all shifted so only prior data used
    gb = df.groupby("element", group_keys=False)

    # 4-GW rolling
    df["starts_4"]            = gb["start_flag"].transform(lambda s: s.shift(1).rolling(window=4,min_periods=1).mean())
    df["full_match_4"]        = gb["full_match_flag"].transform(lambda s: s.shift(1).rolling(window=4,min_periods=1).mean())
    df["points_4"]            = gb["gw_points"].transform(lambda s: s.shift(1).rolling(window=4,min_periods=1).mean())
    df["minutes_4"]           = gb["minutes"].transform(lambda s: s.shift(1).rolling(window=4,min_periods=1).mean())
    df["clean_sheets_4"]      = gb["clean_sheets"].transform(lambda s: s.shift(1).rolling(window=4,min_periods=1).mean())
    df["saves_4"]             = gb["saves"].transform(lambda s: s.shift(1).rolling(window=4,min_periods=1).mean())
    df["influence_4"]         = gb["influence"].transform(lambda s: s.shift(1).rolling(window=4,min_periods=1).mean())
    df["creativity_4"]        = gb["creativity"].transform(lambda s: s.shift(1).rolling(window=4,min_periods=1).mean())
    df["threat_4"]            = gb["threat"].transform(lambda s: s.shift(1).rolling(window=4,min_periods=1).mean())
    df["ict_index_4"]         = gb["ict_index"].transform(lambda s: s.shift(1).rolling(window=4,min_periods=1).mean())
    # opponent clean sheets
    df["opp_clean_sheets_4"]  = gb["opp_clean_sheet_match"].transform(lambda s: s.shift(1).rolling(window=4,min_periods=1).mean())

    # 10-GW rolling
    df["points_10"]           = gb["gw_points"].transform(lambda s: s.shift(1).rolling(window=10,min_periods=1).mean())
    df["minutes_10"]          = gb["minutes"].transform(lambda s: s.shift(1).rolling(window=10,min_periods=1).mean())
    df["clean_sheets_10"]     = gb["clean_sheets"].transform(lambda s: s.shift(1).rolling(window=10,min_periods=1).mean())
    df["saves_10"]            = gb["saves"].transform(lambda s: s.shift(1).rolling(window=10,min_periods=1).mean())
    df["opp_clean_sheets_10"] = gb["opp_clean_sheet_match"].transform(lambda s: s.shift(1).rolling(window=10,min_periods=1).mean())

    # full-season rolling (38)
    df["penalties_saved_38"]   = gb["penalties_saved"].transform(lambda s: s.shift(1).cumsum().fillna(0))
    df["penalties_missed_38"]  = gb["penalties_missed"].transform(lambda s: s.shift(1).cumsum().fillna(0))

    # 6) team / opponent rolling form
    df["team_form_4"]   = gb["team_points"].transform(lambda s: s.shift(1).rolling(window=4,min_periods=1).mean())
    df["team_form_10"]  = gb["team_points"].transform(lambda s: s.shift(1).rolling(window=10,min_periods=1).mean())
    df["team_form_38"]  = gb["team_points"].transform(lambda s: s.shift(1).rolling(window=10,min_periods=1).mean())

    df["opp_form_4"]    = gb["opp_points"].transform(lambda s: s.shift(1).rolling(window=4,min_periods=1).mean())
    df["opp_form_10"]   = gb["opp_points"].transform(lambda s: s.shift(1).rolling(window=10,min_periods=1).mean())
    df["opp_form_38"]   = gb["opp_points"].transform(lambda s: s.shift(1).rolling(window=38,min_periods=1).mean())

    # 7) goals scored/conceded rolling
    df["team_goals_scored_10"]   = gb["team_goals_scored_match"].transform(lambda s: s.shift(1).rolling(window=10,min_periods=1).mean())
    df["team_goals_conceded_10"] = gb["team_goals_conceded_match"].transform(lambda s: s.shift(1).rolling(window=10,min_periods=1).mean())
    df["opp_team_goals_scored_10"]   = gb["opp_goals_scored_match"].transform(lambda s: s.shift(1).rolling(window=10,min_periods=1).mean())
    df["opp_team_goals_conceded_10"] = gb["opp_goals_conceded_match"].transform(lambda s: s.shift(1).rolling(window=10,min_periods=1).mean())

    # 8) xG / xA (if you've merged them into df as 'xG','xA','xGChain','xGBuildup')
    
    df["xg_4"]             = gb["expected_goals"].transform(lambda s: s.shift(1).rolling(window=4,min_periods=1).mean())
    df["xg_10"]            = gb["expected_goals"].transform(lambda s: s.shift(1).rolling(window=10,min_periods=1).mean())
    df["xa_4"]             = gb["expected_assists"].transform(lambda s: s.shift(1).rolling(window=4,min_periods=1).mean())
    df["xa_10"]            = gb["expected_assists"].transform(lambda s: s.shift(1).rolling(window=10,min_periods=1).mean())
    df["xg_involvements_4"] = gb["expected_goal_involvements"].transform(lambda s: s.shift(1).rolling(window=4,min_periods=1).mean())
    df["xg_involvements_10"]= gb["expected_goal_involvements"].transform(lambda s: s.shift(1).rolling(window=10,min_periods=1).mean())
    df["xg_conceded_4"]     = gb["expected_goals_conceded"].transform(lambda s: s.shift(1).rolling(window=4,min_periods=1).mean())
    df["xg_conceded_10"]    = gb["expected_goals_conceded"].transform(lambda s: s.shift(1).rolling(window=10,min_periods=1).mean())
    

    # 9) transfers pct
    df["transfers_in_pct"]   = gb["transfers_in"].transform(lambda s: s.shift(1).fillna(0)) \
                                / (df["ownership"].shift(1).fillna(0) + 1e-6)
    df["transfers_out_pct"]  = gb["transfers_out"].transform(lambda s: s.shift(1).fillna(0)) \
                                / (df["ownership"].shift(1).fillna(0) + 1e-6)
    # 10) double/blank GW
    cnt = hist.groupby(["element","round"]).size().rename("cnt").reset_index()
    df = df.merge(cnt, on=["element","round"], how="left")

    df["double_gw"] = (df["cnt"]>1).astype(int)
    df["blank_gw"]  = (df["cnt"]==0).astype(int)

    df.drop(columns="cnt", inplace=True)
    
    df["is_home"]    = df["was_home"].astype(int)

    df["full_match_4"]  = gb["full_match_flag"].transform(lambda s: s.shift(1).rolling(window=4,min_periods=1).mean())

    df["cum_minutes_prev"]     = gb["minutes"].transform(lambda s: s.shift(1).cumsum().fillna(0))
    df["cum_points_prev"]      = gb["gw_points"].transform(lambda s: s.shift(1).cumsum().fillna(0))
    df["ppm"]                  = df["cum_points_prev"] / (df["cum_minutes_prev"] + 1e-6)
    # cumulative sums (including current GW)
    df["cum_yellow"]  = gb["yellow_cards"].cumsum()
    df["cum_red"]     = gb["red_cards"].cumsum()
    df["cum_minutes"] = gb["minutes"].cumsum()
    # shift by one so we only use _previous_ GWs
    df["yellow_propensity"]    = gb["yellow_cards"].transform(lambda s: s.shift(1).cumsum()) \
                                 / (df["cum_minutes_prev"] + 1e-6)
    df["red_propensity"]       = gb["red_cards"].transform(lambda s: s.shift(1).cumsum()) \
                                 / (df["cum_minutes_prev"] + 1e-6)
    df[["yellow_propensity","red_propensity"]] = df[["yellow_propensity","red_propensity"]].fillna(0)

    # 11) assemble final feature list
    feature_cols = [
        "element","round","element_type","name","price","ownership","gw_points",
        "starts_4","full_match_4","ppm",
        "points_4","points_10","minutes_4","minutes_10",
        "clean_sheets_4","clean_sheets_10",
        "saves_4","saves_10",
        "penalties_saved_38","penalties_missed_38",
        "yellow_propensity","red_propensity",
        "influence_4","creativity_4","threat_4","ict_index_4",
        "xg_4","xg_10","xa_4","xa_10",
        "xg_involvements_4","xg_involvements_10",
        "xg_conceded_4","xg_conceded_10",
        "team_form_4","team_form_10","team_form_38",
        "opp_form_4","opp_form_10","opp_form_38",
        "team_goals_scored_10","team_goals_conceded_10",
        "opp_clean_sheets_4","opp_clean_sheets_10",
        "opp_team_goals_scored_10","opp_team_goals_conceded_10",
        "transfers_in_pct","transfers_out_pct",
        "double_gw","blank_gw", "is_home"
    ]

    df.fillna(0, inplace=True)

    out = DATA_DIR / f"features_{season}.parquet"
    df[feature_cols].to_parquet(out, index=False)
    print(f"Wrote {out} → shape {df[feature_cols].shape}")


if __name__ == "__main__":
    for s in SEASONS:
        build_features_for(s)
