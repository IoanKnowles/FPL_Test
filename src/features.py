import os
import json
import pandas as pd
from pathlib import Path

def build_features():
    # ---------------------------------------------------------------------------------
    # 1. Define paths
    # ---------------------------------------------------------------------------------
    project_root = Path(__file__).parent.parent
    raw_dir       = project_root / 'data' / 'raw'
    proc_dir      = project_root / 'data' / 'processed'
    out_fp        = proc_dir / 'features.parquet'

    # Make sure processed dir exists
    os.makedirs(proc_dir, exist_ok=True)

    # ---------------------------------------------------------------------------------
    # 2. Load processed tables: history, players, teams
    # ---------------------------------------------------------------------------------
    history = pd.read_parquet(proc_dir / 'history.parquet')
    players = pd.read_parquet(proc_dir / 'players.parquet')
    teams   = pd.read_parquet(proc_dir / 'teams.parquet')

    # ---------------------------------------------------------------------------------
    # 3. Merge in player metadata (id → team, now_cost, web_name)
    # ---------------------------------------------------------------------------------
    meta = (
        players[['id', 'team', 'now_cost', 'web_name']]
        .rename(columns={'id': 'element'})
    )
    df = history.merge(meta, on='element', how='left')

    # ---------------------------------------------------------------------------------
    # 4. Rolling‐window “form” for the last 4 GWs (points, minutes)
    # ---------------------------------------------------------------------------------
    df = df.sort_values(['element', 'round'])
    df['form_pts4'] = (
        df.groupby('element')['total_points']
          .transform(lambda x: x.rolling(4, min_periods=1).mean())
    )
    df['form_min4'] = (
        df.groupby('element')['minutes']
          .transform(lambda x: x.rolling(4, min_periods=1).mean())
    )

    # ---------------------------------------------------------------------------------
    # 5. fixture_diff: map opponent_team → strength_ratio
    # ---------------------------------------------------------------------------------
    # Build diff_map from teams.strength (scaled 0–1)
    if 'strength' in teams.columns:
        max_strength = teams['strength'].max()
        diff_map = (teams.set_index('id')['strength'] / max_strength).to_dict()
    else:
        # Fallback if “strength” isn’t available
        diff_map = {tid: 1.0 for tid in teams['id']}

    # “opponent_team” is the FPL ID of the opponent in that GW
    df['fixture_diff'] = df['opponent_team'].map(diff_map)

    # ---------------------------------------------------------------------------------
    # 6. is_home flag (0/1) comes directly from history.was_home
    # ---------------------------------------------------------------------------------
    df['is_home'] = df['was_home'].astype(int)

    # ---------------------------------------------------------------------------------
    # 7. Double/Blank GW flags: count how many fixtures each team has in each round
    # ---------------------------------------------------------------------------------
    # 7a. Read matches.json and flatten with json_normalize
    with open(raw_dir / 'matches.json', 'r', encoding='utf8') as f:
        raw_matches = json.load(f)

    matches = pd.json_normalize(raw_matches['matches'], sep='_').rename(columns={'id': 'match_id'})
    # Ensure types for merging
    matches['matchday']    = matches['matchday'].astype(int)
    matches['homeTeam_id'] = matches['homeTeam_id'].astype(int)
    matches['awayTeam_id'] = matches['awayTeam_id'].astype(int)

    # 7b. Build a small table listing (matchday, team_id) for every fixture
    home_entries = matches[['matchday', 'homeTeam_id']].rename(columns={'homeTeam_id': 'team_id'})
    away_entries = matches[['matchday', 'awayTeam_id']].rename(columns={'awayTeam_id': 'team_id'})

    gw_counts = (
        pd.concat([home_entries, away_entries], ignore_index=True)
          .groupby(['matchday', 'team_id'])
          .size()
          .reset_index(name='gw_matches')
    )

    # 7c. Merge “gw_counts” onto df using (round, team)
    df = df.merge(
        gw_counts,
        left_on=['round', 'team'],
        right_on=['matchday', 'team_id'],
        how='left'
    )
    df['gw_matches'] = df['gw_matches'].fillna(0).astype(int)
    df['double_gw']  = (df['gw_matches'] > 1).astype(int)
    df['blank_gw']   = (df['gw_matches'] == 0).astype(int)

    # Clean up the temporary merge columns
    df.drop(columns=['matchday', 'team_id', 'gw_matches'], inplace=True)

    # ---------------------------------------------------------------------------------
    # 8. Price and minutes indicators
    # ---------------------------------------------------------------------------------
    # price_m = now_cost / 10 (because FPL stores price as integer ×10)
    df['price_m']   = df['now_cost'] / 10
    df['started']   = (df['minutes'] > 0).astype(int)
    df['full_match'] = (df['minutes'] == 90).astype(int)

    # ---------------------------------------------------------------------------------
    # 9. Understat’s xG / xA features (if the JSON exists)
    # ---------------------------------------------------------------------------------
    try:
        with open(raw_dir / 'understat-players-2025.json', 'r', encoding='utf8') as f:
            us_list = json.load(f)
        us = pd.json_normalize(us_list)
        # Understat fields include “player_name”, “xG”, “xA” (among others)
        us = us[['player_name', 'xG', 'xA']]
        df = df.merge(us, left_on='web_name', right_on='player_name', how='left')
    except Exception:
        # If Understat data is missing or can’t be parsed, just set xG/xA = NA
        df['xG'] = pd.NA
        df['xA'] = pd.NA

    # ---------------------------------------------------------------------------------
    # 10. Select exactly the 14 feature columns (in this order) and write to disk
    # ---------------------------------------------------------------------------------
    feature_cols = [
        'element',
        'round',
        'total_points',
        'price_m',
        'started',
        'full_match',
        'form_pts4',
        'form_min4',
        'fixture_diff',
        'is_home',
        'double_gw',
        'blank_gw',
        'xG',
        'xA'
    ]

    features = df[feature_cols]
    features.to_parquet(out_fp, index=False)
    print(f"Wrote features.parquet with shape {features.shape}")

if __name__ == '__main__':
    build_features()