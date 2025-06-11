import pandas as pd
from pathlib import Path

# 1) Clean players_2025.parquet
PARSED = Path("data/processed")
p25 = PARSED / "players_2025.parquet"
df25 = pd.read_parquet(p25)

# <-- put here the exact 2025 columns you want to drop -->
TO_DROP_2025 = ['can_transact', 'can_select', 'chance_of_playing_next_round', 'chance_ofPlaying_this_round', 'code', 'cost_change_event', 'cost_change_event_fall',
                'cost_change_start', 'cost_change_start_fall', 'dreamteam_count', 'ep_next', 'ep_this', 'event_points', 'in_dreamteam', 'news', 'news_added', 'photo',
                'removed', 'special', 'squad_number', 'status', 'transfers_in_event', 'transfers_out_event', 'value_form', 'region', 'team_join_date'
                ,'birth_date', 'has_temporary_code', 'opta_code', 'mng_draw', 'mng_win', 'mng_loss', 'mng_underdog_win','mng_underdog_draw','mng_clean_sheets',
                'mng_goals_scored', 'corners_and_indirect_freekicks_text', 'direct_freekicks_text', 'penalties_text', 'form_rank','form_rank_type', 'chance_of_playing_this_round']

df25_clean = df25.drop(columns=TO_DROP_2025, errors="ignore")
df25_clean.to_parquet(p25, index=False)
print(f"✅ players_2025.parquet cleaned; now has {df25_clean.shape[1]} columns.")

# 2) Ensure players_2023 & players_2024 have the same columns
for season in ("2023","2024"):
    path = PARSED / f"players_{season}.parquet"
    if not path.exists():
        print(f"⚠️  {path.name} not found, skipping.")
        continue

    df = pd.read_parquet(path)
    # add any missing columns from df25_clean, fill with 0
    for col in df25_clean.columns:
        if col not in df.columns:
            df[col] = 0

    # reorder to match 2025 schema (optional but keeps things tidy)
    df = df[df25_clean.columns]

    df.to_parquet(path, index=False)
    print(f"✅ players_{season}.parquet updated; now has {df.shape[1]} columns.")
