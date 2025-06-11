import os
import json
import sqlite3
import pandas as pd

# Paths
root = os.path.dirname(__file__)
RAW_DIR = os.path.abspath(os.path.join(root, os.pardir, 'data', 'raw'))
PROC_DIR = os.path.abspath(os.path.join(root, os.pardir, 'data', 'processed'))
DB_DIR = os.path.abspath(os.path.join(root, os.pardir, 'data'))
DB_PATH = os.path.join(DB_DIR, 'fpl.db')

# Ensure processed directory exists
os.makedirs(PROC_DIR, exist_ok=True)

# Helper to skip existing Parquet outputs
def _should_skip_parquet(path, name):
    if os.path.exists(path):
        print(f"{name} already exists, skipping.")
        return True
    return False


def load_bootstrap():
    """Normalize bootstrap-static.json to players, teams, positions."""
    bs_path = os.path.join(RAW_DIR, 'bootstrap-static.json')
    with open(bs_path, 'r', encoding='utf8') as f:
        bs = json.load(f)

    # Players
    players_pq = os.path.join(PROC_DIR, 'players.parquet')
    if not _should_skip_parquet(players_pq, 'players.parquet'):
        players = pd.json_normalize(bs, record_path=['elements'])
        players.to_parquet(players_pq, index=False)
        print("Wrote players.parquet")

    # Teams
    teams_pq = os.path.join(PROC_DIR, 'teams.parquet')
    if not _should_skip_parquet(teams_pq, 'teams.parquet'):
        teams = pd.json_normalize(bs, record_path=['teams'])
        teams.to_parquet(teams_pq, index=False)
        print("Wrote teams.parquet")

    # Positions
    pos_pq = os.path.join(PROC_DIR, 'positions.parquet')
    if not _should_skip_parquet(pos_pq, 'positions.parquet'):
        positions = pd.json_normalize(bs, record_path=['element_types'])
        positions.to_parquet(pos_pq, index=False)
        print("Wrote positions.parquet")


def load_fixtures():
    """Extract matches list from matches.json, flatten nested data, and write fixtures.parquet."""
    fixtures_pq = os.path.join(PROC_DIR, 'fixtures.parquet')
    if _should_skip_parquet(fixtures_pq, 'fixtures.parquet'):
        return
    json_path = os.path.join(RAW_DIR, 'matches.json')
    with open(json_path, 'r', encoding='utf8') as f:
        data = json.load(f)
    matches = data.get('matches', [])
    # Flatten nested dicts
    fixtures_df = pd.json_normalize(matches, sep='_')
    # Drop problematic list/dict columns
    for col in ['referees', 'odds']:
        if col in fixtures_df.columns:
            fixtures_df = fixtures_df.drop(columns=[col])
    fixtures_df.to_parquet(fixtures_pq, index=False)
    print("Wrote fixtures.parquet")


def load_element_histories():
    """Aggregate per-player history JSONs into history.parquet."""
    history_pq = os.path.join(PROC_DIR, 'history.parquet')
    if _should_skip_parquet(history_pq, 'history.parquet'):
        return
    records = []
    for fname in os.listdir(RAW_DIR):
        if fname.startswith('element-') and fname.endswith('-history.json'):
            with open(os.path.join(RAW_DIR, fname), 'r', encoding='utf8') as f:
                data = json.load(f)
            history = data.get('history', [])
            element_id = history[0].get('element') if history else None
            for rec in history:
                rec['element'] = element_id
                records.append(rec)
    if records:
        hist_df = pd.DataFrame(records)
        hist_df.to_parquet(history_pq, index=False)
        print("Wrote history.parquet")
    else:
        print("No element history found, skipping history.parquet")


def load_sqlite():
    """Load all Parquet tables into an SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    tables = ['players_2025','players_2024', 'players_2023', 'teams', 'positions', 'fixtures', 'history_2025', 'history_2024','history_2023', 'features']
    for tbl in tables:
        pq_path = os.path.join(PROC_DIR, f"{tbl}.parquet")
        if os.path.exists(pq_path):
            df = pd.read_parquet(pq_path)
            # Ensure no nested lists/dicts remain
            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                    df = df.drop(columns=[col])
            df.to_sql(tbl, conn, if_exists='replace', index=False)
            print(f"Loaded {tbl}.parquet into SQLite table '{tbl}'")
        else:
            print(f"{tbl}.parquet not found, skipping")
    conn.close()
    print(f"SQLite database created at {DB_PATH}")


if __name__ == '__main__':
    #load_bootstrap()
    #load_fixtures()
    #load_element_histories()
    load_sqlite()