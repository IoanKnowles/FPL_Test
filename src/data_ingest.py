import os
import json
import requests
from understatapi import UnderstatClient
from statsbombpy import sb

# Base folders
RAW_DIR = os.path.join(os.path.dirname(__file__), os.pardir, 'data', 'raw')
os.makedirs(RAW_DIR, exist_ok=True)

# Helper to skip existing files
def _should_skip(path, name):
    if os.path.exists(path):
        print(f"{name} already exists, skipping.")
        return True
    return False

def fetch_fpl_bootstrap():
    out_path = os.path.join(RAW_DIR, 'bootstrap-static.json')
    if _should_skip(out_path, 'bootstrap-static.json'):
        return
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    with open(out_path, 'w', encoding='utf8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("Saved bootstrap-static.json")

def fetch_fpl_element_history(player_id):
    fname = f'element-{player_id}-history.json'
    out_path = os.path.join(RAW_DIR, fname)
    if _should_skip(out_path, fname):
        return
    url = f'https://fantasy.premierleague.com/api/element-summary/{player_id}/'
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    with open(out_path, 'w', encoding='utf8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved {fname}")

HEADERS = {'X-Auth-Token': os.getenv('FOOTBALL_DATA_API_KEY', 'YOUR_API_KEY_HERE')}

def fetch_fixtures():
    out_path = os.path.join(RAW_DIR, 'matches.json')
    if _should_skip(out_path, 'matches.json'):
        return
    url = 'https://api.football-data.org/v4/competitions/PL/matches'
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    with open(out_path, 'w', encoding='utf8') as f:
        json.dump(resp.json(), f, indent=2)
    print("Saved matches.json")

def fetch_understat_xg():
    out_path = os.path.join(RAW_DIR, 'understat-players-2025.json')
    if _should_skip(out_path, 'understat-players-2025.json'):
        return
    with UnderstatClient() as client:
        data = client.league(league="EPL").get_player_data(season="2025")
    with open(out_path, 'w', encoding='utf8') as f:
        json.dump(data, f, indent=2)
    print("Saved understat players data")

def fetch_statsbomb_events():
    out_path = os.path.join(RAW_DIR, 'statsbomb-events-2025.json')
    if _should_skip(out_path, 'statsbomb-events-2025.json'):
        return
    comps = sb.competitions()
    pl_comps = comps[comps.competition_name == 'Premier League']
    if not any(pl_comps.season_name == '2025'):
        print("No StatsBomb open data for 2025 PL; skipping.")
        return
    pl_row = pl_comps[pl_comps.season_name == '2025'].iloc[0]
    events = sb.events(competition_id=pl_row.competition_id,
                       season_id=pl_row.season_id)
    events.to_json(out_path, orient='records')
    print("Saved statsbomb-events-2025.json")

if __name__ == '__main__':
    bs_path = os.path.join(RAW_DIR, 'bootstrap-static.json')
    with open(bs_path, 'r', encoding='utf8') as f:
        bs = json.load(f)

    element_ids = [e['id'] for e in bs['elements']]
    for pid in element_ids:
        fetch_fpl_element_history(pid)

    fetch_fixtures()
    fetch_understat_xg()
    fetch_statsbomb_events()
    fetch_fpl_bootstrap()