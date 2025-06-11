import pandas as pd
from pathlib import Path

# 1. Define the seasons you want to process
seasons = ["2023", "2024"]

for season in seasons:
    path = Path(f"data/processed/history_{season}.parquet")
    if not path.exists():
        print(f"❌ {path} not found, skipping.")
        continue

    # 2. Load the existing history
    df = pd.read_parquet(path)

    # 3. Create 'starts': 1 if minutes > 45, else 0
    df["starts"] = (df["minutes"] > 45).astype(int)

    # 4. Overwrite the Parquet with the new column added
    df.to_parquet(path, index=False)
    print(f"✅ history_{season}.parquet updated (added 'starts', new shape={df.shape})")