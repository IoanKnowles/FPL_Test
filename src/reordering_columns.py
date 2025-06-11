import pandas as pd
from pathlib import Path

# 1) Load your “reference” table (the one whose column order you want to copy)
ref = pd.read_parquet(Path("data/processed/players_2025.parquet"))
ref_cols = ref.columns.tolist()

# 2) Load the table you’d like to reorder
df24 = pd.read_parquet(Path("data/processed/players_2024.parquet"))
df23 = pd.read_parquet(Path("data/processed/players_2023.parquet"))

# 3) Re‐index its columns to match the reference
#    Any columns in ref_cols but missing in df will be created filled with NaN.
#    Any columns in df but *not* in ref_cols will be dropped.
df24 = df24.reindex(columns=ref_cols)
df23 = df23.reindex(columns=ref_cols)

# 4) Save it back
df24.to_parquet(Path("data/processed/players_2024.parquet"), index=False)
df23.to_parquet(Path("data/processed/players_2024.parquet"), index=False)
