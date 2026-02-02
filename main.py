import pandas as pd


df = pd.read_parquet(".data/green_tripdata_2025-11.parquet")
print(df.head())
