import pandas as pd
from pipeline import pipeline

# df = pd.read_parquet(".data/green_tripdata_2025-11.parquet")
# print(df.head())

pipeline.run_ingestion()
