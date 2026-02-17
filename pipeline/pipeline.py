# from src import ingest_data
from src.ingestion import ingest_data

print("Starting data ingestion process...")


def run_ingestion():
    # ingest_data.from_ingest()
    ingest_data.from_ingest()
    print("Data ingestion process completed.")


run_ingestion()
