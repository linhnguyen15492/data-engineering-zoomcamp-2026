import subprocess
import requests
import gzip
import shutil
import pandas as pd
from glob import glob

MONTHS = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
YEARS = ["2020"]
TAXI_TYPES = ["green", "yellow"]

URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
dir = "../data"


def download_file(url, local_path):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def unzip_file(gz_path, out_path):
    with gzip.open(gz_path, "rb") as f_in:
        with open(out_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


# for year in YEARS:
#     for month in MONTHS:
#         for taxi in TAXI_TYPES:
#             input_url = f'{URL}/{taxi}/{taxi}_tripdata_{year}-{month}.csv.gz'
#             gz_file = f"{dir}/{taxi}_tripdata_{year}-{month}.csv.gz"
#             output_file = f'{dir}/{taxi}_tripdata_{year}-{month}.csv'
#
#             print(f"Downloading {input_url} ...")
#
#             # download
#             download_file(input_url, gz_file)
#             # unzip
#             unzip_file(gz_file, output_file)
#             print(f"Saved {output_file}")


def count_rows():
    for taxi in TAXI_TYPES:
        files = glob(f"{dir}/{taxi}_tripdata_2020-*.csv")
        total_rows = 0
        for file in files:
            df = pd.read_csv(file)
            total_rows += len(df)

        print(f"Total rows for {taxi}:", total_rows)


def main():
    taxi = "yellow"
    year = "2021"
    month = "03"
    input_url = f"{URL}/{taxi}/{taxi}_tripdata_{year}-{month}.csv.gz"
    gz_file = f"{dir}/{taxi}_tripdata_{year}-{month}.csv.gz"
    output_file = f"{dir}/{taxi}_tripdata_{year}-{month}.csv"
    download_file(input_url, gz_file)
    unzip_file(gz_file, output_file)
    file = output_file
    df = pd.read_csv(file)
    print(f"Rows in {file}:", len(df))


def from_ingest():
    print("This function is called from ingest_data.")


if __name__ == "__main__":
    main()
