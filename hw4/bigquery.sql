CREATE OR REPLACE EXTERNAL TABLE `nytaxi.green_tripdata_external`
  OPTIONS (
    format = 'parquet',
    uris =
      [
        'gs://de_source_bucket/green_tripdata_2019-*.parquet',
        'gs://de_source_bucket/green_tripdata_2020-*.parquet']);

CREATE OR REPLACE EXTERNAL TABLE `nytaxi.yellow_tripdata_external`
  OPTIONS (
    format = 'parquet',
    uris =
      [
        'gs://de_source_bucket/yellow_tripdata_2019-*.parquet',
        'gs://de_source_bucket/yellow_tripdata_2020-*.parquet']);


CREATE OR REPLACE TABLE nytaxi.green_tripdata AS
SELECT * FROM taxi-rides-ny.nytaxi.green_tripdata_external;



