"""Test script to verify the taxi_pipeline configuration."""

import dlt
from dlt.sources.rest_api import rest_api_source


def test_taxi_api():
    """Quick test of the taxi API configuration."""
    
    # Configure to fetch only 2 pages (2000 records) for testing
    config = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
            "paginator": {
                "type": "offset",
                "limit": 1000,
                "offset": 0,
                "offset_param": "offset",
                "limit_param": "limit",
                "maximum_offset": 1000,  # Stop after 2 pages for testing
            },
        },
        "resources": [
            {
                "name": "taxi_data",
                "endpoint": {
                    "path": "",
                    "data_selector": "$",
                },
            },
        ],
    }

    # Create test pipeline
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline_test",
        destination="duckdb",
        dataset_name="taxi_data_test",
        dev_mode=True,
    )

    # Run with limited data
    source = rest_api_source(config)
    load_info = pipeline.run(source)
    
    print("\n✅ Test successful!")
    print(f"Load info: {load_info}")
    
    # Check loaded data
    with pipeline.sql_client() as client:
        with client.execute_query("SELECT COUNT(*) as count FROM taxi_data") as cursor:
            result = cursor.fetchone()
            print(f"\n📊 Total records loaded: {result[0]}")


if __name__ == "__main__":
    test_taxi_api()
