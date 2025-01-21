import os
import pytest
from pyspark.sql import SparkSession
from src.data_pipeline.ingestion import load_airbnb_data, load_geojson_data


@pytest.fixture(scope="module")
def spark():
    """
    Pytest fixture to initialize a SparkSession for testing.

    Returns:
        SparkSession: A local SparkSession configured with Delta Lake support.
    """
    return (
        SparkSession.builder.appName("Test Ingestion")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def test_load_airbnb_data(spark):
    """
    Tests the `load_airbnb_data` function to ensure that Airbnb data is loaded correctly.

    Args:
        spark (SparkSession): The Spark session fixture.

    Steps:
        - Provide a sample CSV file path for Airbnb data.
        - Load the data using the `load_airbnb_data` function.
        - Assert that the resulting DataFrame contains rows.
    """
    sample_csv = "data/airbnb.csv"
    df = load_airbnb_data(spark, sample_csv)
    assert df.count() > 0


def test_load_geojson_data(spark):
    """
    Tests the `load_geojson_data` function to ensure that GeoJSON data is loaded correctly.

    Args:
        spark (SparkSession): The Spark session fixture.

    Steps:
        - Provide a sample GeoJSON file path.
        - Load the data using the `load_geojson_data` function.
        - Assert that the resulting DataFrame contains rows.
    """
    sample_geojson = "data/geo/post_codes.geojson"
    df = load_geojson_data(spark, sample_geojson)
    assert df.count() > 0
