import os
import pytest
from pyspark.sql import SparkSession
from src.data_pipeline.ingestion import load_airbnb_data, load_geojson_data


@pytest.fixture(scope="module")
def spark():
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
    sample_csv = "data/airbnb.csv"
    df = load_airbnb_data(spark, sample_csv)
    assert df.count() > 0


def test_load_geojson_data(spark):
    sample_geojson = "data/geo/post_codes.geojson"
    df = load_geojson_data(spark, sample_geojson)
    assert df.count() > 0
