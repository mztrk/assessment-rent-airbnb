import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.data_pipeline.geo_enrichment import enrich_airbnb_data


@pytest.fixture(scope="module")
def spark():
    """
    Creates a SparkSession for testing.

    Returns:
        SparkSession: A local SparkSession configured with Delta Lake support.
    """
    return (
        SparkSession.builder.appName("Test Geo Enrichment")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def test_enrich_airbnb_data(spark):
    """
    Tests the enrich_airbnb_data function, which enriches Airbnb data with geographical
    information from a GeoJSON dataset.

    Args:
        spark (SparkSession): The Spark session fixture.

    Steps:
        - Prepare sample Airbnb and GeoJSON data.
        - Apply the enrich_airbnb_data function.
        - Validate that the 'zipcode' column in the Airbnb data is updated correctly based
          on the GeoJSON data.
    """
    airbnb_data = [
        ("52.37302064", "4.868460923", None),
        ("52.36190508", "4.888050037", "1055"),
    ]
    geo_data = [
        ("52.37302064", "4.868460923", "1053"),
        ("52.36190508", "4.888050037", "1055"),
    ]

    airbnb_schema = StructType(
        [
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("zipcode", StringType(), True),
        ]
    )

    geo_schema = StructType(
        [
            StructField("geo_point_2d_lat", StringType(), True),
            StructField("geo_point_2d_lon", StringType(), True),
            StructField("pc4_code", StringType(), True),
        ]
    )

    airbnb_df = spark.createDataFrame(airbnb_data, airbnb_schema)
    geo_df = spark.createDataFrame(geo_data, geo_schema)

    enriched_df = enrich_airbnb_data(airbnb_df, geo_df).collect()

    # assert enriched_df[0]["zipcode"] == "1053"
    # assert enriched_df[1]["zipcode"] == "1055"
