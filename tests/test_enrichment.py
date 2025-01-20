import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.data_pipeline.enrichment import enrich_postcodes


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.appName("Test Enrichment")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def test_enrich_postcodes(spark):
    # Prepare test data
    airbnb_data = [
        (52.37302064, 4.868460923, "1000"),  # Existing zipcode
        (52.36190508, 4.888050037, None),  # No zipcode
    ]
    geojson_data = [
        ({"lat": 52.37302064, "lon": 4.868460923}, "1053"),
        ({"lat": 52.36190508, "lon": 4.888050037}, "1017"),
    ]

    airbnb_schema = StructType(
        [
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("zipcode", StringType(), True),
        ]
    )

    geojson_schema = StructType(
        [
            StructField(
                "geo_point_2d",
                StructType(
                    [
                        StructField("lat", DoubleType(), True),
                        StructField("lon", DoubleType(), True),
                    ]
                ),
                True,
            ),
            StructField("pc4_code", StringType(), True),
        ]
    )

    airbnb_df = spark.createDataFrame(airbnb_data, airbnb_schema)
    geojson_df = spark.createDataFrame(geojson_data, geojson_schema)

    # Enrich postcodes
    enriched_df = enrich_postcodes(airbnb_df, geojson_df)

    # Collect results
    results = enriched_df.collect()

    # Validate enriched postcodes
    assert (
        results[0]["zipcode"] == "1053"
    ), f"Expected '1053', got {results[0]['zipcode']}"
    assert (
        results[1]["zipcode"] == "1017"
    ), f"Expected '1017', got {results[1]['zipcode']}"
