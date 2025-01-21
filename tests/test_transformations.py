import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.data_pipeline.transformations import impute_review_scores, transform_room_type


@pytest.fixture(scope="module")
def spark():
    """
    Pytest fixture to initialize a SparkSession for testing.

    Returns:
        SparkSession: A local SparkSession configured with Delta Lake support.
    """
    return (
        SparkSession.builder.appName("Test Transformations")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def test_impute_review_scores(spark):
    """
    Tests the `impute_review_scores` function to ensure it fills missing review scores with the average.

    Args:
        spark (SparkSession): The Spark session fixture.

    Steps:
        - Create a test DataFrame with missing review scores.
        - Apply the `impute_review_scores` transformation.
        - Assert that missing scores are correctly imputed with the average value for the group.
    """
    data = [
        ("1053", "Entire home/apt", 90.0),
        ("1053", "Entire home/apt", None),
    ]
    schema = StructType(
        [
            StructField("cleaned_zipcode", StringType(), True),
            StructField("room_type", StringType(), True),
            StructField("review_scores_value", DoubleType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)
    result = impute_review_scores(df).collect()
    assert result[1]["review_scores_value"] == 90.0


def test_transform_room_type(spark):
    """
    Tests the `transform_room_type` function to ensure room types are standardized.

    Args:
        spark (SparkSession): The Spark session fixture.

    Steps:
        - Create a test DataFrame with different room types.
        - Apply the `transform_room_type` transformation.
        - Assert that room types are mapped correctly to the expected standardized values.
    """
    data = [("Entire home/apt",), ("Private room",), ("Shared room",), ("Unknown",)]
    schema = StructType([StructField("room_type", StringType(), True)])
    df = spark.createDataFrame(data, schema)
    result = transform_room_type(df).collect()
    assert result[0]["room_type"] == "EntireHomeApt"
    assert result[1]["room_type"] == "PrivateRoom"
    assert result[2]["room_type"] == "SharedRoom"
    assert result[3]["room_type"] == "Unknown"
