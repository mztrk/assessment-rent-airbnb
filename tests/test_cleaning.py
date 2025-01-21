import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.data_pipeline.cleaning import clean_postcode, clean_airbnb_data


@pytest.fixture(scope="module")
def spark():
    """
    Creates a SparkSession for testing.

    Returns:
        SparkSession: A local SparkSession configured with Delta Lake support.
    """
    return (
        SparkSession.builder.appName("Test Cleaning")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def test_clean_postcode():
    """
    Tests the clean_postcode function for various valid and invalid inputs.

    Valid cases:
        - Postcodes with 4 or more characters.
        - Postcodes with 6 characters are split correctly (e.g., "1055XP" -> "1055 XP").

    Invalid cases:
        - Empty string and None should return None.
    """
    # Valid postcodes
    assert clean_postcode("1053") == "1053"
    assert clean_postcode("1055XP") == "1055 XP"
    # Invalid postcodes
    assert clean_postcode("") == None
    assert clean_postcode(None) == None


def test_clean_airbnb_data(spark):
    """
    Tests the clean_airbnb_data function, which adds a cleaned_zipcode column to a DataFrame.

    Args:
        spark (SparkSession): The Spark session fixture.

    Steps:
        - Create a sample DataFrame with sample Airbnb data.
        - Apply the clean_airbnb_data function.
        - Validate the results for the cleaned_zipcode column and other unaffected columns.
    """
    # Sample data with various zipcode formats and room types.
    data = [
        ("1053", 52.37302064, 4.868460923, "Entire home/apt"),
        (None, 52.36190508, 4.888050037, "Private room"),
        ("1016 AM", 52.3713592, 4.888072287, "Shared room"),
    ]
    schema = StructType(
        [
            StructField("zipcode", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("room_type", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)
    result = clean_airbnb_data(df).collect()
    assert result[0]["zipcode"] == "1053"
    assert result[1]["zipcode"] is None
    assert result[2]["room_type"] == "Shared room"
