import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.data_pipeline.transformations import impute_review_scores, transform_room_type


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("Test Transformations") \
        .master("local[*]") \
        .getOrCreate()

def test_impute_review_scores(spark):
    data = [
        ("1053", "Entire home/apt", 90.0),
        ("1053", "Entire home/apt", None),
    ]
    schema = StructType([
        StructField("cleaned_zipcode", StringType(), True),
        StructField("room_type", StringType(), True),
        StructField("review_scores_value", DoubleType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    result = impute_review_scores(df).collect()
    assert result[1]["review_scores_value"] == 90.0

def test_transform_room_type(spark):
    data = [("Entire home/apt",), ("Private room",), ("Shared room",), ("Unknown",)]
    schema = StructType([StructField("room_type", StringType(), True)])
    df = spark.createDataFrame(data, schema)
    result = transform_room_type(df).collect()
    assert result[0]["room_type"] == "EntireHomeApt"
    assert result[1]["room_type"] == "PrivateRoom"
    assert result[2]["room_type"] == "SharedRoom"
    assert result[3]["room_type"] == "Unknown"