import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.data_pipeline.cleaning import clean_postcode, clean_airbnb_data

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("Test Cleaning") \
        .master("local[*]") \
        .getOrCreate()

def test_clean_postcode():
    # Valid postcodes
    assert clean_postcode("1053") == "1053"
    assert clean_postcode("1055XP") == "1055 XP"
    # Invalid postcodes
    assert clean_postcode("") == None
    assert clean_postcode(None) == None

def test_clean_airbnb_data(spark):
    data = [
        ("1053", 52.37302064, 4.868460923, "Entire home/apt"),
        (None, 52.36190508, 4.888050037, "Private room"),
        ("1016 AM", 52.3713592, 4.888072287, "Shared room")
    ]
    schema = StructType([
        StructField("zipcode", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("room_type", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    result = clean_airbnb_data(df).collect()
    assert result[0]["zipcode"] == "1053"
    assert result[1]["zipcode"] is None
    assert result[2]["room_type"] == "Shared room"