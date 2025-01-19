import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.data_pipeline.enrichment import enrich_postcodes

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("Test Enrichment") \
        .master("local[*]") \
        .getOrCreate()

def test_enrich_postcodes(spark):
    airbnb_data = [
        (52.37302064, 4.868460923, None),
        (52.36190508, 4.888050037, None),
    ]
    geojson_data = [
        (52.37302064, 4.868460923, "1053"),
        (52.36190508, 4.888050037, "1017"),
    ]
    airbnb_schema = StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("zipcode", StringType(), True)
    ])
    geojson_schema = StructType([
        StructField("geo_point_2d.lat", DoubleType(), True),
        StructField("geo_point_2d.lon", DoubleType(), True),
        StructField("pc4_code", StringType(), True)
    ])
    airbnb_df = spark.createDataFrame(airbnb_data, airbnb_schema)
    geojson_df = spark.createDataFrame(geojson_data, geojson_schema)

    result = enrich_postcodes(airbnb_df, geojson_df).collect()
    assert result[0]["zipcode"] == "1053"
    assert result[1]["zipcode"] == "1017"