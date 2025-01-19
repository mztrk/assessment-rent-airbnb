import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from geo_enrichment import enrich_airbnb_data

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("Test Geo Enrichment") \
        .master("local[*]") \
        .getOrCreate()

def test_enrich_airbnb_data(spark):
    airbnb_data = [
        ("52.37302064", "4.868460923", None),
        ("52.36190508", "4.888050037", "1055")
    ]
    geo_data = [
        ("52.37302064", "4.868460923", "1053"),
        ("52.36190508", "4.888050037", "1055")
    ]
    
    airbnb_schema = StructType([
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("zipcode", StringType(), True)
    ])
    
    geo_schema = StructType([
        StructField("geo_point_2d.lat", StringType(), True),
        StructField("geo_point_2d.lon", StringType(), True),
        StructField("pc4_code", StringType(), True)
    ])
    
    airbnb_df = spark.createDataFrame(airbnb_data, airbnb_schema)
    geo_df = spark.createDataFrame(geo_data, geo_schema)
    
    enriched_df = enrich_airbnb_data(airbnb_df, geo_df).collect()
    
    assert enriched_df[0]["zipcode"] == "1053"
    assert enriched_df[1]["zipcode"] == "1055"