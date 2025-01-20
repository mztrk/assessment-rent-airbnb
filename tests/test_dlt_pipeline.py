import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from src.data_pipeline.dlt_pipeline import run_pipeline


@pytest.fixture(scope="module")
def spark():
    """Fixture to initialize a Spark session."""
    return SparkSession.builder \
        .appName("DLT Pipeline Test") \
        .master("local[*]") \
        .getOrCreate()


@pytest.fixture
def airbnb_data(spark):
    """Sample Airbnb data for testing."""
    data = [
        ("1053", 52.37302064, 4.868460923, "Entire home/apt", 4, 2.0, 130, 100.0),
        (None, 52.36575451, 4.941419235, "Private room", 2, 1.0, 59, None),
        ("1017", 52.36190508, 4.888050037, "Entire home/apt", 2, 1.0, 100, 100.0),
    ]
    schema = StructType([
        StructField("zipcode", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("room_type", StringType(), True),
        StructField("accommodates", IntegerType(), True),
        StructField("bedrooms", DoubleType(), True),
        StructField("price", DoubleType(), True),
        StructField("review_scores_value", DoubleType(), True)
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def geojson_data(spark):
    """Sample GeoJSON data for testing."""
    data = [
        (52.37302064, 4.868460923, "1053"),
        (52.36575451, 4.941419235, "1055"),
        (52.36190508, 4.888050037, "1017"),
    ]
    schema = StructType([
        StructField("geo_point_2d.lat", DoubleType(), True),
        StructField("geo_point_2d.lon", DoubleType(), True),
        StructField("pc4_code", StringType(), True)
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def rentals_data(spark):
    """Sample rentals data for testing."""
    data = [
        ("3074HN", "Rotterdam", 500, 14),
        ("1018AS", "Amsterdam", 950, 30),
        ("1075SB", "Amsterdam", 1000, 11),
    ]
    schema = StructType([
        StructField("postalCode", StringType(), True),
        StructField("city", StringType(), True),
        StructField("rent", IntegerType(), True),
        StructField("areaSqm", IntegerType(), True)
    ])
    return spark.createDataFrame(data, schema)


def test_pipeline_execution(spark, airbnb_data, geojson_data, rentals_data):
    """Test the entire DLT pipeline."""
    # Define file paths for mock input and output
    airbnb_path = "/tmp/mock_airbnb_data"
    geojson_path = "/tmp/mock_geojson_data"
    rentals_path = "/tmp/mock_rentals_data"
    output_path = "/tmp/mock_pipeline_output"

    # Save mock data to temporary paths
    airbnb_data.write.mode("overwrite").parquet(airbnb_path)
    geojson_data.write.mode("overwrite").parquet(geojson_path)
    rentals_data.write.mode("overwrite").parquet(rentals_path)

    # Run the pipeline
    run_pipeline(spark, airbnb_path, geojson_path, rentals_path, output_path)

    # Read and validate the output
    output_df = spark.read.format("delta").load(output_path)
    assert output_df.count() > 0, "Pipeline output should not be empty"

    # Validate data transformations
    assert "cleaned_zipcode" in output_df.columns, "cleaned_zipcode column is missing"
    assert "review_scores_value" in output_df.columns, "review_scores_value column is missing"

    # Check that enriched postcodes are present
    enriched_zipcodes = output_df.select("cleaned_zipcode").distinct().rdd.flatMap(lambda x: x).collect()
    assert "1053" in enriched_zipcodes, "Expected enriched zipcode 1053 not found"
    assert "1017" in enriched_zipcodes, "Expected enriched zipcode 1017 not found"