import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from src.data_pipeline.utils import save_to_delta

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("Test Utils") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def test_save_to_delta(spark, tmp_path):
    data = [(1, "test"), (2, "data")]
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("value", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    delta_path = str(tmp_path / "delta_test")
    save_to_delta(df, delta_path)
    assert os.path.exists(delta_path)