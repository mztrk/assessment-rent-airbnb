import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.data_pipeline.visualization import generate_visualizations
import os


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.appName("Test Visualization")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def test_generate_visualizations(spark, tmp_path):
    data = [
        ("1053", "EntireHomeApt", 120.0),
        ("1055", "PrivateRoom", 80.0),
        ("1053", "EntireHomeApt", 150.0),
    ]
    schema = StructType(
        [
            StructField("cleaned_zipcode", StringType(), True),
            StructField("room_type", StringType(), True),
            StructField("price", DoubleType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)
    output_dir = str(tmp_path / "visualizations")
    os.makedirs(output_dir, exist_ok=True)

    generate_visualizations(df, output_dir)

    assert os.path.exists(f"{output_dir}/average_price_by_postcode.png")
    assert os.path.exists(f"{output_dir}/room_type_distribution.png")
