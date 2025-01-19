import pytest
from pyspark.sql import SparkSession
from stream_ingestion import ingest_rentals_stream

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("Test Stream Ingestion") \
        .master("local[*]") \
        .getOrCreate()

def test_ingest_rentals_stream(spark, tmp_path):
    input_path = str(tmp_path / "rental_stream.json")
    output_path = str(tmp_path / "delta_rentals")

    # Write sample JSON data
    sample_data = [{"_id": "1", "city": "Amsterdam", "latitude": 52.3738, "longitude": 4.8910, "rent": "1200"}]
    with open(input_path, "w") as f:
        f.write("\n".join(map(str, sample_data)))

    # Run streaming ingestion
    ingest_rentals_stream(spark, input_path, output_path)
    
    # Validate output
    output_df = spark.read.format("delta").load(output_path)
    assert output_df.count() == 1
    assert output_df.filter(output_df.city == "Amsterdam").count() == 1