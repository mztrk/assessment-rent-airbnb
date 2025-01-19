from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

def ingest_rentals_stream(spark, input_path, output_path):
    """
    Streams rental data and saves it to Delta format.

    Args:
        spark (pyspark.sql.SparkSession): The Spark session.
        input_path (str): Path to the JSON file containing rental data.
        output_path (str): Path to save the streamed data in Delta format.
    """
    schema = StructType().add("_id", StringType()) \
                         .add("city", StringType()) \
                         .add("latitude", DoubleType()) \
                         .add("longitude", DoubleType()) \
                         .add("rent", StringType())

    rental_stream = spark.readStream.schema(schema).json(input_path)
    rental_stream = rental_stream.withColumn("cleaned_rent", col("rent").cast(DoubleType()))

    rental_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", output_path + "/_checkpoint") \
        .start(output_path)