from pyspark.sql import SparkSession

def load_airbnb_data(spark, file_path):
    return spark.read.option("header", "true").csv(file_path)

def load_rentals_data(spark, file_path):
    return spark.read.json(file_path)

def load_geojson_data(spark, file_path):
    return spark.read.format("json").load(file_path)