from pyspark.sql import SparkSession


def load_airbnb_data(spark, file_path):
    """
    Load Airbnb data from a CSV file into a Spark DataFrame.

    Args:
        spark (SparkSession): The active Spark session.
        file_path (str): The path to the CSV file containing Airbnb data.

    Returns:
        DataFrame: A Spark DataFrame containing the Airbnb data.
    """
    # Read the CSV file with headers and return as a DataFrame
    return spark.read.option("header", "true").csv(file_path)


def load_rentals_data(spark, file_path):
    """
    Load rental data from a JSON file into a Spark DataFrame.

    Args:
        spark (SparkSession): The active Spark session.
        file_path (str): The path to the JSON file containing rental data.

    Returns:
        DataFrame: A Spark DataFrame containing the rental data.
    """
    # Read the JSON file and return as a DataFrame
    return spark.read.json(file_path)


def load_geojson_data(spark, file_path):
    """
    Load GeoJSON data from a file into a Spark DataFrame.

    Args:
        spark (SparkSession): The active Spark session.
        file_path (str): The path to the GeoJSON file.

    Returns:
        DataFrame: A Spark DataFrame containing the GeoJSON data.
    """
    # Read the GeoJSON file as JSON and return as a DataFrame
    return spark.read.format("json").load(file_path)
