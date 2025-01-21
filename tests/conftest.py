import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Creates a PySpark SparkSession fixture for testing.

    This fixture:
        - Configures the Spark session with Delta Lake support.
        - Uses `local[*]` as the master to run locally on all available cores.
        - Ensures the Spark session is shared across all tests in the session scope.

    Returns:
        SparkSession: A configured PySpark SparkSession for testing.
    """
    return (
        SparkSession.builder.appName(
            "Test Suite"
        )  # Set the application name for test runs.
        .master("local[*]")  # Use all available cores for local testing.
        .config(
            "spark.jars.packages", "io.delta:delta-core_2.12:2.1.0"
        )  # Include Delta Lake dependency.
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )  # Enable Delta SQL extensions.
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",  # Use Delta catalog for Spark SQL.
        )
        .getOrCreate()
    )
