from pyspark.sql import SparkSession
from src.data_pipeline.geo_enrichment import enrich_airbnb_data
from src.data_pipeline.stream_ingestion import ingest_rentals_stream
from src.data_pipeline.visualization import generate_visualizations
from src.data_pipeline.ingestion import load_airbnb_data, load_geojson_data
from src.data_pipeline.cleaning import clean_airbnb_data
from src.data_pipeline.transformations import impute_review_scores, transform_room_type
from src.data_pipeline.utils import save_to_delta


def run_pipeline(spark, airbnb_path, geojson_path, rentals_path, output_path):
    """
    Runs the data pipeline.

    Args:
        spark (SparkSession): Spark session object.
        airbnb_path (str): Path to Airbnb data.
        geojson_path (str): Path to geojson data.
        rentals_path (str): Path to rentals data.
        output_path (str): Path to save the pipeline outputs.
    """
    # Load Airbnb data and geo data
    airbnb_df = load_airbnb_data(spark, airbnb_path)
    geojson_df = load_geojson_data(spark, geojson_path)

    # Clean and enrich Airbnb data
    airbnb_cleaned = clean_airbnb_data(airbnb_df)
    airbnb_enriched = enrich_airbnb_data(airbnb_cleaned, geojson_df)

    # Apply transformations
    airbnb_transformed = impute_review_scores(airbnb_enriched)
    airbnb_transformed = transform_room_type(airbnb_transformed)

    # Save enriched and transformed data
    save_to_delta(airbnb_transformed, f"{output_path}/airbnb_gold")

    # Stream rental data
    ingest_rentals_stream(spark, rentals_path, f"{output_path}/rentals_bronze")

    # Generate visualizations
    generate_visualizations(airbnb_transformed, f"{output_path}/visualizations")


def main():
    """
    Entry point for the pipeline.
    """
    spark = (
        SparkSession.builder.appName("DLT Pipeline")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    # Define file paths
    airbnb_path = "data/airbnb.csv"
    geojson_path = "data/geo/post_codes.geojson"
    rentals_path = "data/rentals.json"
    output_path = "delta"

    # Run the pipeline
    run_pipeline(spark, airbnb_path, geojson_path, rentals_path, output_path)

    spark.stop()


if __name__ == "__main__":
    main()
