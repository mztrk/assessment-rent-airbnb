from pyspark.sql import SparkSession
from src.data_pipeline.geo_enrichment import enrich_airbnb_data
from src.data_pipeline.stream_ingestion import ingest_rentals_stream
from src.data_pipeline.visualization import generate_visualizations
from src.data_pipeline.ingestion import (
    load_airbnb_data,
    load_geojson_data,
    load_rentals_data,
)
from src.data_pipeline.cleaning import clean_airbnb_data
from src.data_pipeline.transformations import (
    impute_review_scores,
    transform_room_type,
    calculate_investment_potential,
)
from src.data_pipeline.utils import save_to_delta


def run_pipeline(spark, airbnb_path, geojson_path, rentals_path, output_path):
    """
    Executes the full data pipeline for Airbnb and rental data analysis.

    Steps:
        1. Load data: Load Airbnb, GeoJSON, and rental data.
        2. Clean and enrich: Clean Airbnb data and enrich it with GeoJSON data.
        3. Transform data: Apply transformations like imputing missing review scores
           and standardizing room types.
        4. Calculate metrics: Calculate investment potential based on transformed Airbnb
           and rental data.
        5. Save results: Save the enriched, transformed, and calculated metrics to Delta tables.
        6. Stream ingestion: Stream rental data into a Delta table.
        7. Generate visualizations: Create visualizations for analysis.

    Args:
        spark (SparkSession): Spark session object.
        airbnb_path (str): Path to the Airbnb data file.
        geojson_path (str): Path to the GeoJSON data file.
        rentals_path (str): Path to the rentals data file.
        output_path (str): Path to save the output files in Delta format.
    """
    # Step 1: Load Airbnb, GeoJSON, and rental data
    airbnb_df = load_airbnb_data(spark, airbnb_path)  # Load Airbnb data
    geojson_df = load_geojson_data(
        spark, geojson_path
    )  # Load GeoJSON data for enrichment

    # Step 2: Clean and enrich Airbnb data
    airbnb_cleaned = clean_airbnb_data(
        airbnb_df
    )  # Clean Airbnb data (e.g., format zip codes)
    airbnb_enriched = enrich_airbnb_data(
        airbnb_cleaned, geojson_df
    )  # Enrich data with geo features

    # Step 3: Apply transformations to the Airbnb data
    airbnb_transformed = impute_review_scores(
        airbnb_enriched
    )  # Impute missing review scores
    airbnb_transformed = transform_room_type(
        airbnb_transformed
    )  # Standardize room type categories

    # Step 4: Load and process rental data
    rentals_df = load_rentals_data(spark, rentals_path)  # Load rental data

    # Step 5: Calculate investment metrics
    investment_metrics = calculate_investment_potential(
        airbnb_transformed, rentals_df
    )  # Generate investment insights

    # Step 6: Save transformed and calculated data to Delta tables
    save_to_delta(
        airbnb_transformed, f"{output_path}/airbnb_gold"
    )  # Save cleaned and enriched Airbnb data
    save_to_delta(
        investment_metrics, f"{output_path}/investment_metrics"
    )  # Save investment metrics

    # Step 7: Stream rental data for further analysis
    ingest_rentals_stream(
        spark, rentals_path, f"{output_path}/rentals_bronze"
    )  # Stream rental data to Delta table

    # Step 8: Generate visualizations for analysis
    generate_visualizations(
        airbnb_transformed, f"{output_path}/visualizations"
    )  # Create charts and graphs


def main():
    """
    Entry point for the data pipeline.

    Configures the Spark session and defines file paths for the pipeline.
    """
    spark = (
        SparkSession.builder.appName("DLT Pipeline")
        .config(
            "spark.jars.packages", "io.delta:delta-core_2.12:2.1.0"
        )  # Include Delta Lake dependencies
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )  # Enable Delta Lake
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    # Define file paths for input data and output location
    airbnb_path = "data/airbnb.csv"  # Path to Airbnb CSV data
    geojson_path = "data/geo/post_codes.geojson"  # Path to GeoJSON data
    rentals_path = "data/rentals.json"  # Path to rental JSON data
    output_path = "delta"  # Path for saving processed Delta tables

    # Run the pipeline
    run_pipeline(spark, airbnb_path, geojson_path, rentals_path, output_path)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
