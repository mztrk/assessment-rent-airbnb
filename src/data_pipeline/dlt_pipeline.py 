from pyspark.sql import SparkSession
from geo_enrichment import enrich_airbnb_data
from stream_ingestion import ingest_rentals_stream
from visualization import generate_visualizations
from src.data_pipeline.ingestion import load_airbnb_data, load_geojson_data
from src.data_pipeline.cleaning import clean_airbnb_data
from src.data_pipeline.transformations import impute_review_scores, transform_room_type
from src.data_pipeline.utils import save_to_delta

def main():
    spark = SparkSession.builder.appName("DLT Pipeline").getOrCreate()

    # Load Airbnb data and geo data
    airbnb_df = load_airbnb_data(spark, "data/airbnb.csv")
    geojson_df = load_geojson_data(spark, "data/geo/post_codes.geojson")

    # Clean and enrich Airbnb data
    airbnb_cleaned = clean_airbnb_data(airbnb_df)
    airbnb_enriched = enrich_airbnb_data(airbnb_cleaned, geojson_df)

    # Apply transformations
    airbnb_transformed = impute_review_scores(airbnb_enriched)
    airbnb_transformed = transform_room_type(airbnb_transformed)

    # Save enriched and transformed data
    save_to_delta(airbnb_transformed, "delta/airbnb_gold")

    # Stream rental data
    ingest_rentals_stream(spark, "data/rentals.json", "delta/rentals_bronze")

    # Generate visualizations
    generate_visualizations(airbnb_transformed, "output/visualizations")

    spark.stop()

if __name__ == "__main__":
    main()