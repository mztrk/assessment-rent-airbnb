from pyspark.sql.functions import col

def enrich_airbnb_data(airbnb_df, geojson_df):
    """
    Enriches Airbnb data with geographic information from a GeoJSON dataset.
    
    Args:
        airbnb_df (pyspark.sql.DataFrame): The Airbnb DataFrame.
        geojson_df (pyspark.sql.DataFrame): The GeoJSON DataFrame with location data.

    Returns:
        pyspark.sql.DataFrame: Enriched Airbnb DataFrame.
    """
    enriched_df = airbnb_df.join(
        geojson_df,
        (airbnb_df["latitude"] == geojson_df["geo_point_2d.lat"]) &
        (airbnb_df["longitude"] == geojson_df["geo_point_2d.lon"]),
        how="left"
    )
    return enriched_df