from pyspark.sql.functions import col


def enrich_airbnb_data(airbnb_df, geojson_df):
    """
    Enrich the Airbnb DataFrame with postcode information from the GeoJSON DataFrame.

    This function joins the Airbnb dataset with the GeoJSON dataset based on matching latitude
    and longitude values. It adds a 'cleaned_zipcode' column to the Airbnb dataset by renaming
    the 'pc4_code' column from the GeoJSON dataset.

    Args:
        airbnb_df (DataFrame): Airbnb dataset containing latitude and longitude columns.
        geojson_df (DataFrame): GeoJSON dataset containing 'geo_point_2d_lat', 'geo_point_2d_lon', and 'pc4_code' columns.

    Returns:
        DataFrame: Enriched Airbnb dataset with added 'cleaned_zipcode' column.
    """
    # Perform a left join between Airbnb and GeoJSON data using latitude and longitude
    enriched_df = airbnb_df.join(
        geojson_df,
        (airbnb_df["latitude"] == geojson_df["geo_point_2d_lat"])
        & (  # Match latitude
            airbnb_df["longitude"] == geojson_df["geo_point_2d_lon"]
        ),  # Match longitude
        how="left",  # Retain all Airbnb records even if no match is found
    ).withColumnRenamed(
        "pc4_code",
        "cleaned_zipcode",  # Rename 'pc4_code' to 'cleaned_zipcode' for clarity
    )

    return enriched_df
