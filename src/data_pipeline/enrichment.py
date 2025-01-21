from pyspark.sql.functions import col, when


def enrich_postcodes(airbnb_df, geojson_df):
    """
    Enrich the Airbnb DataFrame with postcode information from the GeoJSON DataFrame.

    This function maps latitude and longitude coordinates from the Airbnb dataset to
    corresponding postcodes in the GeoJSON dataset. It updates the 'zipcode' column
    with data from the GeoJSON file, prioritizing 'pc4_code' if available.

    Args:
        airbnb_df (DataFrame): Airbnb dataset containing latitude and longitude columns.
        geojson_df (DataFrame): GeoJSON dataset with geographical coordinates and postcode information.

    Returns:
        DataFrame: Enriched Airbnb data with updated 'zipcode' column.
    """
    # Extract latitude and longitude fields from the nested GeoJSON column
    geojson_df = (
        geojson_df.withColumn(
            "geo_point_2d_lat", col("geo_point_2d.lat")
        )  # Extract latitude
        .withColumn("geo_point_2d_lon", col("geo_point_2d.lon"))  # Extract longitude
        .drop("geo_point_2d")  # Drop the original nested column
    )

    # Perform a left join to enrich Airbnb data with GeoJSON information based on coordinates
    enriched_df = airbnb_df.join(
        geojson_df,
        (airbnb_df["latitude"] == col("geo_point_2d_lat"))  # Match latitude
        & (airbnb_df["longitude"] == col("geo_point_2d_lon")),  # Match longitude
        how="left",  # Keep all Airbnb data even if no match is found
    )

    # Update the 'zipcode' column: use 'pc4_code' if available; otherwise, retain the original 'zipcode'
    enriched_df = enriched_df.withColumn(
        "zipcode",
        when(
            col("pc4_code").isNotNull(), col("pc4_code")
        ).otherwise(  # Use 'pc4_code' if it exists
            col("zipcode")
        ),  # Fallback to original 'zipcode'
    ).drop(
        "pc4_code"  # Drop 'pc4_code' column after updating
    )

    return enriched_df


def enrich_airbnb_data(airbnb_df, geojson_df):
    """
    Enrich the Airbnb DataFrame with cleaned postcode information.

    This function maps latitude and longitude from the Airbnb dataset to geographical
    postcodes in the GeoJSON dataset. It adds a 'cleaned_zipcode' column for enriched results.

    Args:
        airbnb_df (DataFrame): Airbnb dataset containing latitude and longitude columns.
        geojson_df (DataFrame): GeoJSON dataset with nested geo_point_2d column and postcode information.

    Returns:
        DataFrame: Enriched Airbnb data with 'cleaned_zipcode' column.
    """
    # Perform a left join to map latitude and longitude to the corresponding GeoJSON postcodes
    enriched_df = airbnb_df.join(
        geojson_df,
        (airbnb_df["latitude"] == col("geo_point_2d.lat"))  # Match latitude
        & (airbnb_df["longitude"] == col("geo_point_2d.lon")),  # Match longitude
        how="left",  # Keep all Airbnb data even if no match is found
    ).withColumnRenamed(
        "pc4_code",
        "cleaned_zipcode",  # Rename 'pc4_code' to 'cleaned_zipcode' for clarity
    )

    return enriched_df
