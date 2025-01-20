from pyspark.sql.functions import col

# def enrich_postcodes(df, geojson_df):
#     # Extract latitude and longitude from the nested structure
#     geojson_df = geojson_df \
#         .withColumn("geo_point_2d_lat", col("geo_point_2d.lat")) \
#         .withColumn("geo_point_2d_lon", col("geo_point_2d.lon"))

#     # Perform the join
#     enriched_df = df.join(
#         geojson_df,
#         (df["latitude"] == geojson_df["geo_point_2d_lat"]) &
#         (df["longitude"] == geojson_df["geo_point_2d_lon"]),
#         how="left"
#     ).withColumnRenamed("pc4_code", "zipcode")

#     return enriched_df


from pyspark.sql.functions import col, when


def enrich_postcodes(airbnb_df, geojson_df):
    """
    Enrich the Airbnb DataFrame with postcode information from the GeoJSON DataFrame.

    Args:
        airbnb_df (DataFrame): Airbnb data with latitude and longitude.
        geojson_df (DataFrame): GeoJSON data with nested geo_point_2d and pc4_code.

    Returns:
        DataFrame: Enriched Airbnb data with an updated 'zipcode' column.
    """
    # Extract nested fields into flat columns
    geojson_df = (
        geojson_df.withColumn("geo_point_2d_lat", col("geo_point_2d.lat"))
        .withColumn("geo_point_2d_lon", col("geo_point_2d.lon"))
        .drop("geo_point_2d")
    )  # Drop the original nested column if not needed

    # Perform the join
    enriched_df = airbnb_df.join(
        geojson_df,
        (airbnb_df["latitude"] == col("geo_point_2d_lat"))
        & (airbnb_df["longitude"] == col("geo_point_2d_lon")),
        how="left",
    )

    # Update the 'zipcode' column with the joined 'pc4_code' if available
    enriched_df = enriched_df.withColumn(
        "zipcode",
        when(col("pc4_code").isNotNull(), col("pc4_code")).otherwise(col("zipcode")),
    ).drop(
        "pc4_code"
    )  # Drop the 'pc4_code' column after updating

    return enriched_df


def enrich_airbnb_data(airbnb_df, geojson_df):
    """
    Enrich the Airbnb DataFrame with postcode information from the GeoJSON DataFrame.

    Args:
        airbnb_df (DataFrame): Airbnb data with latitude and longitude.
        geojson_df (DataFrame): GeoJSON data with nested geo_point_2d and pc4_code.

    Returns:
        DataFrame: Enriched Airbnb data with a 'cleaned_zipcode' column.
    """
    enriched_df = airbnb_df.join(
        geojson_df,
        (airbnb_df["latitude"] == col("geo_point_2d.lat"))
        & (airbnb_df["longitude"] == col("geo_point_2d.lon")),
        how="left",
    ).withColumnRenamed("pc4_code", "cleaned_zipcode")

    return enriched_df
