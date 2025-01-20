from pyspark.sql.functions import col


def enrich_airbnb_data(airbnb_df, geojson_df):
    enriched_df = airbnb_df.join(
        geojson_df,
        (airbnb_df["latitude"] == geojson_df["geo_point_2d_lat"])
        & (airbnb_df["longitude"] == geojson_df["geo_point_2d_lon"]),
        how="left",
    ).withColumnRenamed("pc4_code", "cleaned_zipcode")
    return enriched_df
