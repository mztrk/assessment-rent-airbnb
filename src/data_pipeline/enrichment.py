def enrich_postcodes(df, geojson_df):
    enriched_df = df.join(
        geojson_df,
        (df["latitude"] == geojson_df["geo_point_2d.lat"]) &
        (df["longitude"] == geojson_df["geo_point_2d.lon"]),
        how="left"
    )
    return enriched_df