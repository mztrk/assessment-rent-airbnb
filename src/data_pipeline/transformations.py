from pyspark.sql.functions import mean, when, col

def impute_review_scores(df):
    avg_scores = df.groupBy("cleaned_zipcode", "room_type").agg(mean("review_scores_value").alias("avg_score"))
    return df.join(avg_scores, on=["cleaned_zipcode", "room_type"], how="left").withColumn(
        "review_scores_value",
        when(col("review_scores_value").isNull(), col("avg_score")).otherwise(col("review_scores_value"))
    ).drop("avg_score")


def transform_room_type(df):
    """
    Transform room_type column into standardized categories.

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame with a room_type column.

    Returns:
        pyspark.sql.DataFrame: Transformed DataFrame.
    """
    return df.withColumn(
        "room_type",
        when(col("room_type") == "Entire home/apt", "EntireHomeApt")
        .when(col("room_type") == "Private room", "PrivateRoom")
        .when(col("room_type") == "Shared room", "SharedRoom")
        .otherwise("Unknown")
    )