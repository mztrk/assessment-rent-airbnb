from pyspark.sql.functions import mean, when, col, lit


def impute_review_scores(df):
    avg_scores = df.groupBy("cleaned_zipcode", "room_type").agg(
        mean("review_scores_value").alias("avg_score")
    )
    return (
        df.join(avg_scores, on=["cleaned_zipcode", "room_type"], how="left")
        .withColumn(
            "review_scores_value",
            when(col("review_scores_value").isNull(), col("avg_score")).otherwise(
                col("review_scores_value")
            ),
        )
        .drop("avg_score")
    )


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
        .otherwise("Unknown"),
    )


def calculate_investment_potential(airbnb_df, rental_df):
    """
    Calculates potential average revenue per house and determines investment type by postcode.

    Args:
        airbnb_df (DataFrame): Transformed Airbnb DataFrame with price and zipcode.
        rental_df (DataFrame): Transformed Rental DataFrame with rent and postalCode.

    Returns:
        DataFrame: A DataFrame with investment analysis metrics.
    """
    # Aggregate Airbnb and rental revenue by postcode
    airbnb_revenue = (
        airbnb_df.groupBy("cleaned_zipcode").avg("price").alias("avg_airbnb_revenue")
    )
    rental_revenue = (
        rental_df.groupBy("postalCode").avg("rent").alias("avg_rental_revenue")
    )

    # Rename columns for clarity
    airbnb_revenue = airbnb_revenue.withColumnRenamed("cleaned_zipcode", "zipcode")
    rental_revenue = rental_revenue.withColumnRenamed("postalCode", "zipcode")

    # Join the aggregated revenues
    combined_revenue = airbnb_revenue.join(rental_revenue, on="zipcode", how="outer")

    # Calculate revenue ratio and investment type
    combined_revenue = combined_revenue.withColumn(
        "revenue_ratio", col("avg_airbnb_revenue") / col("avg_rental_revenue")
    ).withColumn(
        "investment_type",
        when(
            col("avg_airbnb_revenue") > col("avg_rental_revenue"),
            lit("Airbnb Profitable"),
        ).otherwise(lit("Rental Profitable")),
    )

    return combined_revenue
