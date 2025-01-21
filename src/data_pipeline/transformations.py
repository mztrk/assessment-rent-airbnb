from pyspark.sql.functions import mean, when, col, lit


def impute_review_scores(df):
    """
    Impute missing review scores with the average review score for the same zipcode and room type.

    Args:
        df (DataFrame): Input DataFrame containing the columns 'cleaned_zipcode', 'room_type',
                        and 'review_scores_value'.

    Returns:
        DataFrame: DataFrame with missing review scores replaced by the average scores.
    """
    # Calculate the average review score for each combination of zipcode and room type
    avg_scores = df.groupBy("cleaned_zipcode", "room_type").agg(
        mean("review_scores_value").alias("avg_score")
    )

    # Join the original DataFrame with the average scores and impute missing values
    return (
        df.join(avg_scores, on=["cleaned_zipcode", "room_type"], how="left")
        .withColumn(
            "review_scores_value",
            # Replace null values with the calculated average score
            when(col("review_scores_value").isNull(), col("avg_score")).otherwise(
                col("review_scores_value")
            ),
        )
        .drop("avg_score")  # Drop the intermediate column used for imputation
    )


def transform_room_type(df):
    """
    Transform the 'room_type' column into standardized categories.

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame with a 'room_type' column.

    Returns:
        pyspark.sql.DataFrame: DataFrame with a transformed 'room_type' column.
    """
    return df.withColumn(
        "room_type",
        # Map specific room types to standard categories
        when(col("room_type") == "Entire home/apt", "EntireHomeApt")
        .when(col("room_type") == "Private room", "PrivateRoom")
        .when(col("room_type") == "Shared room", "SharedRoom")
        .otherwise("Unknown"),  # Assign "Unknown" for other or missing values
    )


def calculate_investment_potential(airbnb_df, rental_df):
    """
    Calculates the potential average revenue per house and determines the preferred investment type
    for each postcode.

    Args:
        airbnb_df (DataFrame): Airbnb DataFrame with 'cleaned_zipcode' and 'price' columns.
        rental_df (DataFrame): Rental DataFrame with 'postalCode' and 'rent' columns.

    Returns:
        DataFrame: A DataFrame containing aggregated revenues, revenue ratio, and investment type
                   for each postcode.
    """
    # Aggregate Airbnb revenue by cleaned_zipcode
    airbnb_revenue = (
        airbnb_df.groupBy("cleaned_zipcode").avg("price").alias("avg_airbnb_revenue")
    )

    # Aggregate rental revenue by postalCode
    rental_revenue = (
        rental_df.groupBy("postalCode").avg("rent").alias("avg_rental_revenue")
    )

    # Rename columns for consistency
    airbnb_revenue = airbnb_revenue.withColumnRenamed("cleaned_zipcode", "zipcode")
    rental_revenue = rental_revenue.withColumnRenamed("postalCode", "zipcode")

    # Join Airbnb and rental revenues on zipcode
    combined_revenue = airbnb_revenue.join(rental_revenue, on="zipcode", how="outer")

    # Calculate revenue ratio (Airbnb revenue to rental revenue)
    combined_revenue = combined_revenue.withColumn(
        "revenue_ratio", col("avg_airbnb_revenue") / col("avg_rental_revenue")
    )

    # Determine the preferred investment type based on revenue comparison
    combined_revenue = combined_revenue.withColumn(
        "investment_type",
        when(
            col("avg_airbnb_revenue") > col("avg_rental_revenue"),
            lit("Airbnb Profitable"),  # Airbnb is more profitable
        ).otherwise(
            lit("Rental Profitable")
        ),  # Rentals are more profitable
    )

    return combined_revenue
