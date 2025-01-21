from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType


def clean_postcode(postcode):
    """
    Cleans and formats a postcode string into a standardized format.

    If the postcode is at least 6 characters long, it splits the string into two parts:
    the first 4 characters and the remaining characters, separated by a space.
    If the postcode is at least 4 characters long but less than 6, it returns
    the first 4 characters only. If the postcode is shorter than 4 or invalid,
    it returns None.

    Args:
        postcode (str): The original postcode string.

    Returns:
        str: A cleaned and formatted postcode (e.g., "1234 56"), the first 4 characters
             of the postcode (e.g., "1234"), or None for invalid input.
    """
    # Check if the postcode is at least 6 characters long
    if postcode and len(postcode) >= 6:
        return f"{postcode[:4]} {postcode[4:]}"  # Format as "1234 56"

    # Check if the postcode is at least 4 characters long
    return (
        postcode[:4] if postcode and len(postcode) >= 4 else None
    )  # Return first 4 characters or None


def clean_airbnb_data(df):
    """
    Cleans the 'zipcode' column in the given DataFrame by formatting postcodes.

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame with a 'zipcode' column.

    Returns:
        pyspark.sql.DataFrame: DataFrame with an additional 'cleaned_zipcode' column.
    """
    clean_postcode_udf = udf(clean_postcode, StringType())
    return df.withColumn("cleaned_zipcode", clean_postcode_udf(col("zipcode")))
