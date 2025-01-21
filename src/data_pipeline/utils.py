def save_to_delta(df, path):
    """
    Save a DataFrame to Delta format.

    Args:
        df (DataFrame): The PySpark DataFrame to be saved.
        path (str): The file path where the DataFrame should be saved in Delta format.

    Returns:
        None
    """
    # Write the DataFrame to the specified path in Delta format, overwriting existing data
    df.write.format("delta").mode("overwrite").save(path)
