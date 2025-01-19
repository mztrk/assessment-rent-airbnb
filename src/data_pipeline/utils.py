def save_to_delta(df, path):
    df.write.format("delta").mode("overwrite").save(path)