from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

def clean_postcode(postcode):
    return postcode[:4] if postcode and len(postcode) >= 4 else None

def clean_airbnb_data(df):
    clean_postcode_udf = udf(clean_postcode, StringType())
    return df.withColumn("cleaned_zipcode", clean_postcode_udf(col("zipcode")))