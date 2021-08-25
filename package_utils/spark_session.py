from pyspark.sql import SparkSession

__all__ = ["spark"]
# try to initialize the spark object
try:
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Maersk Assignment by Sajal") \
        .getOrCreate()
except Exception as e:
    print("Error occured while trying to create the spark session")
    raise Exception(f"{e}")
