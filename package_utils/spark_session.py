from pyspark.sql import SparkSession
execute = 0
__all__ = ["spark"]
# try to initialize the spark object
try:
    if execute == 0:
        spark = SparkSession.builder \
            .master("local[1]") \
            .appName("Maersk Assignment by Sajal") \
            .config("spark.executor.allowSparkContext", "true") \
            .getOrCreate()
    execute = 1
except Exception as e:
    print("Error occured while trying to create the spark session")
    raise Exception(f"{e}")
