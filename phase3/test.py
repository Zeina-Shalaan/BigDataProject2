from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.parquet("../data/cleaned_ecommerce_logs.parquet")

count = df.count()

with open("test_output.txt", "w") as f:
    f.write(f"Total rows: {count}\n")

spark.stop()