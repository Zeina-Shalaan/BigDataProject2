import os

os.environ["JAVA_HOME"] = r"D:\Java\JDK-17"
os.environ["HADOOP_HOME"] = r"D:\hadoop"
os.environ["PATH"] += os.pathsep + r"D:\hadoop\bin"

#Start spark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create Spark session
spark = SparkSession.builder \
    .appName("UserAffinityAnalytics") \
    .master("local[*]") \
    .getOrCreate()

print("Spark session started!")

# Read cleaned parquet dataset
df = spark.read.parquet("data/cleaned_ecommerce_logs.parquet")

print("Dataset loaded!")

#printing columns for simulaation
print("Columns:")
print(df.columns) #show all columns names

print("Sample data:")
df.show(5, truncate=False)

# Add weighted scores
#convert events into scores that can be calculated by spark
df = df.withColumn( #create a new column
    "score",
    F.when(F.col("event_type") == "view", 1) 
     .when(F.col("event_type") == "cart", 3)
     .when(F.col("event_type") == "purchase", 5)
)

print("Dataset with scores:")
df.select("user_id", "category", "event_type", "score").show(10, truncate=False)

# Aggregate user preferences
#Reduce and sum up data
#group by userID and Category

user_preferences = df.groupBy("user_id", "category") \
    .agg(F.sum("score").alias("total_score")) # alias renames the coloumn 

print("User preference scores:")

user_preferences.show(10, truncate=False)

# Rank user preferences
ranked_preferences = user_preferences.orderBy(
    F.col("total_score").desc()
)

print("Ranked user preferences:")

ranked_preferences.show(10, truncate=False)

# Export affinity scores
#Reduced output Dataset 
user_preferences.write.mode("overwrite").parquet(
    "data/affinity_scores"
)

print("Affinity scores exported!")

# Export ranked user profiles
ranked_preferences.write.mode("overwrite").json(
    "data/user_profiles"
)

print("User profiles exported!")

