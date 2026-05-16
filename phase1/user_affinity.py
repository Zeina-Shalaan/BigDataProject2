import os
import sys

# PySpark Environment Setup for Windows
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Start spark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder \
    .appName("UserAffinityAnalytics") \
    .master("local[*]") \
    .getOrCreate()

print("Spark session started!")

# Read cleaned parquet dataset
df = spark.read.parquet("data/cleaned_ecommerce_logs.parquet")

print("Dataset loaded!")

# Add weighted scores
df = df.withColumn(
    "score",
    F.when(F.col("event_type") == "view", 1) 
     .when(F.col("event_type") == "cart", 3)
     .when(F.col("event_type") == "purchase", 5)
)

# Aggregate user preferences
user_preferences = df.groupBy("user_id", "category") \
    .agg(F.sum("score").alias("total_score"))

# Rank user preferences
structured_df = user_preferences.withColumn(
    "score_category_struct", 
    F.struct(F.col("total_score"), F.col("category"))
)

grouped_profiles = structured_df.groupBy("user_id").agg(
    F.sort_array(F.collect_list("score_category_struct"), asc=False).alias("sorted_categories")
)

# Format top_categories as an array of structs
final_profiles = grouped_profiles.withColumn(
    "top_categories",
    F.expr("transform(sorted_categories, x -> struct(x.category as category, x.total_score as score))")
).drop("sorted_categories")

# ==========================================
# Market Basket Integration (Flattened)
# ==========================================
recs_path = "data/market_basket_recommendations.csv"
if os.path.exists(recs_path):
    print("Integrating Market Basket Recommendations (Flattened)...")
    recs_df = spark.read.csv(recs_path, header=True, inferSchema=True)
    
    # Get product -> category mapping
    prod_cat_map = df.select(F.col("product_id").alias("item_1"), F.col("category").alias("item_category")).distinct()
    
    # Join recommendations with categories
    recs_with_cat = recs_df.join(prod_cat_map, on="item_1")
    
    # Get top 2 recommendation pairs per category and FLATTEN them
    window = Window.partitionBy("item_category").orderBy(F.col("frequency").desc())
    category_recommendations = recs_with_cat.withColumn("rn", F.row_number().over(window)) \
        .filter(F.col("rn") <= 2) \
        .groupBy("item_category") \
        .agg(F.flatten(F.collect_list(F.array("item_1", "item_2"))).alias("recommended_pairs"))

    # Extract primary category
    final_profiles = final_profiles.withColumn("primary_category", F.col("top_categories")[0].category)
    
    # Attach recommendations
    final_profiles = final_profiles.join(
        category_recommendations, 
        final_profiles.primary_category == category_recommendations.item_category, 
        "left"
    ).drop("item_category", "primary_category")
else:
    print("Warning: Market Basket Recommendations not found.")
    final_profiles = final_profiles.withColumn("recommended_pairs", F.array())

# Handle nulls
final_profiles = final_profiles.withColumn(
    "recommended_pairs", 
    F.coalesce(F.col("recommended_pairs"), F.array())
)

print("Final user profiles with flattened recommendations:")
final_profiles.show(5, truncate=False)

# Export results
user_preferences.repartition(48).write.mode("overwrite").parquet("data/affinity_scores")
final_profiles.repartition(48).write.mode("overwrite").json("data/user_profiles")

print("User profiles exported successfully!")
spark.stop()
