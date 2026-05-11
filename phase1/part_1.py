# **Spark setup**

import os
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"
os.environ['HADOOP_HOME'] = 'C:\\hadoop' 

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "data")

from pyspark.sql    import SparkSession
from pyspark        import SparkContext

spark= SparkSession.builder \
    .appName("EcommerceProject") \
    .master('local[*]')\
    .config("spark.driver.memory", "2g")\
    .config('spark.sql.shuffle.partitions', '8')\
    .getOrCreate()

sc= spark.sparkContext

print(f'Spark version  : {spark.version}')
print(f'Python version : {sc.pythonVer}')
print(f'App name       : {sc.appName}')
print(f'Master         : {sc.master}')
print(f'Cores available: {sc.defaultParallelism}')
print()
print('Spark UI → http://localhost:4040')
    

# Reading csv file 

rawdf=(
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("nullValue", "")
    .option('multiLine', 'true')
    .option('escape', '"')
    .csv(os.path.join(DATA_DIR, 'ecommerce_logs.csv'))
)
print(f'Shape of the raw dataframe: {rawdf.count()} rows x {len(rawdf.columns)} columns')
print()
rawdf.show(10,truncate=False)

rawdf.printSchema()
print()


# Inspecting the metdata 
# and extracting categories as well 

rawdf.select(
"user_metadata",
    "product_metadata"
).show(5, truncate=False)

import pyspark.sql.functions as F

print('Null counts per column:')
null_counts = rawdf.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in rawdf.columns
])
null_counts.show()

import pyspark.sql.functions as F

#print("Original row count:", rawdf.count())

# 1. Extract JSON fields into their own columns
parsed_df = rawdf.withColumn(
    "category", F.get_json_object(F.col("product_metadata"), "$.category")
).withColumn(
    "brand", F.get_json_object(F.col("product_metadata"), "$.brand")
).withColumn(
    "device", F.get_json_object(F.col("user_metadata"), "$.device")
)

# 2. Data Cleaning
cleaned_df = (
    parsed_df
    # Handle Null Values & Missing Categories (Keep rows where critical fields are NOT null)
    .dropna(subset=["session_id", "user_id", "product_id", "event_type", "timestamp", "category","user_metadata"])
    # Handle Incorrect Timestamps & Invalid Records (e.g., price must be > 0)
    .filter(F.col("timestamp").isNotNull())
    .filter(F.col("price") > 0)
)
print("dropped row count:", rawdf.count()-cleaned_df.count())


# Show the new schema and a few rows
cleaned_df.show(5, truncate=False)



df=cleaned_df
cleaned_df.select("event_type").distinct().show()

# Lets start basket Analysis phase 1.1

# 4.4 Event Separation
views_df = cleaned_df.filter(F.col("event_type") == "view")
cart_df = cleaned_df.filter(F.col("event_type") == "cart")
purchase_df = cleaned_df.filter(F.col("event_type") == "purchase")

total   =views_df.count()+cart_df.count()+ purchase_df.count()
# Let's verify how many rows we have for each event!
print("Total views:", views_df.count())
print("Total carts:", cart_df.count())
print("Total purchases:", purchase_df.count())
print("Total Count:",total ,total==cleaned_df.count() )


# Checking partions

print(f'Default partitions: {cleaned_df.rdd.getNumPartitions()}')

# spark.sql.shuffle.partitions controls post-shuffle partitions (groupBy, join)
print(f'Shuffle partitions: {spark.conf.get("spark.sql.shuffle.partitions")}')

# For small data, 200 (default) is wasteful — we set 8 at startup
# For large data on a real cluster: num_cores × 2-4 is a good starting point

# Count records per partition
print('\nRecords per partition:')
part_sizes = cleaned_df.rdd.mapPartitionsWithIndex(
    lambda idx, it: [(idx, sum(1 for _ in it))]
).collect()
for pidx, cnt in part_sizes:
    bar = '#' * (cnt // 10)
    print(f'  Partition {pidx}: {cnt:>4} rows  {bar}')

# 4.5 Performance Optimization

# Use 48 partitions (12 cores * 4) for optimal parallel processing
num_partitions = 48

# 1. Optimize the main dataframe 
cleaned_df = cleaned_df.repartition(num_partitions)
cleaned_df.cache()

# 2. Optimize YOUR specific dataframe for Market Basket Analysis (Partitioned specifically by session)
optimized_purchase_df = purchase_df.repartition(num_partitions, "session_id")
optimized_purchase_df.cache()

# Force Spark to execute the caching into memory
print("Total clean events cached :", cleaned_df.count())

# *Map*

import itertools
purchase_rdd = optimized_purchase_df.select("session_id", "product_id").rdd
# MAP STEP: Emit (session_id, item_id)
mapped_rdd = purchase_rdd.map(lambda x: (x.session_id, x.product_id))

#REDUCE STEP 1: Group by Session
# Group by key (session_id) and convert the items into list
grouped_rdd = mapped_rdd.groupByKey().mapValues(lambda items: list(set(items)))

# 4.7 / 4.8 REDUCE STEP 2: Generate Product Pairs
def generate_pairs(items):
    # Sort items so (Item A, Item B) is the same as (Item B, Item A)
    sorted_items = sorted(items)
    # Generate all combinations of length 2
    return list(itertools.combinations(sorted_items, 2))

# flatMap extracts the pairs out of the session groups into a flat list of pairs
pairs_rdd = grouped_rdd.flatMap(lambda x: generate_pairs(x[1]))





# ==========================================
# 4.9 Recommendation Pair Counting
# Classic WordCount logic: map each pair to (pair, 1), then reduceByKey to add them up
pair_frequencies_rdd = pairs_rdd.map(lambda pair: (pair, 1)).reduceByKey(lambda a, b: a + b)

# Sort by the most frequently bought together pairs and print the Top 10!
top_recommendations = pair_frequencies_rdd.sortBy(lambda x: x[1], ascending=False)

print("Top 10 Most Frequently Bought Together Product Pairs:")
for pair, count in top_recommendations.take(10):
    print(f"Pair: {pair} | Bought Together: {count} times")

# ==========================================
# 1. Save the Cleaned Dataset
# ==========================================
print("Saving cleaned dataset as Parquet...")
# We use 'overwrite' so you don't get an error if you run this cell twice!
cleaned_df.write.mode("overwrite").parquet(os.path.join(DATA_DIR, "cleaned_ecommerce_logs.parquet"))
print("Cleaned data saved successfully!")

# ==========================================
# 2. Save the Market Basket Recommendations
# ==========================================
print("Saving Market Basket recommendations...")

# First, our RDD looks like: (('PS5', 'Controller'), 3200)
# We map it to flatten the pair so it looks like: ('PS5', 'Controller', 3200)
flat_pairs_rdd = pair_frequencies_rdd.map(lambda x: (x[0][0], x[0][1], x[1]))

# Convert the flattened RDD back into a beautiful PySpark DataFrame
recommendations_df = flat_pairs_rdd.toDF(["item_1", "item_2", "frequency"])

# Save it as a standard CSV with headers
recommendations_df.write.mode("overwrite").csv(os.path.join(DATA_DIR, "market_basket_recommendations.csv"), header=True)
print("Recommendations saved successfully!")




