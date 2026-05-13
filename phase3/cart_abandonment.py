from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CartAbandonment") \
    .master("local[*]") \
    .getOrCreate()

# Load parquet (NO header / inferSchema needed)
df = spark.read.parquet("../data/cleaned_ecommerce_logs.parquet")

# df.show()

# Split events
cart_df = df.filter(df.event == "cart")
purchase_df = df.filter(df.event == "purchase")

# Abandoned carts = in cart but NOT purchased
abandoned_df = cart_df.join(
    purchase_df,
    on=["user_id", "session_id", "product"],
    how="left_anti"
)

# abandoned_df.show()

print(f"Total abandoned carts: {abandoned_df.count()}")

# Save output
abandoned_df.write.csv(
    "abandonment_outputs",
    header=True,
    mode="overwrite"
)

spark.stop()