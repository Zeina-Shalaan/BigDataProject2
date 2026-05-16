import os
import sys

# Use the project root and preserve the user's existing Java/Hadoop environment when available.
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PARQUET_PATH = os.path.join(ROOT_DIR, "data", "cleaned_ecommerce_logs.parquet")
OUTPUT_DIR = os.path.join(ROOT_DIR, "abandonment_outputs")

if not os.environ.get("JAVA_HOME"):
    java_home = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
    if os.path.isdir(java_home):
        os.environ["JAVA_HOME"] = java_home

if not os.environ.get("HADOOP_HOME"):
    hadoop_home = r"C:\hadoop"
    if os.path.isdir(hadoop_home):
        os.environ["HADOOP_HOME"] = hadoop_home

# Normalize HADOOP_HOME if it points directly to the bin directory
hadoop_home = os.environ.get("HADOOP_HOME")
if hadoop_home and hadoop_home.lower().endswith(os.path.join("hadoop", "bin").lower()):
    normalized_home = os.path.dirname(hadoop_home)
    if os.path.isdir(normalized_home):
        os.environ["HADOOP_HOME"] = normalized_home

if os.environ.get("HADOOP_HOME"):
    hadoop_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
    if os.path.isdir(hadoop_bin) and hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] += os.pathsep + hadoop_bin

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("CartAbandonment") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# Load parquet (NO header / inferSchema needed)
df = spark.read.parquet(PARQUET_PATH)

# df.show()

# Split events
cart_df = df.filter(df.event_type == "cart")
purchase_df = df.filter(df.event_type == "purchase")

# Abandoned carts = in cart but NOT purchased
abandoned_df = cart_df.join(
    purchase_df,
    on=["user_id", "session_id", "product_id"],
    how="left_anti"
)

# abandoned_df.show()

print(f"Total abandoned carts: {abandoned_df.count()}")

# Save output
abandoned_df.write.csv(
    OUTPUT_DIR,
    header=True,
    mode="overwrite"
)

spark.stop()