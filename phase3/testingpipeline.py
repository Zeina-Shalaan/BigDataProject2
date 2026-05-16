import os
import sys
from pyspark.sql import SparkSession

# Environment Setup
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Use the winutils downloaded by final_pipeline.py
hadoop_home = os.path.abspath(".hadoop")
if os.path.exists(hadoop_home):
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] += os.pathsep + os.path.join(hadoop_home, "bin")

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/final_marketing_recommendations.parquet")
df.show(20)
# See how many High Discounts were offered
df.groupBy("discount_offer").count().show()
