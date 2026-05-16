import os
import sys
import urllib.request
import ssl
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def setup_windows_hadoop():
    """Workaround for PySpark on Windows to avoid the HADOOP_HOME/winutils error."""
    if os.name == 'nt':
        print("Setting up Windows Hadoop environment (winutils)...")
        hadoop_home = os.path.abspath(".hadoop")
        bin_dir = os.path.join(hadoop_home, "bin")
        os.makedirs(bin_dir, exist_ok=True)
        
        winutils_path = os.path.join(bin_dir, "winutils.exe")
        hadoop_dll_path = os.path.join(bin_dir, "hadoop.dll")
        
        # Disable SSL verification just in case of local network issues
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        # Download hadoop 3.2.0 binaries which are generally very stable for PySpark 3.x
        base_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.0/bin/"
        
        try:
            if not os.path.exists(winutils_path):
                print("Downloading winutils.exe...")
                with urllib.request.urlopen(base_url + "winutils.exe", context=ctx) as response, open(winutils_path, 'wb') as out_file:
                    out_file.write(response.read())
            
            if not os.path.exists(hadoop_dll_path):
                print("Downloading hadoop.dll...")
                with urllib.request.urlopen(base_url + "hadoop.dll", context=ctx) as response, open(hadoop_dll_path, 'wb') as out_file:
                    out_file.write(response.read())
        except Exception as e:
            print(f"Warning: Failed to download winutils: {e}")
            
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["PATH"] += os.pathsep + bin_dir
        print("Hadoop environment set successfully.")

if __name__ == "__main__":
    print("Starting Final Integration Pipeline (Phase 3)...")
    
    # 1. Setup Windows Environment
    setup_windows_hadoop()
    
    # 2. Initialize Spark with MongoDB connector
    # We use mongo-spark-connector for Spark 4.x
    print("Initializing PySpark Session with MongoDB connector...")
    spark = SparkSession.builder \
        .appName("Phase3_Marketing_Pipeline") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.3.0") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # 3. Read Raw Logs and Filter for Abandoned Carts (MapReduce / DataFrame API)
    print("Reading raw logs to filter for abandoned carts...")
    raw_logs_path = "data/cleaned_ecommerce_logs.parquet"
    raw_logs = spark.read.parquet(raw_logs_path)
    
    # Identify sessions that ended in a purchase
    sessions_with_purchase = raw_logs.filter(F.col("event_type") == "purchase") \
                                     .select("session_id").distinct()
    
    # Get all cart events
    cart_events = raw_logs.filter(F.col("event_type") == "cart")
    
    # Anti-Join: Keep carts that belong to sessions WITHOUT a purchase
    abandoned_carts = cart_events.join(sessions_with_purchase, on="session_id", how="left_anti")
    print(f"Abandoned carts filtered.")

    # 4. Read User Profiles from NoSQL (MongoDB)
    print("Querying historical user profiles from MongoDB...")
    user_profiles = spark.read.format("mongodb") \
        .schema("user_id STRING, top_categories ARRAY<STRUCT<category: STRING, score: INT>>") \
        .option("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/ecommerce_db.user_profiles") \
        .load()
    
    # Based on the latest data in MongoDB, top_categories is an array of structs: 
    # [{'category': 'Home', 'score': 101}, ...]
    # We simply extract the 'category' field from each struct!
    user_top_categories = user_profiles.withColumn(
        "top_categories_list", 
        F.expr("transform(top_categories, x -> x.category)")
    ).select("user_id", "top_categories_list")

    # 5. Join & Enrichment Pipeline (Decision Engine)
    print("Joining abandoned carts with user profiles...")
    joined_df = abandoned_carts.join(user_top_categories, on="user_id", how="left")
    
    print("Applying marketing logic...")
    final_df = joined_df.withColumn("marketing_action", F.lit("Send cart recovery email")) \
                        .withColumn(
                            "discount_offer",
                            F.when(
                                # If the abandoned item's category is in the user's top categories list
                                F.array_contains(F.col("top_categories_list"), F.col("category")),
                                F.lit("High_Discount")
                            ).otherwise(F.lit("Standard_Reminder"))
                        )

    # Clean up the array column for CSV/Parquet export
    final_df = final_df.drop("top_categories_list")

    # 6. Output Generation
    output_path = "data/final_marketing_recommendations.parquet"
    print(f"Writing final campaign dataset as Parquet to: {output_path}")
    
    # The requirement asks for exactly 48 partitions and saving as parquet
    final_df.repartition(48).write.mode("overwrite").parquet(output_path)
    
    print("Phase 3 complete! Marketing pipeline finished successfully.")
    spark.stop()
