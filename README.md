# Ecommerce Analytics Pipeline - Phase 1

This repository contains the Phase 1 Big Data pipeline for processing and analyzing an e-commerce logs dataset (10.8 million records) using PySpark.

## Overview of `part 1.ipynb`
This notebook handles the initial Spark infrastructure, data cleaning, and Market Basket Analysis. Here is a summary of the steps completed:

1. **Spark Infrastructure Setup**
   - Configured `SparkSession` to run locally utilizing all available cores (`local[*]`, 12 cores).
   - Resolved Windows-specific PySpark I/O errors (`winutils.exe` and `hadoop.dll`).
   - Applied specific configurations to bypass Windows file-locking issues (`mapreduce.fileoutputcommitter.algorithm.version = 2`).

2. **Data Ingestion & Parsing**
   - Loaded the raw `ecommerce_logs.csv`.
   - Successfully parsed complex, multiline JSON fields (`user_metadata`, `product_metadata`) by properly escaping quotes.
   - Extracted the `category`, `brand`, and `device` attributes directly into their own DataFrame columns.

3. **Data Cleaning**
   - Dropped invalid rows containing `Null` values in critical fields (`session_id`, `user_id`, `product_id`, `event_type`, `timestamp`, `category`).
   - Filtered out anomalous records (e.g., `price <= 0`).
   - The cleaned dataset was reduced from ~10.8M to exactly **7,107,117** highly-usable records.

4. **Performance Optimization**
   - Implemented `.cache()` on the main dataframe to prevent re-evaluating the complex JSON parsing logic.
   - Repartitioned the data to 48 partitions by `session_id` (4x the available cores) to heavily optimize shuffle operations for downstream tasks.

5. **Market Basket Analysis (MapReduce)**
   - Filtered exclusively for `purchase` events.
   - Implemented a custom RDD-based MapReduce pipeline:
     - **Map**: Emitted `(session_id, product_id)` pairs.
     - **Reduce**: Grouped items by session, generated all possible co-purchased pairs using `itertools.combinations`, and calculated global frequencies.
   - Filtered for item pairs purchased together more than 10 times and sorted by most frequent.
   - Exported the final recommendations to `data/market_basket_recommendations.csv`.

6. **Data Persistence**
   - Saved the 7.1M cleaned records as a compressed `.parquet` file for extremely fast loading by the rest of the team.

---

## Instructions for Team Members

To ensure you are working with the clean, validated data (and to save yourself 5-10 minutes of processing time on every run!), **do NOT load the raw CSV file.** 

Instead, you should load the **Parquet** file that was generated at the end of Phase 1. 

### How to Load the Cleaned Data
Parquet files are heavily compressed and perfectly memorize their schemas (data types). You can load the entire 7.1 million row dataset instantly using this single line of PySpark code:

```python
# 1. Load the perfectly cleaned and parsed dataframe
cleaned_df = spark.read.parquet("../data/cleaned_ecommerce_logs.parquet")

# 2. Verify it loaded correctly
cleaned_df.printSchema()
cleaned_df.show(5)
```

### Notes for Members 2 & 3:
* The `cleaned_df` already has the `category`, `brand`, and `device` columns fully extracted and ready to use. You do not need to deal with the JSON strings!
* The `price` column is already cast to a `double`, and `timestamp` is a valid Spark Timestamp.
* All anomalous data (negative prices, missing user IDs) has been removed. You can proceed straight to advanced analytics, aggregations, or machine learning!
