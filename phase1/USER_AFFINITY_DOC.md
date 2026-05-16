# Phase 1: User Affinity & Profiling Documentation

### 🎯 Objective
The goal of this phase was to analyze user interactions from the cleaned ecommerce dataset and identify user preferences based on their behavior. Apache Spark was used to process the large scale dataset efficiently and generate affinity scores for different product categories.

### 💻 Technologies Used
*   **Python** (Logic & Data Manipulation)
*   **Java** (Required for Spark Runtime)
*   **Apache Spark (PySpark)** (Distributed Computing Engine)
*   **Hadoop Utilities** (Windows Filesystem Compatibility)
*   **Parquet** (Columnar Data Storage)
*   **JSON** (Flexible Schema Output for NoSQL)

### 📊 Input Dataset
The input dataset used in this phase was: `cleaned_ecommerce_logs.parquet`. This dataset was generated during the cleaning phase and contained the following critical columns:

| Column | Description |
| :--- | :--- |
| **user_id** | Unique identifier for each user |
| **event_type** | User interaction type (view, cart, purchase) |
| **category** | Product category extracted from metadata |
| **brand** | Product brand |
| **device** | User device type |

---

### Step 1 — Spark Session Initialization
A Spark session was created to initialize the distributed processing environment.
*   **Purpose**: Start Apache Spark, enable distributed Dataframe processing, and allow Spark to use all available CPU cores via `local[*]`.
*   **Code Logic**:
    ```python
    spark = SparkSession.builder \
       .appName("UserAffinityAnalytics") \
       .master("local[*]") \
       .getOrCreate()
    ```

### Step 2 — Loading the Dataset
The cleaned parquet dataset was loaded into a Spark DataFrame.
*   **Logic**: `df = spark.read.parquet("data/cleaned_ecommerce_logs.parquet")`
*   **Why Parquet**: It reduces storage size, improves query performance, and supports distributed analytics efficiently.

### Step 3 — Dataset Inspection
The dataset structure and sample rows were inspected to verify the schema and confirm that all required columns (user_id, event_type, category) existed before transformation.

### Step 4 — Weighted Event Scoring
User interactions were converted into numerical scores to measure interest levels.
*   **Event Weight Mapping**:
    *   **view**: 1 (Low Interest)
    *   **cart**: 3 (Medium Interest)
    *   **purchase**: 5 (Strong Interest)
*   **Transformation Logic**:
    ```python
    df = df.withColumn("score",
       F.when(F.col("event_type") == "view", 1)
        .when(F.col("event_type") == "cart", 3)
        .when(F.col("event_type") == "purchase", 5))
    ```

### Step 5 — User Preference Aggregation
The weighted scores were aggregated to calculate total affinity scores for each user-category combination.
*   **Aggregation Logic**: 
    ```python
    user_preferences = df.groupBy("user_id", "category").agg(F.sum("score").alias("total_score"))
    ```
*   **Purpose**: Identifies exactly how strongly a user is interested in a specific category (e.g., User_6556 has a score of 184 for "Electronics").

---

### 🧠 MapReduce Conceptual Flow
1.  **Map Phase**: User events were transformed into numerical scores (purchase → 5, cart → 3).
2.  **Shuffle Phase**: Spark grouped records by key: `groupBy("user_id", "category")`.
3.  **Reduce Phase**: Spark aggregated values using `sum("score")` to produce final affinity scores.

---

### Step 6 — Ranking & Schema Formatting
The aggregated scores were sorted and formatted into a rich profile structure.
*   **Code Logic**: We used `F.struct` to combine category and score, then `F.sort_array` to keep the most relevant interests at the top.
*   **Final Format**: `ARRAY<STRUCT<category: STRING, score: INT>>`

### Step 7 — Market Basket Integration
To enhance the profile, we joined the user's primary interest with global **Market Basket Analysis** results.
*   **Optimization**: Recommendation pairs were **flattened** into a single array to ensure lightning-fast searching in MongoDB.

### Step 8 — Exporting Results
The final datasets were exported for the NoSQL loading phase:
*   `data/affinity_scores/`: Aggregated scores in Parquet format.
*   `data/user_profiles/`: Ranked user profiles in JSON format.

---

### ✅ Final Outcome
This phase successfully:
1.  Processed large-scale ecommerce interaction data.
2.  Transformed user actions into weighted affinity scores.
3.  Generated ranked user profiles with integrated product recommendations.
4.  Exported analytics-ready datasets for the next phase.
