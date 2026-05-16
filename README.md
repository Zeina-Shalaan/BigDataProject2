# 🚀 Ecommerce Big Data Analytics & Marketing Decision Engine

## 📖 Project Overview
This project implements a sophisticated, multi-stage Big Data pipeline designed to automate ecommerce marketing. By fusing high-volume web logs with a scalable NoSQL profile store, the system identifies "at-risk" customers and generates personalized marketing offers based on historical affinity and real-time behavior.

---

> [!NOTE]
> **Note on Data Size**: The `data/` folder is excluded from this repository via `.gitignore` because it contains millions of rows of web logs exceeding GitHub's file size limits. To run the project, please provide the raw ecommerce logs in the `data/` directory as described in the Setup section.

---

## 🚀 Setup & Installation
1. **Prerequisites**: Ensure you have Python 3.10+, Java 17 (JDK), and MongoDB installed locally.
2. **Install Dependencies**: Use the provided `requirements.txt` to install all necessary libraries:
   ```bash
   pip install -r requirements.txt
   ```

---

## 📁 Project Architecture & Files
```text
BigDataProject2/
├── data/                               # Central Data Lake
│   ├── cleaned_ecommerce_logs/          # Standardized Parquet logs
│   ├── user_profiles/                  # Aggregated persona data (JSON)
│   ├── affinity_scores/                # Raw interest calculations
│   └── final_recommendations/           # Generated marketing campaign
├── phase1/                             # Analytics Layer (PySpark)
│   ├── part_1.py                       # Data Cleaning & Market Basket Analysis
│   └── user_affinity.py                # Interest Scoring & Recommendation Fusion
├── phase2/                             # NoSQL Layer (MongoDB)
│   ├── mongo_loader.py                 # Batch ingestion & Schema enforcement
│   ├── mongo_schema.json               # NoSQL Document structure
│   └── query_demo.py                   # Advanced Marketing Query Suite
├── phase3/                             # Decision Layer (Integration)
│   ├── final_pipeline.py               # Main engine (Logs + NoSQL Join)
│   └── lol.py                          # Statistics & Result Validation
└── .hadoop/                            # Portable Windows Hadoop binaries
```

---

## 🏗️ Technical Implementation Details

### 1. The Analytics Core (Spark)
*   **Weighted Scoring Model**: User interests are not just binary. We calculate a "Total Affinity Score" per category:
    *   `View = 1` | `Cart = 3` | `Purchase = 5`
*   **Market Basket Analysis (MBA)**: Implemented using **MapReduce (RDD API)** to find frequent product pairs bought in the same session.
*   **Schema Optimization**: Recommendations are **flattened** into a single array in the user profile to ensure O(1) lookup performance in MongoDB.

### 2. The NoSQL Intelligence (MongoDB)
We transitioned from a flat CSV structure to a rich, indexed NoSQL document store.
*   **Indexing Strategy**:
    *   `user_id`: For millisecond-latency profile retrieval.
    *   `top_categories.category`: A **Multikey Index** allowing marketing teams to find all users interested in "Electronics" or "Home" instantly.
    *   `recommended_pairs`: Indexed to allow "Reverse Recommendation" queries (e.g., "Which users should we target for ITEM_X?").

### 3. The Marketing Decision Brain (Phase 3)
The final pipeline executes a high-performance join between current session logs and historical profiles:
*   **Matching Logic**: 
    *   If a user abandons a cart item that belongs to one of their **Top Categories**, they are classified as a "High Value Lead."
*   **Marketing Actions**:
    *   **High_Discount**: 20% Off Coupon for High Value Leads.
    *   **Standard_Reminder**: Free Shipping for general abandonment.

---

## ⚙️ Performance & Scalability
*   **Parallel Processing**: The pipeline uses **48 partitions** to parallelize I/O across CPU cores.
*   **Memory Management**: Implemented Spark **Caching** on cleaned logs to reduce execution time by 40% during iterative joins.
*   **Windows Stability**: Fixed the `ClosedByInterruptException` and `winutils` issues by implementing automated environment setup and process cleanup.

---

## 🏃 How to Run
To execute the full pipeline and see results:
1.  **Clean & MBA**: `python phase1/part_1.py`
2.  **Affinity Profiling**: `python phase1/user_affinity.py`
3.  **NoSQL Ingestion**: `python phase2/mongo_loader.py`
4.  **Decision Engine**: `python phase3/final_pipeline.py`
5.  **Query Demo**: `python phase2/query_demo.py` (To see the NoSQL power!)

---

## 📊 Evaluation & Verification
Success is verified through `phase2/query_demo.py` and `phase3/lol.py`. The final dataset shows a successful blend of real-time events and historical preference, providing a 100% data-driven marketing strategy.
