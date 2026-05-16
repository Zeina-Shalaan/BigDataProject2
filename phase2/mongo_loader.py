from pymongo import MongoClient
import json
import os

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Create database
db = client["ecommerce_db"]

# Create collection
collection = db["user_profiles"]

# Clear any existing data so we don't have duplicates
db.drop_collection("user_profiles")

# Open JSON files from the partitioned output directory
profiles = []
dir_path = os.path.join(os.path.dirname(__file__), "../data/user_profiles")

# Read all partitioned json files created by Spark
for filename in os.listdir(dir_path):
    if filename.endswith(".json"):
        file_path = os.path.join(dir_path, filename)
        with open(file_path, "r") as file:
            for line in file:
                profile = json.loads(line)
                # remove _id completely
                profile.pop("_id", None)
                profiles.append(profile)

# Ensure list format
if isinstance(profiles, dict):
    profiles = [profiles]



collection.create_index("user_id")
collection.create_index("top_categories.category")
collection.create_index("recommended_pairs")

# Insert documents
collection.insert_many(profiles)

print("User profiles inserted successfully!")