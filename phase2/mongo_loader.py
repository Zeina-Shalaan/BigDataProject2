from pymongo import MongoClient
import json

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Create database
db = client["ecommerce_db"]

# Create collection
collection = db["user_profiles"]

# Open JSON file
with open("../data/user_profiles/user_profiles.json", "r") as file:
    profiles = []

    for line in file:
        profile = json.loads(line)

        # remove _id completely
        profile.pop("_id", None)

        profiles.append(profile)

# Ensure list format
if isinstance(profiles, dict):
    profiles = [profiles]



collection.create_index("user_id")

# Insert documents
collection.insert_many(profiles)

print("User profiles inserted successfully!")