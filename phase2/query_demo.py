from pymongo import MongoClient
import json

client = MongoClient("mongodb://localhost:27017/")

db = client["ecommerce_db"]
collection = db["user_profiles"]

user_id = "User_10905"

result = collection.find_one({"user_id": user_id})
if result:
    print("\nFound user document:")
    print(json.dumps(result, indent=2, default=str))
else:
    print(f"\nNo user found with user_id: {user_id}")


#collection.find()    this prints all 

# this finds first 3 , and also specfic user id
#from pymongo import MongoClient
#import json

#client = MongoClient("mongodb://localhost:27017/")

#db = client["ecommerce_db"]
#collection = db["user_profiles"]

# First, check if collection has any documents
#count = collection.count_documents({})
#print(f"Total documents in collection: {count}\n")

# Show first 3 documents
#print("First few documents:")
#for doc in collection.find().limit(3):
 #   print(json.dumps(doc, indent=2, default=str))
  #  print("---")

# Try to find specific user
#user_id = "User_10905"
#result = collection.find_one({"user_id": user_id})
#if result:
 #   print(f"\nUser {user_id}:")
  #  print(json.dumps(result, indent=2, default=str))
#else:
 #   print(f"\nNo user found with user_id: {user_id}")