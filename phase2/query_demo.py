from pymongo import MongoClient
import json

# Setup connection
client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_db"]
collection = db["user_profiles"]

print("="*40)
print("   MONGODB MARKETING QUERY DEMO")
print("="*40)

# 1. Query by User ID
test_user = "User_1"
print(f"\n[1] Querying Profile for: {test_user}")
result = collection.find_one({"user_id": test_user})
if result:
    print(json.dumps(result, indent=2, default=str))

# 2. Query by Category Interest
test_category = "Home"
print(f"\n[2] Finding top users interested in: {test_category}")
results = collection.find({"top_categories.category": test_category}).limit(3)
for doc in results:
    score = next(item['score'] for item in doc['top_categories'] if item['category'] == test_category)
    print(f"- User: {doc['user_id']} (Interest Score: {score})")

# 3. Query by Item Recommendation (Finding target audience for an item)
# This searches inside the FLATTENED recommended_pairs array
test_item = "ITEM_1874"
print(f"\n[3] Finding users who received recommendation for: {test_item}")
# With a flat array, MongoDB query is simple and lightning fast!
query = {"recommended_pairs": test_item}
recs = collection.find(query).limit(5)

found = False
for doc in recs:
    found = True
    print(f"- Target User: {doc['user_id']}")

if not found:
    print(f"No active recommendations found for {test_item} in current sample.")

print("\n" + "="*40)
print("   Demo Complete!")
print("="*40)