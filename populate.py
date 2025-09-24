import redis
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# read values from .env
host = os.getenv("REDIS_HOST")
port = int(os.getenv("REDIS_PORT"))
username = os.getenv("REDIS_USERNAME")
password = os.getenv("REDIS_PASSWORD")

# connect to redis
r = redis.Redis(
    host=host,
    port=port,
    username=username,
    password=password,
    decode_responses=True
)

# Read the JSON file
with open("institutes.json", "r", encoding="utf-8") as f:
    data = json.load(f)

institutions = data["institution"]
companies = data["companies"]

# Merge institutions into companies (preserve order, remove duplicates)
# institutions will be appended after existing companies, but we remove duplicates
companies = list(dict.fromkeys(companies + institutions))

# Clear old keys
r.delete("institutions")
r.delete("companies")

# Push lists into Redis

for inst in institutions:
    r.rpush("institutions", inst)

for company in companies:
    r.rpush("companies", company)

print("Inserted", len(institutions), "institutions into Redis")
print("Inserted", len(companies), "companies into Redis")
