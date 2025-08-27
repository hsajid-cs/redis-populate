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
with open("data.json", "r", encoding="utf-8") as f:
    data = json.load(f)

degrees = data["degree"]
institutions = data["institution"]
roles = data["role"]
companies = data["companies"]

# Merge institutions into companies (preserve order, remove duplicates)
# institutions will be appended after existing companies, but we remove duplicates
companies = list(dict.fromkeys(companies + institutions))

# Clear old keys
r.delete("degrees")
r.delete("institutions")
r.delete("roles")
r.delete("companies")

# Push lists into Redis
for degree in degrees:
    r.rpush("degrees", degree)

for inst in institutions:
    r.rpush("institutions", inst)

for role in roles:
    r.rpush("roles", role)

for company in companies:
    r.rpush("companies", company)

print("Inserted", len(degrees), "degrees and", len(institutions), "institutions into Redis")
print("Inserted", len(roles), "roles and", len(companies), "companies into Redis")
