import os
import redis
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
# Get all keys
keys = r.keys("*")
print("Keys in Redis:", keys)

for key in keys:
    key_type = r.type(key).decode() if isinstance(r.type(key), bytes) else r.type(key)

    if key_type == "string":
        value = r.get(key)
    elif key_type == "list":
        value = r.lrange(key, 0, -1)
    elif key_type == "set":
        value = list(r.smembers(key))
    elif key_type == "hash":
        value = r.hgetall(key)
    else:
        value = f"(Unhandled type: {key_type})"

    print(f"{key} ({key_type}) => {value}")

