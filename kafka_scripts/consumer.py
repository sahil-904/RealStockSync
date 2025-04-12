from kafka import KafkaConsumer
import json
from pymongo import MongoClient

# Initialize MongoDB Connection
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["stock_data"]
collection = db["gail_stock"]

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    "gail_stock_data",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("📥 Consumer started, waiting for messages...")

# Consume Messages
for message in consumer:
    data = message.value
    print(f"✅ Received: {data}")  # Debugging step

    # Ensure data is correctly formatted
    if data and all(k in data for k in ["timestamp", "ticker", "open", "high", "low", "close", "volume"]):
        # Insert data into MongoDB
        collection.insert_one(data)
        print("✅ Data stored in MongoDB")
    else:
        print("⚠️ Invalid data format, skipping...")
