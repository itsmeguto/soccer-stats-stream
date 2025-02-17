from kafka import KafkaConsumer
from pymongo import MongoClient
import json

def store_in_mongodb(data):
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")  # Adjust if using a different host/port
    db = client["epl_database"]  # Database name
    collection = db["epl_database"]  # Collection name

    # Use upsert to avoid duplicates
    result = collection.update_one(
        {"id": data.get("id")},  # Query by unique identifier
        {"$set": data},  # Update or insert the document
        upsert=True  # Insert if no matching document exists
    )

    if result.upserted_id:
        print(f"✅ New document inserted with ID: {result.upserted_id}")
    else:
        print(f"✅ Existing document updated for ID: {data.get('id')}")

def test_kafka_integration():
    consumer = KafkaConsumer(
        'epl-matches',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='test-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Listening to messages on 'epl-matches' topic...")

    for message in consumer:
        print(f"Received message: {message.value}")

        # Store the consumed message in MongoDB
        store_in_mongodb(message.value)

        break  # Exit after receiving the first message for testing purposes

if __name__ == "__main__":
    test_kafka_integration()
