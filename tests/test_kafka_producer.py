from kafka import KafkaProducer
from pymongo import MongoClient
import requests
import json

def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def store_in_mongodb(data):
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")  # Adjust if using a different host/port
    db = client["epl_database"]  # Database name
    collection = db["epl_database"]  # Collection name

    # Insert data into the collection
    result = collection.insert_one(data)
    print(f"✅ Data inserted into MongoDB with ID: {result.inserted_id}")

def send_message(producer, topic, message):
    # Send message to Kafka
    producer.send(topic, message)
    producer.flush()
    print(f"Message sent to topic '{topic}': {message}")

    # Store the same message in MongoDB
    store_in_mongodb(message)

def fetch_api_data():
    url = "https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

def test_kafka_producer():
    producer = create_producer()

    # Fetch data from the API
    api_data = fetch_api_data()
    if not api_data:
        print("No data fetched from the API. Exiting.")
        return

    # Send and store each match from the API response
    for match in api_data.get("events", []):  # Adjust based on the API's JSON structure
        send_message(producer, 'epl-matches', match)

if __name__ == "__main__":
    test_kafka_producer()
