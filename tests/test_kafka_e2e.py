import threading
import time
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pymongo import MongoClient
from test_kafka_producer import test_kafka_producer
from test_kafka_integration import test_kafka_integration
from src.data_processor.process_matches import process_raw_data
from src.data_fetcher.api_client import fetch_epl_scoreboard, parse_match_data, send_to_kafka


def run_consumer():
    test_kafka_integration()

## PRODUCER FOR TESTING WITH DUMMY DATA
##def run_producer():
##    time.sleep(2)  # Give the consumer a moment to start up
##    test_kafka_producer()

## PRODUCER FOR TESTING WITH PROD OBJECTS
def run_producer():
    time.sleep(2)  # Give the consumer a moment to start up
    data = fetch_epl_scoreboard()
    if data:
        parsed_data = parse_match_data(data)
        for match in parsed_data:
            send_to_kafka("epl-matches", match)


def check_processed_data():
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")  # Adjust if using a different host/port
    db = client["epl_database"]  # Database name
    processed_collection = db["processed_matches"]  # Collection name

    # Query the database for documents
    documents = list(processed_collection.find())
    if documents:
        print(f"✅ Processed data found in MongoDB! Total documents: {len(documents)}")
        for doc in documents:
            print(doc)  # Print each document (optional)
    else:
        print("❌ No processed data found in MongoDB!")

def test_kafka_e2e():
    consumer_thread = threading.Thread(target=run_consumer)
    producer_thread = threading.Thread(target=run_producer)

    consumer_thread.start()
    producer_thread.start()

    consumer_thread.join(timeout=10)  # Wait for up to 10 seconds
    producer_thread.join()

    # Process raw data into processed_matches collection
    process_raw_data()

    # Check processed data in MongoDB after processing
    check_processed_data()

if __name__ == "__main__":
    test_kafka_e2e()
