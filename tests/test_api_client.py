import requests
import json
from pymongo import MongoClient

def fetch_epl_data():
    url = "https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard"  # Replace with actual API URL
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

def store_in_mongodb(data):
    if not data:
        print("No data to store.")
        return

    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")  # Adjust if using a different host/port
    db = client["epl_database"]  # Database name
    collection = db["epl_database"]  # Collection name

    # Insert data into the collection
    result = collection.insert_one(data)
    print(f"Data inserted with ID: {result.inserted_id}")

def main():
    # Fetch EPL match data from API
    raw_data = fetch_epl_data()

    # Store raw data in MongoDB
    store_in_mongodb(raw_data)

if __name__ == "__main__":
    main()
