import threading
import time
from pymongo import MongoClient
from test_kafka_producer import test_kafka_producer
from test_kafka_integration import test_kafka_integration

def run_consumer():
    test_kafka_integration()

def run_producer():
    time.sleep(2)  # Give the consumer a moment to start up
    test_kafka_producer()

def process_raw_data():
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")  # Adjust if using a different host/port
    db = client["epl_database"]  # Database name

    # Collections
    raw_collection = db["epl_database"]
    processed_collection = db["processed_matches"]

    # Fetch all raw matches
    raw_matches = raw_collection.find()
    for raw_match in raw_matches:
        try:
            # Extract relevant fields
            date = raw_match.get("date")
            short_name = raw_match.get("shortName")
            
            # Get competition details
            competition = raw_match.get("competitions", [])[0]
            venue = competition.get("venue", {}).get("fullName")

            # Extract teams (home and away)
            competitors = competition.get("competitors", [])
            home_team = next((team.get("team", {}).get("displayName") for team in competitors if team.get("homeAway") == "home"), None)
            away_team = next((team.get("team", {}).get("displayName") for team in competitors if team.get("homeAway") == "away"), None)

            # Extract odds
            odds = competition.get("odds", [{}])
            win_home_team = odds[0].get("homeTeamOdds", {}).get("value")
            draw = odds[0].get("drawOdds", {}).get("value")
            win_away_team = odds[0].get("awayTeamOdds", {}).get("value")

            # Extract goal odds
            goal_odds = competition.get("odds", [{}])[1]
            over_odds = goal_odds.get("total", {}).get("over", {}).get("close", {}).get("odds")
            under_odds = goal_odds.get("total", {}).get("under", {}).get("close", {}).get("odds")

            # Prepare processed match document
            processed_match = {
                "date": date,
                "short_name": short_name,
                "home_team": home_team,
                "away_team": away_team,
                "venue": venue,
                "odds": {
                    "win_home_team": win_home_team,
                    "draw": draw,
                    "win_away_team": win_away_team,
                },
                "goal_odds": {
                    "over": over_odds,
                    "under": under_odds,
                },
            }

            # Insert processed match into secondary collection
            result = processed_collection.insert_one(processed_match)
            print(f"✅ Processed match inserted with ID: {result.inserted_id}")

        except Exception as e:
            print(f"❌ Error processing match {raw_match.get('id')}: {e}")

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
