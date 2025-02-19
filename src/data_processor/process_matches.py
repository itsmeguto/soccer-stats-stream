from pymongo import MongoClient

def process_raw_data():
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["epl_database"]  # Database name
    raw_collection = db["epl_database"]  # Collection name
    processed_collection = db["processed_matches"]  # Processed collection name

    # Fetch all raw matches
    raw_matches = raw_collection.find()
    for raw_data in raw_matches:
        try:
            # Extract match details from the top-level fields
            match_id = raw_data.get("id")
            match_date = raw_data.get("date")
            short_name = raw_data.get("name")
            venue = raw_data.get("venue")

            # Extract competitors
            competitors = raw_data.get("competitors", [])
            home_team = next((team.get("name") for team in competitors if team.get("homeAway") == "home"), None)
            away_team = next((team.get("name") for team in competitors if team.get("homeAway") == "away"), None)

            # Handle odds (store None if not available)
            odds = raw_data.get("odds", [])
            if odds:
                win_home_team = odds[0].get("homeTeamOdds", {}).get("value")
                draw = odds[0].get("drawOdds", {}).get("value")
                win_away_team = odds[0].get("awayTeamOdds", {}).get("value")
            else:
                win_home_team = draw = win_away_team = None

            # Prepare processed match document
            processed_match = {
                "match_id": match_id,
                "date": match_date,
                "short_name": short_name,
                "home_team": home_team,
                "away_team": away_team,
                "venue": venue,
                "odds": {
                    "win_home_team": win_home_team,
                    "draw": draw,
                    "win_away_team": win_away_team,
                },
            }

            # Insert or update the processed match document (deduplication)
            result = processed_collection.update_one(
                {"match_id": match_id},  # Query to check for existing record
                {"$set": processed_match},  # Update fields if record exists
                upsert=True  # Insert if no matching record is found
            )
            
            if result.upserted_id:
                print(f"✅ New match inserted with ID: {result.upserted_id}")
            else:
                print(f"✅ Existing match updated for ID: {match_id}")

        except Exception as e:
            print(f"❌ Error processing match {raw_data.get('id')}: {e}")

    client.close()
