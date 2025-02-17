from pymongo import MongoClient

def process_raw_data():
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")  # Adjust if using a different host/port
    db = client["epl_database"]  # Database name

    # Collections
    raw_collection = db["raw_matches"]
    processed_collection = db["processed_matches"]

    # Fetch all raw matches
    raw_matches = raw_collection.find()
    for raw_match in raw_matches:
        try:
            # Extract relevant fields
            match_id = raw_match.get("id")  # Unique identifier for deduplication
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
                "match_id": match_id,
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
            print(f"❌ Error processing match {raw_match.get('id')}: {e}")
