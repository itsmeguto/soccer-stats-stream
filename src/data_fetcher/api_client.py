import requests
import json
from datetime import datetime
from kafka import KafkaProducer

def fetch_epl_scoreboard():
    url = "https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

def parse_match_data(data):
    matches = []
    for event in data.get('events', []):
        match = {
            'id': event.get('id'),
            'name': event.get('name'),
            'date': event.get('date'),
            'status': event.get('status', {}).get('type', {}).get('description'),
            'venue': event.get('venue', {}).get('displayName'),
            'competitors': []
        }
        for competitor in event.get('competitions', [])[0].get('competitors', []):
            team_data = {
                'id': competitor.get('id'),
                'name': competitor.get('team', {}).get('displayName'),
                'score': competitor.get('score'),
                'homeAway': competitor.get('homeAway')
            }
            # Add additional statistics if available
            if 'statistics' in competitor:
                team_data['statistics'] = competitor['statistics']
            match['competitors'].append(team_data)
        matches.append(match)
    return matches

def send_to_kafka(topic, data):
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        producer.send(topic, data)
        producer.flush()
        print(f"Data sent to Kafka topic '{topic}'")
    except Exception as e:
        print(f"Error sending data to Kafka: {str(e)}")

def main():
    data = fetch_epl_scoreboard()
    if data:
        parsed_data = parse_match_data(data)
        print(f"Fetched data for {len(parsed_data)} matches at {datetime.now()}")
        for match in parsed_data:
            print(f"Match: {match['name']} - Status: {match['status']}")
            for team in match['competitors']:
                print(f"  {team['name']}: {team['score']}")
            
            # Send each match data to Kafka
            send_to_kafka("epl_matches", match)
    else:
        print("Failed to fetch data from the API")

if __name__ == "__main__":
    main()