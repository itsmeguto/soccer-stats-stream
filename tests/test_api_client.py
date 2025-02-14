import unittest
from src.data_fetcher.api_client import fetch_epl_scoreboard, parse_match_data

class TestApiClient(unittest.TestCase):

    def test_fetch_epl_scoreboard(self):
        data = fetch_epl_scoreboard()
        self.assertIsNotNone(data, "API response is None")
        self.assertIn("events", data, "Key 'events' not found in API response")

    def test_parse_match_data(self):
        sample_data = {
            "events": [
                {
                    "id": "12345",
                    "name": "Team A vs Team B",
                    "date": "2025-02-14T15:00Z",
                    "status": {"type": {"description": "In Progress"}},
                    "venue": {"displayName": "Stadium X"},
                    "competitions": [
                        {
                            "competitors": [
                                {
                                    "id": "1",
                                    "team": {"displayName": "Team A"},
                                    "score": "2",
                                    "homeAway": "home"
                                },
                                {
                                    "id": "2",
                                    "team": {"displayName": "Team B"},
                                    "score": "1",
                                    "homeAway": "away"
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        parsed_data = parse_match_data(sample_data)
        self.assertEqual(len(parsed_data), 1, "Parsed data does not contain the expected number of matches")
        self.assertEqual(parsed_data[0]['name'], "Team A vs Team B", "Match name does not match expected value")
        self.assertEqual(parsed_data[0]['competitors'][0]['name'], "Team A", "Team A data is incorrect")
        self.assertEqual(parsed_data[0]['competitors'][1]['score'], "1", "Team B score is incorrect")

if __name__ == "__main__":
    unittest.main()
