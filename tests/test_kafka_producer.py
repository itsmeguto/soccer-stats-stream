# tests/test_kafka_producer.py

from kafka import KafkaProducer
import json

def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def send_test_message(producer, topic, message):
    producer.send(topic, message)
    producer.flush()
    print(f"Message sent to topic '{topic}': {message}")

def test_kafka_producer():
    producer = create_producer()
    test_message = {"match_id": "12345", "team_a": "Team A", "team_b": "Team B", "score": "2-1"}
    send_test_message(producer, 'epl-matches', test_message)

if __name__ == "__main__":
    test_kafka_producer()
