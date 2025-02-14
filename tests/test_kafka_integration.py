from kafka import KafkaConsumer
import json

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
        break  # Exit after receiving the first message for testing purposes

if __name__ == "__main__":
    test_kafka_integration()

