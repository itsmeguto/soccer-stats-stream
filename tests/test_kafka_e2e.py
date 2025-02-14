# tests/test_kafka_e2e.py

import threading
import time
from test_kafka_producer import test_kafka_producer
from test_kafka_integration import test_kafka_integration

def run_consumer():
    test_kafka_integration()

def run_producer():
    time.sleep(2)  # Give the consumer a moment to start up
    test_kafka_producer()

def test_kafka_e2e():
    consumer_thread = threading.Thread(target=run_consumer)
    producer_thread = threading.Thread(target=run_producer)

    consumer_thread.start()
    producer_thread.start()

    consumer_thread.join(timeout=10)  # Wait for up to 10 seconds
    producer_thread.join()

if __name__ == "__main__":
    test_kafka_e2e()
