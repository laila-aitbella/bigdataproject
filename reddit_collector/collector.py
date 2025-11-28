#Script qui lit Reddit et envoie vers Kafka.
from kafka import KafkaProducer
import time
import json

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    msg = {"source": "reddit_collector", "test": "hello reddit"}
    producer.send("reddit", msg)
    print("Sent reddit test message")
    time.sleep(5)
