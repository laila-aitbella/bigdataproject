#Script qui lit Yahoo Finance.
from kafka import KafkaProducer
import time
import json
# attendre kafka
time.sleep(20)  # attendre kafka

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    msg = {"source": "finance_collector", "price": 123}
    producer.send("finance", msg)
    print("Sent finance test message")
    time.sleep(5)
