# (test minimal qui lit kafka et écrit dans Mongo)
from kafka import KafkaConsumer
from pymongo import MongoClient
import json

consumer = KafkaConsumer(
    "reddit", "finance",
    bootstrap_servers="kafka:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

client = MongoClient("mongodb://mongodb:27017")
db = client["streaming_db"]
collection = db["messages"]

for msg in consumer:
    data = msg.value
    print("Received:", data)
    collection.insert_one(data)
