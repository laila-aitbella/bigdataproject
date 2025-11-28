from kafka import KafkaConsumer
import json
from pymongo import MongoClient

# Connexion à Kafka
consumer = KafkaConsumer(
    'finance',  # Topic à écouter
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',  # lire depuis le début
    group_id='finance_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Sentiment Service démarré, en attente des messages...")

# Connexion à MongoDB
mongo_client = MongoClient("mongodb://mongodb:27017")
db = mongo_client["finance_db"]
collection = db["finance_messages"]

# Boucle pour consommer les messages
for message in consumer:
    data = message.value
    print(f"Reçu message: {data}")
    # Stockage dans MongoDB
    collection.insert_one(data)
