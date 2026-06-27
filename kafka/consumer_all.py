from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "suroboyo-bus-live",
    "bmkg-raw",
    "events-raw",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest"
)

print("Listening all topics...")

for message in consumer:

    print("\n==============================")
    print("TOPIC :", message.topic)
    print("==============================")
    print(json.dumps(message.value, indent=4))