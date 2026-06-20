from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "transjakarta-raw",
    "bmkg-raw",
    "events-raw",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest"
)

print("Listening all topics...")

for message in consumer:
    print(f"\nTOPIC: {message.topic}")
    print(message.value)