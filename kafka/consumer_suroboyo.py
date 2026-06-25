from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "suroboyo-bus-live",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Menunggu data realtime...\n")

for message in consumer:

    data = message.value

    print("=" * 50)

    print("Route :", data["route_id"])
    print("Type  :", data["bus_type"])
    print("Time  :", data["timestamp"])

    print("Data  :")
    print(data["data"])