from kafka import KafkaConsumer
import json
from config import *

consumer = KafkaConsumer(
    TOPIC_SUROBOYO_BUS,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest"
)

print("Menunggu data dari Kafka...")

for msg in consumer:
    print("=" * 50)
    print(json.dumps(msg.value, indent=4, ensure_ascii=False))