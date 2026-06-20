from kafka import KafkaConsumer
import json
from config import *
consumer=KafkaConsumer(TOPIC_TRANSJAKARTA,bootstrap_servers=KAFKA_SERVER,value_deserializer=lambda m: json.loads(m.decode()))
for msg in consumer:
    print(msg.value)
