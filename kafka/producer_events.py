from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:

    event = {
    "event_name":"Car Free Day Surabaya",
    "location":"Jalan Tunjungan",
    "impact":"medium"
    }

    producer.send(
        "events-raw",
        value=event
    )

    print(event)

    time.sleep(5)