from kafka import KafkaProducer
import requests
import json
import time

from config import *

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

URL = "https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4=31.74.01.1001"

while True:

    try:

        response = requests.get(URL, timeout=10)

        data = response.json()

        weather = data["data"][0]["cuaca"][0][0]

        message = {

            "timestamp": weather["local_datetime"],

            "suhu": weather["t"],

            "hujan": weather["weather_desc"],

            "kelembapan": weather["hu"],

            "kecepatan_angin": weather["ws"]

        }

        producer.send(
            TOPIC_BMKG,
            value=message
        )

        producer.flush()

        print("BMKG data sent")

        print(message)

    except Exception as e:

        print("ERROR :", e)

    time.sleep(300)