from kafka import KafkaProducer
import requests
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "suroboyo-bus-live"

ROUTES = [
    ("sbybus", "1"),
    ("sbybus", "12"),
    ("sbybus", "51"),
    ("temanbus", "10")
]


def get_tokens():
    try:
        response = requests.get(
            "https://busmapapi.fly.dev/all",
            timeout=10
        )

        return response.json()

    except Exception as e:
        print("Gagal mengambil token:", e)
        return None


while True:

    tokens = get_tokens()

    if not tokens:
        time.sleep(5)
        continue

    api_url = tokens.get("apiUrl")

    for bus_type, route_id in ROUTES:

        try:

            token_info = tokens.get(route_id)

            if not token_info:
                print(f"Token route {route_id} tidak ditemukan")
                continue

            bearer_token = token_info.split("/")[1]

            headers = {
                "Authorization": f"Bearer {bearer_token}"
            }

            url = f"{api_url}/track/{bus_type}/{route_id}"

            response = requests.get(
                url,
                headers=headers,
                timeout=10
            )

            print(
                f"Route {route_id} status:",
                response.status_code
            )

            data = response.json()

            message = {
                "route_id": route_id,
                "bus_type": bus_type,
                "timestamp": time.time(),
                "data": data
            }

            producer.send(
                TOPIC,
                message
            )

            producer.flush()

            print(
                f"Route {route_id} berhasil dikirim"
            )

        except Exception as e:

            print(
                f"Route {route_id} gagal:",
                e
            )

    time.sleep(5)
