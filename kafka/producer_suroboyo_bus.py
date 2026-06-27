from kafka import KafkaProducer
import requests
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "suroboyo-bus-live"


def get_tokens():
    try:
        response = requests.get(
            "https://busmapapi.fly.dev/all",
            timeout=10
        )

        response.raise_for_status()

        return response.json()

    except Exception as e:
        print("Gagal mengambil daftar route:", e)
        return None


while True:

    tokens = get_tokens()

    if not tokens:
        time.sleep(5)
        continue

    api_url = tokens.get("apiUrl")

    for route_id, token_info in tokens.items():

        if route_id == "apiUrl":
            continue

        try:

            bearer_token = token_info.split("/")[1]

            headers = {
                "Authorization": f"Bearer {bearer_token}"
            }

            # coba sebagai sbybus dulu
            bus_type = "sbybus"

            url = f"{api_url}/track/{bus_type}/{route_id}"

            response = requests.get(
                url,
                headers=headers,
                timeout=10
            )

            # kalau gagal coba temanbus
            if response.status_code != 200:

                bus_type = "temanbus"

                url = f"{api_url}/track/{bus_type}/{route_id}"

                response = requests.get(
                    url,
                    headers=headers,
                    timeout=10
                )

            if response.status_code != 200:
                print(f"Route {route_id} tidak tersedia")
                continue

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

            jumlah_bus = len(data) if isinstance(data, list) else 0

            print(
                f"Route {route_id} | {bus_type} | "
                f"{jumlah_bus} bus aktif"
            )

        except Exception as e:

            print(
                f"Route {route_id} gagal:",
                e
            )

    print("=" * 60)

    time.sleep(5)