"""
Producer dummy untuk testing pipeline P2 (delta_layers.py).
Mengirim data simulasi tap-in/tap-out Transjakarta ke topic 'transjakarta-raw'.

Cara pakai:
    python kafka/producer_dummy_test.py

Catatan: Ini HANYA untuk testing P2 secara mandiri, BUKAN pengganti
producer asli milik P1. Skema mengikuti asumsi dari dokumen pembagian
kerja (corridorID, halte, tapInTime, tapOutTime, timestamp).
"""

import json
import random
import time
from datetime import datetime, timedelta

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "transjakarta-raw"

# Daftar koridor dan halte contoh (subset kecil untuk testing)
KORIDOR_HALTE = {
    "1": ["Blok M", "Bundaran Senayan", "Gelora Bung Karno", "Bendungan Hilir"],
    "6": ["Ragunan", "Departemen Pertanian", "SMK 57", "Kuningan Timur"],
    "9": ["Pinang Ranti", "Cawang UKI", "Cikoko Stasiun Cawang", "Tebet"],
}


def generate_event():
    """Buat satu event tap-in/tap-out simulasi."""
    koridor = random.choice(list(KORIDOR_HALTE.keys()))
    halte = random.choice(KORIDOR_HALTE[koridor])

    now = datetime.now()
    tap_in_time = now - timedelta(minutes=random.randint(0, 5))

    # Sengaja buat sebagian kecil data anomali (tap-in tanpa tap-out)
    # untuk menguji logika filter anomali di Silver layer.
    has_tap_out = random.random() > 0.05  # 5% anomali
    tap_out_time = (
        tap_in_time + timedelta(minutes=random.randint(5, 45))
        if has_tap_out
        else None
    )

    event = {
        "koridor": koridor,
        "halte": halte,
        "tapInTime": tap_in_time.strftime("%Y-%m-%d %H:%M:%S"),
        "tapOutTime": tap_out_time.strftime("%Y-%m-%d %H:%M:%S") if tap_out_time else None,
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
    }
    return event


def main():
    print(f"Menghubungkan ke Kafka di {BOOTSTRAP_SERVERS} ...")
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"Terhubung. Mengirim event dummy ke topic '{TOPIC}' setiap 1 detik...")
    print("Tekan Ctrl+C untuk berhenti.\n")

    count = 0
    try:
        while True:
            event = generate_event()
            producer.send(TOPIC, value=event)
            count += 1
            print(f"[{count}] Terkirim: {event}")
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\nDihentikan. Total {count} event terkirim.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()