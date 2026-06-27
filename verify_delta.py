"""
Script untuk verifikasi & demo pipeline P2 (Bronze, Silver, Weather, Gold) - Suroboyo Bus.

Cara pakai:
    python verify_delta.py            -> mode demo lengkap (default)
    python verify_delta.py --quick    -> mode cepat, cuma jumlah baris + sample kecil
"""

import os
import sys

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages io.delta:delta-spark_4.1_2.13:4.3.0 pyspark-shell"
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

MODE = "demo"
if "--quick" in sys.argv:
    MODE = "quick"

spark = SparkSession.builder \
    .appName("P2_Verify") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


def garis(judul=""):
    print("\n" + "=" * 70)
    if judul:
        print(f"  {judul}")
        print("=" * 70)


def load_table(path, label):
    """Load tabel Delta dengan aman; kalau belum ada, kasih tahu dan return None."""
    try:
        df = spark.read.format("delta").load(path)
        return df
    except Exception:
        print(f"[INFO] Tabel '{label}' ({path}) belum terbentuk - mungkin belum ada data/window belum selesai.")
        return None


bronze = load_table("delta/bronze", "Bronze")
silver = load_table("delta/silver", "Silver")
weather = load_table("delta/weather", "Weather")
gold = load_table("delta/gold", "Gold")

n_bronze = bronze.count() if bronze is not None else 0
n_silver = silver.count() if silver is not None else 0
n_weather = weather.count() if weather is not None else 0
n_gold = gold.count() if gold is not None else 0

if MODE == "quick":
    print(f"\nBronze: {n_bronze} baris")
    if bronze is not None:
        bronze.select("json_data", "ingest_date").show(3, truncate=80)

    print(f"\nSilver: {n_silver} baris")
    if silver is not None:
        silver.select("route_id", "bus_info", "lat", "lng", "speed", "engine", "sisa_kapasitas").show(5, truncate=False)

    print(f"\nWeather: {n_weather} baris")
    if weather is not None:
        weather.show(5, truncate=False)

    print(f"\nGold: {n_gold} baris")
    if gold is not None:
        gold.show(10, truncate=False)

else:
    garis("PIPELINE P2 - DATA PROCESSING & STORAGE (Suroboyo Bus)")
    print(f"""
  Kafka (suroboyo-bus-live)              Kafka (bmkg-raw)
        |                                       |
        v                                       v
  [BRONZE]  {n_bronze:>5} baris            [WEATHER]  {n_weather:>5} baris
        |                                       |
        v   (explode per-bus, real-time)         |
  [SILVER]  {n_silver:>5} baris                  |
        |                                       |
        +-------------------+------------------+
                            |
                            v   (agregasi per jam per rute + join cuaca)
                      [GOLD]  {n_gold:>5} baris   {'(belum ada - tunggu window 1 jam selesai)' if gold is None else ''}
""")

    garis("BRONZE - Raw data dari Kafka (suroboyo-bus-live)")
    if bronze is not None:
        bronze.select("json_data", "ingest_date").show(3, truncate=100)

    garis("SILVER - Posisi & status bus real-time")
    if silver is not None:
        silver.select("route_id", "bus_info", "lat", "lng", "speed", "engine", "sisa_kapasitas", "event_time") \
            .orderBy(col("event_time").desc()) \
            .show(10, truncate=False)

    garis("WEATHER - Data cuaca BMKG (sudah diparsing)")
    if weather is not None:
        weather.orderBy(col("weather_time").desc()).show(10, truncate=False)

    garis("GOLD - Agregasi per rute per jam + cuaca")
    if gold is not None:
        gold.orderBy("tanggal", "jam", "route_id").show(20, truncate=False)

        garis("RINGKASAN: Rata-rata bus aktif per rute (dari Gold)")
        gold.groupBy("route_id").avg("jumlah_bus_aktif", "avg_speed", "avg_sisa_kapasitas") \
            .orderBy("route_id") \
            .show()
    else:
        print("Gold belum ada data. Window agregasi 1 jam + watermark 10 menit belum selesai.")
        print("Biarkan pipeline (Terminal 2) dan producer (Terminal 3 & 4) tetap berjalan,")
        print("lalu jalankan ulang script ini setelah ~1 jam 10 menit dari mulai data masuk.")

    garis("SELESAI")

spark.stop()