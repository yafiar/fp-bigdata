"""
Script untuk verifikasi & demo pipeline P2 (Bronze, Silver, Gold).

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


bronze = spark.read.format("delta").load("delta/bronze")
silver = spark.read.format("delta").load("delta/silver")
gold = spark.read.format("delta").load("delta/gold")

n_bronze = bronze.count()
n_silver = silver.count()
n_gold = gold.count()

if MODE == "quick":
    print(f"\nBronze: {n_bronze} baris")
    bronze.select("json_data", "ingest_date").show(3, truncate=80)

    print(f"\nSilver: {n_silver} baris")
    silver.select("koridor", "halte", "tapInTime", "tapOutTime").show(3, truncate=False)

    print(f"\nGold: {n_gold} baris")
    gold.show(10, truncate=False)

else:
    garis("PIPELINE P2 - DATA PROCESSING & STORAGE")
    print(f"""
  Kafka (transjakarta-raw)
        |
        v
  [BRONZE]  {n_bronze:>5} baris   <- raw data apa adanya, partisi per tanggal
        |
        v   (dedup + filter anomali tap-in tanpa tap-out)
  [SILVER]  {n_silver:>5} baris   <- {n_bronze - n_silver} baris dibuang saat cleaning
        |
        v   (agregasi per halte/jam/koridor + join cuaca BMKG)
  [GOLD]    {n_gold:>5} baris
""")

    garis("BRONZE - Raw data dari Kafka")
    bronze.select("json_data", "ingest_date").show(5, truncate=80)

    garis("SILVER - Setelah dedup & filter anomali")
    silver.select("koridor", "halte", "tapInTime", "tapOutTime", "corridorID").show(10, truncate=False)

    garis("GOLD - Agregasi penumpang + cuaca")
    gold.orderBy("tanggal", "jam", "koridor").show(20, truncate=False)

    garis("RINGKASAN: Total penumpang per koridor (dari Gold)")
    gold.groupBy("koridor").sum("penumpang") \
        .withColumnRenamed("sum(penumpang)", "total_penumpang") \
        .orderBy("koridor") \
        .show()

    garis("SELESAI")

spark.stop()