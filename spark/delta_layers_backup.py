import os

# ============================================================
# SETUP DEPENDENCIES (Delta Lake + Kafka connector)
# Pakai PYSPARK_SUBMIT_ARGS supaya tidak ditimpa oleh config lain.
# Versi delta-spark_4.1_2.13:4.3.0 cocok untuk Spark 4.1.x
# Versi spark-sql-kafka-0-10_2.13:4.1.1 cocok untuk Spark 4.1.1
# ============================================================
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages "
    "io.delta:delta-spark_4.1_2.13:4.3.0,"
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 "
    "pyspark-shell"
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, current_timestamp, to_date,
    date_format, hour, dayofweek, when, coalesce, lit,
)
from pyspark.sql.types import StructType, StringType, TimestampType

# ============================================================
# INISIALISASI SPARK + DELTA LAKE
# ============================================================
spark = SparkSession.builder \
    .appName("P2_DataProcessing") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark dengan Delta Lake berhasil diinisialisasi!")

# ============================================================
# SKEMA DATA (SEMENTARA — sesuaikan begitu P1 kasih skema final)
# Mengikuti asumsi dokumen: corridorID/koridor, halte, tapInTime,
# tapOutTime, timestamp.
# ============================================================
schema = StructType() \
    .add("koridor", StringType()) \
    .add("halte", StringType()) \
    .add("tapInTime", StringType()) \
    .add("tapOutTime", StringType()) \
    .add("timestamp", StringType())

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "transjakarta-raw"

# ============================================================
# BACA STREAM DARI KAFKA
# ============================================================
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# ============================================================
# BRONZE LAYER
# Simpan raw data apa adanya. Partisi per tanggal (ingest_date)
# supaya query/maintenance ke depan lebih cepat, sesuai requirement:
# "Simpan raw data apa adanya ... Delta table partisi per tanggal."
# ============================================================
bronze_df = raw_stream.selectExpr(
    "CAST(key AS STRING) as kafka_key",
    "CAST(value AS STRING) as json_data",
    "topic", "partition", "offset", "timestamp as kafka_timestamp",
).withColumn("ingest_date", to_date(col("kafka_timestamp")))

bronze_query = bronze_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "delta/bronze/_checkpoints") \
    .partitionBy("ingest_date") \
    .start("delta/bronze")

# ============================================================
# SILVER LAYER
# Parse JSON dari kolom raw, lalu:
#  - dedup transaksi
#  - fill missing corridorID dari nama halte (placeholder: pakai
#    halte sebagai fallback selama belum ada mapping resmi)
#  - filter anomali: tap-in tanpa tap-out
# ============================================================
parsed_df = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

silver_df = parsed_df \
    .filter(col("koridor").isNotNull()) \
    .withColumn(
        "corridorID",
        coalesce(col("koridor"), col("halte"))  # fallback corridorID dari nama halte
    ) \
    .withColumn("tapInTime", col("tapInTime").cast(TimestampType())) \
    .withColumn("tapOutTime", col("tapOutTime").cast(TimestampType())) \
    .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
    .filter(col("tapOutTime").isNotNull())  # filter anomali: tap-in tanpa tap-out

silver_query = silver_df.withWatermark("timestamp", "10 minutes") \
    .dropDuplicates(["koridor", "halte", "tapInTime", "timestamp"]) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "delta/silver/_checkpoints") \
    .start("delta/silver")

# ============================================================
# GOLD LAYER
# Agregasi: penumpang per halte per jam per koridor.
# Partisi per koridor + tanggal untuk query cepat.
# NOTE: join dengan data cuaca (bmkg-raw) dan kalender libur
# (events-raw / referensi hari libur) BELUM disambungkan di sini
# karena topic tersebut belum tersedia/terverifikasi dari P1.
# Placeholder kolom suhu/hujan/is_libur diisi null dulu agar
# schema Feature Store tetap konsisten dengan spesifikasi P2.
# ============================================================
gold_df = silver_df.withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "1 hour"),
        col("koridor"),
        col("halte"),
    ) \
    .agg(count("*").alias("penumpang")) \
    .withColumn("tanggal", to_date(col("window.start"))) \
    .withColumn("jam", hour(col("window.start"))) \
    .withColumn("suhu", lit(None).cast(StringType())) \
    .withColumn("hujan", lit(None).cast(StringType())) \
    .withColumn("is_libur", lit(None).cast(StringType())) \
    .withColumn(
        "is_weekend",
        when(dayofweek(col("tanggal")).isin([1, 7]), lit(True)).otherwise(lit(False))
    ) \
    .select(
        col("koridor"), col("halte"), col("tanggal"), col("jam"),
        col("penumpang"), col("suhu"), col("hujan"),
        col("is_libur"), col("is_weekend"),
    )

gold_query = gold_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "delta/gold/_checkpoints") \
    .partitionBy("koridor", "tanggal") \
    .start("delta/gold")


# ============================================================
# FEATURE STORE OUTPUT (CSV) UNTUK P3 & P5
# Query Gold layer secara periodik dan export ke CSV.
# Dijalankan sebagai stream terpisah dengan trigger interval,
# supaya tidak overwrite file CSV terus-menerus tiap micro-batch.
# Skema: [koridor, halte, tanggal, jam, penumpang, suhu, hujan,
#         is_libur, is_weekend] -- sesuai requirement P2.
# ============================================================
def export_features_to_csv(batch_df, batch_id):
    """Tulis ulang features.csv dari snapshot Gold layer terbaru.
    Dipanggil tiap micro-batch lewat foreachBatch."""
    if batch_df.isEmpty():
        return
    (
        batch_df
        .coalesce(1)  # satu file CSV, cukup untuk skala project ini
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv("delta/gold/features_csv_tmp")
    )
    print(f"[Feature Store] Batch {batch_id}: features.csv diperbarui.")


feature_export_query = gold_df.writeStream \
    .foreachBatch(export_features_to_csv) \
    .option("checkpointLocation", "delta/gold/_feature_export_checkpoints") \
    .trigger(processingTime="1 minute") \
    .start()

print("Pipeline P2 (Bronze, Silver, Gold, Feature Store) sedang berjalan...")
print("  - Bronze   -> delta/bronze (partisi: ingest_date)")
print("  - Silver   -> delta/silver (cleaned, deduped, filtered)")
print("  - Gold     -> delta/gold (partisi: koridor, tanggal)")
print("  - Features -> delta/gold/features_csv_tmp (update tiap 1 menit)")

spark.streams.awaitAnyTermination()