import os

# ============================================================
# SETUP DEPENDENCIES (Delta Lake + Kafka connector)
# driver-memory 2g untuk fix heap memory yang penuh saat join cuaca
# ============================================================
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--driver-memory 2g --packages "
    "io.delta:delta-spark_4.1_2.13:4.3.0,"
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 "
    "pyspark-shell"
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, current_timestamp, to_date,
    date_format, hour, dayofweek, when, coalesce, lit, explode,
    to_timestamp, expr,
)
from pyspark.sql.types import (
    StructType, StringType, TimestampType, DoubleType, ArrayType,
)

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
# SKEMA DATA TRANSJAKARTA (SEMENTARA — sesuaikan skema final P1)
# ============================================================
schema_transjakarta = StructType() \
    .add("koridor", StringType()) \
    .add("halte", StringType()) \
    .add("tapInTime", StringType()) \
    .add("tapOutTime", StringType()) \
    .add("timestamp", StringType())

# ============================================================
# SKEMA DATA BMKG
# Mengikuti struktur asli API BMKG:
# { "data": [ { "cuaca": [ [ {local_datetime, t, tp, weather_desc}, ... ], [...] ] } ] }
# ============================================================
cuaca_item_schema = StructType() \
    .add("local_datetime", StringType()) \
    .add("t", DoubleType()) \
    .add("tp", DoubleType()) \
    .add("weather_desc", StringType())

schema_bmkg = StructType().add(
    "data", ArrayType(
        StructType().add("cuaca", ArrayType(ArrayType(cuaca_item_schema)))
    )
)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_TRANSJAKARTA = "transjakarta-raw"
TOPIC_BMKG = "bmkg-raw"

# ============================================================
# BACA STREAM TRANSJAKARTA DARI KAFKA
# ============================================================
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC_TRANSJAKARTA) \
    .option("startingOffsets", "earliest") \
    .load()

# ============================================================
# BRONZE LAYER
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
# ============================================================
parsed_df = raw_stream.select(
    from_json(col("value").cast("string"), schema_transjakarta).alias("data")
).select("data.*")

silver_df = parsed_df \
    .filter(col("koridor").isNotNull()) \
    .withColumn("corridorID", coalesce(col("koridor"), col("halte"))) \
    .withColumn("tapInTime", col("tapInTime").cast(TimestampType())) \
    .withColumn("tapOutTime", col("tapOutTime").cast(TimestampType())) \
    .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
    .filter(col("tapOutTime").isNotNull())

silver_query = silver_df.withWatermark("timestamp", "10 minutes") \
    .dropDuplicates(["koridor", "halte", "tapInTime", "timestamp"]) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "delta/silver/_checkpoints") \
    .start("delta/silver")

# ============================================================
# CUACA: BACA STREAM BMKG DARI KAFKA, WRITE KE DELTA TABLE
# TERPISAH (delta/weather), supaya bisa dibaca sebagai static
# table untuk di-join ke Gold.
# ============================================================
bmkg_raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC_BMKG) \
    .option("startingOffsets", "earliest") \
    .load()

bmkg_parsed = bmkg_raw_stream.select(
    from_json(col("value").cast("string"), schema_bmkg).alias("data")
).select(explode(col("data.data")).alias("loc")) \
 .select(explode(col("loc.cuaca")).alias("cuaca_batch")) \
 .select(explode(col("cuaca_batch")).alias("c")) \
 .select(
    to_timestamp(col("c.local_datetime"), "yyyy-MM-dd HH:mm:ss").alias("weather_time"),
    col("c.t").alias("suhu"),
    col("c.tp").alias("hujan"),
    col("c.weather_desc").alias("weather_desc"),
 )

weather_query = bmkg_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "delta/weather/_checkpoints") \
    .start("delta/weather")

# ============================================================
# GOLD LAYER
# ============================================================
gold_agg = silver_df.withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "1 hour"),
        col("koridor"),
        col("halte"),
    ) \
    .agg(count("*").alias("penumpang")) \
    .withColumn("tanggal", to_date(col("window.start"))) \
    .withColumn("jam", hour(col("window.start"))) \
    .withColumn("window_start", col("window.start")) \
    .withColumn(
        "is_weekend",
        when(dayofweek(col("tanggal")).isin([1, 7]), lit(True)).otherwise(lit(False))
    ) \
    .select(
        col("koridor"), col("halte"), col("tanggal"), col("jam"),
        col("window_start"), col("penumpang"), col("is_weekend"),
    )


def join_weather_and_write(batch_df, batch_id):
    """Tiap micro-batch Gold: join dengan cuaca terbaru (forward-fill),
    lalu tulis ke Delta + export Feature Store CSV."""
    if batch_df.isEmpty():
        return

    try:
        weather = spark.read.format("delta").load("delta/weather")
    except Exception:
        weather = None

    if weather is not None and weather.count() > 0:
        joined = batch_df.alias("g").join(
            weather.alias("w"),
            expr("w.weather_time <= g.window_start"),
            "left",
        )
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number

        rn_window = Window.partitionBy(
            "g.koridor", "g.halte", "g.tanggal", "g.jam"
        ).orderBy(col("w.weather_time").desc())

        result_df = joined.withColumn("rn", row_number().over(rn_window)) \
            .filter(col("rn") == 1) \
            .select(
                col("g.koridor").alias("koridor"),
                col("g.halte").alias("halte"),
                col("g.tanggal").alias("tanggal"),
                col("g.jam").alias("jam"),
                col("g.penumpang").alias("penumpang"),
                col("w.suhu").alias("suhu"),
                col("w.hujan").alias("hujan"),
                lit(None).cast(StringType()).alias("is_libur"),
                col("g.is_weekend").alias("is_weekend"),
            )
    else:
        result_df = batch_df.select(
            col("koridor"), col("halte"), col("tanggal"), col("jam"),
            col("penumpang"),
            lit(None).cast(DoubleType()).alias("suhu"),
            lit(None).cast(DoubleType()).alias("hujan"),
            lit(None).cast(StringType()).alias("is_libur"),
            col("is_weekend"),
        )

    result_df.write.format("delta").mode("append") \
        .option("partitionOverwriteMode", "dynamic") \
        .save("delta/gold")

    (
        result_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv("delta/gold/features_csv_tmp")
    )
    print(f"[Gold+Weather] Batch {batch_id}: {result_df.count()} baris ditulis.")


gold_query = gold_agg.writeStream \
    .foreachBatch(join_weather_and_write) \
    .option("checkpointLocation", "delta/gold/_checkpoints") \
    .start()

print("Pipeline P2 (Bronze, Silver, Gold + Weather Join, Feature Store) sedang berjalan...")
print("  - Bronze   -> delta/bronze (partisi: ingest_date)")
print("  - Silver   -> delta/silver (cleaned, deduped, filtered)")
print("  - Weather  -> delta/weather (cuaca BMKG, forward-fill source)")
print("  - Gold     -> delta/gold (joined dengan cuaca)")
print("  - Features -> delta/gold/features_csv_tmp")

spark.streams.awaitAnyTermination()