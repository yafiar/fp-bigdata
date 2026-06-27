import os

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--driver-memory 2g --packages "
    "io.delta:delta-spark_4.1_2.13:4.3.0,"
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 "
    "pyspark-shell"
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, avg, to_date, hour, dayofweek,
    when, lit, explode, regexp_extract, from_unixtime, expr,
)
from pyspark.sql.types import (
    StructType, StringType, DoubleType, LongType, ArrayType, IntegerType,
)

spark = SparkSession.builder \
    .appName("P2_DataProcessing_SuroboyoBus") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark dengan Delta Lake berhasil diinisialisasi!")

bus_item_schema = StructType() \
    .add("info", StringType()) \
    .add("lat", StringType()) \
    .add("lng", StringType()) \
    .add("direction", StringType()) \
    .add("pnpnaik", IntegerType()) \
    .add("pnpturun", IntegerType()) \
    .add("kuning", IntegerType()) \
    .add("engine", StringType()) \
    .add("keterangan", StringType()) \
    .add("speed", IntegerType())

schema_bus = StructType() \
    .add("route_id", StringType()) \
    .add("bus_type", StringType()) \
    .add("timestamp", DoubleType()) \
    .add("data", ArrayType(bus_item_schema))

schema_bmkg = StructType() \
    .add("timestamp", StringType()) \
    .add("suhu", DoubleType()) \
    .add("hujan", StringType()) \
    .add("kelembapan", DoubleType()) \
    .add("kecepatan_angin", DoubleType())

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_BUS = "suroboyo-bus-live"
TOPIC_BMKG = "bmkg-raw"

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC_BUS) \
    .option("startingOffsets", "earliest") \
    .load()

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

parsed_df = raw_stream.select(
    from_json(col("value").cast("string"), schema_bus).alias("d")
).select(
    col("d.route_id").alias("route_id"),
    col("d.bus_type").alias("bus_type"),
    col("d.timestamp").alias("event_timestamp"),
    explode(col("d.data")).alias("bus"),
)

silver_df = parsed_df.select(
    col("route_id"),
    col("bus_type"),
    col("bus.info").alias("bus_info"),
    expr("try_cast(bus.lat as double)").alias("lat"),
    expr("try_cast(bus.lng as double)").alias("lng"),
    expr("try_cast(bus.direction as int)").alias("direction"),
    col("bus.speed").alias("speed"),
    col("bus.engine").alias("engine"),
    expr(
        "try_cast(regexp_extract(bus.keterangan, 'Sisa Kapasitas\\\\s*:\\\\s*</b>(\\\\d+)', 1) as int)"
    ).alias("sisa_kapasitas"),
    from_unixtime(col("event_timestamp")).cast("timestamp").alias("event_time"),
).filter(col("lat").isNotNull() & col("lng").isNotNull())

silver_query = silver_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "delta/silver/_checkpoints") \
    .start("delta/silver")

bmkg_raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC_BMKG) \
    .option("startingOffsets", "earliest") \
    .load()

bmkg_parsed = bmkg_raw_stream.select(
    from_json(col("value").cast("string"), schema_bmkg).alias("w")
).select(
    col("w.timestamp").cast("timestamp").alias("weather_time"),
    col("w.suhu").alias("suhu"),
    col("w.hujan").alias("weather_desc"),
    col("w.kelembapan").alias("kelembapan"),
    col("w.kecepatan_angin").alias("kecepatan_angin"),
)

weather_query = bmkg_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "delta/weather/_checkpoints") \
    .start("delta/weather")

gold_agg = silver_df.withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "1 hour"),
        col("route_id"),
    ) \
    .agg(
        count(when(col("engine") == "ON", True)).alias("jumlah_bus_aktif"),
        avg("speed").alias("avg_speed"),
        avg("sisa_kapasitas").alias("avg_sisa_kapasitas"),
    ) \
    .withColumn("tanggal", to_date(col("window.start"))) \
    .withColumn("jam", hour(col("window.start"))) \
    .withColumn("window_start", col("window.start")) \
    .withColumn(
        "is_weekend",
        when(dayofweek(col("tanggal")).isin([1, 7]), lit(True)).otherwise(lit(False))
    ) \
    .select(
        col("route_id"), col("tanggal"), col("jam"), col("window_start"),
        col("jumlah_bus_aktif"), col("avg_speed"), col("avg_sisa_kapasitas"),
        col("is_weekend"),
    )


def join_weather_and_write(batch_df, batch_id):
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
            "g.route_id", "g.tanggal", "g.jam"
        ).orderBy(col("w.weather_time").desc())

        result_df = joined.withColumn("rn", row_number().over(rn_window)) \
            .filter(col("rn") == 1) \
            .select(
                col("g.route_id").alias("route_id"),
                col("g.tanggal").alias("tanggal"),
                col("g.jam").alias("jam"),
                col("g.jumlah_bus_aktif").alias("jumlah_bus_aktif"),
                col("g.avg_speed").alias("avg_speed"),
                col("g.avg_sisa_kapasitas").alias("avg_sisa_kapasitas"),
                col("w.suhu").alias("suhu"),
                col("w.weather_desc").alias("weather_desc"),
                col("g.is_weekend").alias("is_weekend"),
            )
    else:
        result_df = batch_df.select(
            col("route_id"), col("tanggal"), col("jam"),
            col("jumlah_bus_aktif"), col("avg_speed"), col("avg_sisa_kapasitas"),
            lit(None).cast(DoubleType()).alias("suhu"),
            lit(None).cast(StringType()).alias("weather_desc"),
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

print("Pipeline P2 (Suroboyo Bus) sedang berjalan...")
print("  - Bronze   -> delta/bronze (partisi: ingest_date)")
print("  - Silver   -> delta/silver (real-time, per-bus, untuk dashboard P4)")
print("  - Weather  -> delta/weather (cuaca BMKG)")
print("  - Gold     -> delta/gold (agregasi per jam per rute + cuaca)")
print("  - Features -> delta/gold/features_csv_tmp")

spark.streams.awaitAnyTermination()