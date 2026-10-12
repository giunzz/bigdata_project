
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = (
    SparkSession.builder
    .appName("Duong-Spark-Latency-Benchmark")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = T.StructType([
    T.StructField("building_id", T.LongType()),
    T.StructField("site_id", T.LongType()),
    T.StructField("meter", T.IntegerType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("meter_reading", T.DoubleType()),
    T.StructField("primary_use", T.StringType()),
    T.StructField("square_feet", T.DoubleType()),
    T.StructField("floor_count", T.DoubleType()),
    T.StructField("air_temperature", T.DoubleType()),
    T.StructField("dew_temperature", T.DoubleType()),
    T.StructField("cloud_coverage", T.DoubleType()),
    T.StructField("sea_level_pressure", T.DoubleType()),
    T.StructField("wind_speed", T.DoubleType())
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "energy_raw")
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    raw.select(
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_created_at"),
        F.from_json(
            F.col("value").cast("string"), schema
        ).alias("data")
    )
    .select(
        "data.*",
        "kafka_partition",
        "kafka_offset",
        "kafka_created_at"
    )
    .withColumn(
        "event_time",
        F.to_timestamp("timestamp")
    )
)

sink_root = Path("logs/duong_latency_sink")
receipt_root = Path("logs/duong_latency_receipts")

sink_root.mkdir(parents=True, exist_ok=True)
receipt_root.mkdir(parents=True, exist_ok=True)


def process_batch(batch_df, batch_id):
    batch_df = batch_df.persist()

    try:
        if batch_df.isEmpty():
            return

        # 1. Ghi dữ liệu đã parse xuống Parquet.
        sink_path = sink_root / f"batch_{batch_id:06d}"

        batch_df.write.mode("overwrite").parquet(
            str(sink_path)
        )

        # 2. Ghi thời điểm sau khi Parquet write thành công.
        ready_at_ms = time.time_ns() // 1_000_000

        # 3. Tính latency riêng cho từng Kafka record.
        receipts = (
            batch_df.select(
                "kafka_partition",
                "kafka_offset",
                F.unix_millis(
                    "kafka_created_at"
                ).alias("produced_at_ms")
            )
            .withColumn(
                "ready_at_ms",
                F.lit(ready_at_ms)
            )
            .withColumn(
                "latency_ms",
                F.col("ready_at_ms")
                - F.col("produced_at_ms")
            )
        )

        # 4. Lưu bằng chứng để tính Mean/P50/P95/P99.
        receipt_path = (
            receipt_root / f"batch_{batch_id:06d}"
        )

        receipts.write.mode("overwrite").parquet(
            str(receipt_path)
        )

        print(
            f"[LATENCY] Batch={batch_id} "
            f"| Sink={sink_path} "
            f"| Receipts={receipt_path}",
            flush=True
        )

    finally:
        batch_df.unpersist()


query = (
    parsed.writeStream
    .queryName("duong_latency_benchmark")
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        "checkpoints/duong_latency"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

print("Spark latency consumer started!", flush=True)
print("Topic: energy_raw", flush=True)
print("Endpoint: durable Parquet sink", flush=True)
print("Receipts: logs/duong_latency_receipts", flush=True)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
finally:
    spark.stop()
