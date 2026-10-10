
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# 1. Khởi tạo Spark
spark = (
    SparkSession.builder
    .appName("Duong-Energy-Streaming")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. JSON schema theo Producer của Hùng
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

# 3. Kết nối Kafka
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "energy_raw")
    .option("startingOffsets", "earliest")
    .load()
)

# 4. Parse JSON
parsed_stream = (
    raw_stream
    .select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("json_value"),
        F.col("partition"),
        F.col("offset")
    )
    .withColumn(
        "data",
        F.from_json("json_value", schema)
    )
    .select(
        "data.*",
        "kafka_key",
        "partition",
        "offset"
    )
    .withColumn(
        "event_time",
        F.to_timestamp("timestamp")
    )
)

# 5. Hiển thị dữ liệu theo micro-batch
query = (
    parsed_stream.writeStream
    .queryName("duong_kafka_debug")
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 10)
    .option(
        "checkpointLocation",
        "checkpoints/duong_debug"
    )
    .trigger(processingTime="5 seconds")
    .start()
)


import json
import time
from datetime import datetime
from pathlib import Path

log_dir = Path("logs")
log_dir.mkdir(parents=True, exist_ok=True)

run_time = datetime.now().strftime("%Y%m%d_%H%M%S")
log_path = log_dir / f"duong_spark_progress_{run_time}.jsonl"

print("Spark Structured Streaming started!")
print(f"Metrics log: {log_path}")

logged_batches = set()

try:
    while query.isActive:
        for progress in query.recentProgress:
            batch_id = progress["batchId"]
            input_rows = progress.get("numInputRows", 0)


            if input_rows <= 0 or batch_id in logged_batches:
                continue

            with log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(progress, ensure_ascii=False, default=str) + "\n")

            logged_batches.add(batch_id)

            duration_ms = progress.get("durationMs", {}).get(
                "triggerExecution"
            )

            print(
                f"[METRICS] Batch={batch_id} | "
                f"InputRows={progress.get('numInputRows')} | "
                f"InputRPS={progress.get('inputRowsPerSecond')} | "
                f"ProcessedRPS={progress.get('processedRowsPerSecond')} | "
                f"DurationMs={duration_ms}"
            )

        time.sleep(1)

except KeyboardInterrupt:
    print("Stopping Spark Streaming...")
    query.stop()

finally:
    print(f"Metrics saved to: {log_path}")

