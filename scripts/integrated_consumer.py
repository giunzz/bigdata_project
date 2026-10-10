
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# 1. Spark Session
spark = (
    SparkSession.builder
    .appName("Duong-Integrated-Streaming")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Schema thực tế của ASHRAE Producer
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
    T.StructField("wind_speed", T.DoubleType()),
])

# 3. Kafka Source
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "energy_raw")
    .option("startingOffsets", "latest")
    .load()
)

# 4. Parse JSON
events = (
    raw.select(
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_created_at"),
        F.col("value").cast("string").alias("json_value")
    )
    .withColumn(
        "raw_map",
        F.from_json(
            "json_value",
            T.MapType(T.StringType(), T.StringType())
        )
    )
    .withColumn(
        "data",
        F.from_json("json_value", schema)
    )
    .select(
        "kafka_partition",
        "kafka_offset",
        "kafka_created_at",
        "json_value",
        "raw_map",
        "data.*"
    )
    .withColumn(
        "event_time",
        F.try_to_timestamp(
            F.col("timestamp"),
            F.lit("yyyy-MM-dd HH:mm:ss")
        )
    )
)

# 5. Schema Validation
validated = (
    events.withColumn(
        "reject_reason",
        F.when(
            F.col("raw_map").isNull(),
            "MALFORMED_JSON"
        )
        .when(
            ~F.map_contains_key(
                F.col("raw_map"), "meter_reading"
            ),
            "MISSING_METER_READING"
        )
        .when(
            F.col("meter_reading").isNull()
            | F.isnan("meter_reading")
            | (F.col("meter_reading") < 0),
            "INVALID_METER_READING"
        )
        .when(
            F.col("building_id").isNull(),
            "INVALID_BUILDING_ID"
        )
        .when(
            F.col("site_id").isNull(),
            "INVALID_SITE_ID"
        )
        .when(
            F.col("meter").isNull(),
            "INVALID_METER"
        )
        .when(
            F.col("event_time").isNull(),
            "INVALID_TIMESTAMP"
        )
        .otherwise(F.lit(None).cast("string"))
    )
    .withColumn(
        "validation_status",
        F.when(
            F.col("reject_reason").isNull(), "VALID"
        ).otherwise("REJECTED")
    )
)

# 6. Deduplication Key
# ASHRAE không có event_id.
# Sử dụng khóa nghiệp vụ building_id + meter + timestamp.
# Rejected record dùng partition + offset để không vô tình
# gộp các bản ghi lỗi khác nhau.
with_key = validated.withColumn(
    "dedup_key",
    F.when(
        F.col("reject_reason").isNull(),
        F.concat_ws(
            "|",
            F.col("building_id"),
            F.col("meter"),
            F.col("timestamp")
        )
    ).otherwise(
        F.concat_ws(
            "|",
            F.lit("invalid"),
            F.col("kafka_partition"),
            F.col("kafka_offset")
        )
    )
)

# 7. Watermark theo Kafka CreateTime phục vụ replay
# Timestamp lịch sử 2016 vẫn được giữ tại event_time.
deduplicated = (
    with_key
    .withWatermark("kafka_created_at", "5 minutes")
    .dropDuplicatesWithinWatermark(["dedup_key"])
)

# 8. Persist kết quả từng micro-batch
root = Path("logs/duong_integrated")

valid_root = root / "valid"
rejected_root = root / "rejected"

valid_root.mkdir(parents=True, exist_ok=True)
rejected_root.mkdir(parents=True, exist_ok=True)


def process_batch(batch_df, batch_id):
    cached = batch_df.persist()

    try:
        total = cached.count()

        if total == 0:
            return

        valid = cached.filter(
            F.col("reject_reason").isNull()
        )

        rejected = cached.filter(
            F.col("reject_reason").isNotNull()
        )

        valid_count = valid.count()
        rejected_count = total - valid_count

        internal_columns = [
            "raw_map",
            "dedup_key"
        ]

        if valid_count > 0:
            valid.write.mode("overwrite").parquet(
                str(valid_root / f"batch_{batch_id:06d}")
            )

        if rejected_count > 0:
            rejected.drop(*internal_columns).write.mode(
                "overwrite"
            ).parquet(
                str(rejected_root / f"batch_{batch_id:06d}")
            )

        print(
            f"[INTEGRATED] Batch={batch_id} "
            f"| OutputRows={total} "
            f"| VALID={valid_count} "
            f"| REJECTED={rejected_count}",
            flush=True
        )

    finally:
        cached.unpersist()


# 9. Streaming Query với checkpoint riêng
query = (
    deduplicated.writeStream
    .queryName("duong_integrated_stream")
    .foreachBatch(process_batch)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "checkpoints/duong_integrated"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

print("Integrated Streaming Consumer started!", flush=True)
print("Topic: energy_raw", flush=True)
print("Validation: enabled", flush=True)
print("Dedup key: building_id + meter + timestamp", flush=True)
print("Watermark: 5 minutes on Kafka timestamp", flush=True)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
finally:
    spark.stop()
