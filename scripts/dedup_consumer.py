from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = (
    SparkSession.builder
    .appName("Duong-Watermark-Dedup-Test")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("building_id", T.LongType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("meter_reading", T.DoubleType())
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "energy_stream_test")
    .option("startingOffsets", "earliest")
    .load()
)

events = (
    raw.select(
        F.col("offset"),
        F.col("value").cast("string").alias("json_value")
    )
    .withColumn("data", F.from_json("json_value", schema))
    .select("offset", "data.*")
    .withColumn("event_time", F.to_timestamp("timestamp"))
    .filter(
        F.col("event_id").isNotNull()
        & F.col("event_time").isNotNull()
    )
)

deduplicated = (
    events
    .withWatermark("event_time", "5 minutes")
    .dropDuplicatesWithinWatermark(["event_id"])
)

query = (
    deduplicated.writeStream
    .queryName("duong_dedup_test")
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 100)
    .option("checkpointLocation", "checkpoints/duong_dedup_test")
    .trigger(processingTime="5 seconds")
    .start()
)

print("Watermark + Deduplication Consumer started!")
print("Topic: energy_stream_test")
print("Watermark delay: 5 minutes")
print("Deduplication key: event_id")

query.awaitTermination()
