from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = (
    SparkSession.builder
    .appName("Duong-ASHRAE-Schema-Validation")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = T.StructType([
    T.StructField("building_id", T.IntegerType(), True),
    T.StructField("site_id", T.IntegerType(), True),
    T.StructField("meter", T.IntegerType(), True),
    T.StructField("timestamp", T.StringType(), True),
    T.StructField("meter_reading", T.DoubleType(), True),
    T.StructField("primary_use", T.StringType(), True),
    T.StructField("square_feet", T.DoubleType(), True),
    T.StructField("floor_count", T.IntegerType(), True),
    T.StructField("air_temperature", T.DoubleType(), True),
    T.StructField("dew_temperature", T.DoubleType(), True),
    T.StructField("cloud_coverage", T.DoubleType(), True),
    T.StructField("sea_level_pressure", T.DoubleType(), True),
    T.StructField("wind_speed", T.DoubleType(), True),
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "energy_schema_test")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    raw.select(
        F.col("offset"),
        F.col("value").cast("string").alias("json_value")
    )
    .withColumn(
        "raw_map",
        F.from_json("json_value", T.MapType(T.StringType(), T.StringType()))
    )
    .withColumn("data", F.from_json("json_value", schema))
    .withColumn(
        "event_time",
        F.try_to_timestamp(
            F.col("data.timestamp"),
            F.lit("yyyy-MM-dd HH:mm:ss")
        )
    )
)

validated = (
    parsed
    .withColumn(
        "reject_reason",
        F.when(
            F.col("raw_map").isNull(),
            "MALFORMED_JSON"
        )
        .when(
            ~F.map_contains_key(F.col("raw_map"), "meter_reading"),
            "MISSING_METER_READING"
        )
        .when(
            F.col("data.meter_reading").isNull(),
            "INVALID_METER_READING"
        )
        .when(
            F.col("data.building_id").isNull(),
            "INVALID_BUILDING_ID"
        )
        .when(
            F.col("data.site_id").isNull(),
            "INVALID_SITE_ID"
        )
        .when(
            F.col("data.meter").isNull(),
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
        F.when(F.col("reject_reason").isNull(), "VALID")
        .otherwise("REJECTED")
    )
    .select(
        "offset",
        "validation_status",
        "reject_reason",
        F.col("data.building_id").alias("building_id"),
        F.col("data.timestamp").alias("timestamp"),
        F.col("data.meter_reading").alias("meter_reading")
    )
)

query = (
    validated.writeStream
    .queryName("duong_schema_validation")
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 100)
    .option(
        "checkpointLocation",
        "checkpoints/duong_schema_test"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

print("ASHRAE Schema Validator started!")
print("Topic: energy_schema_test")
print("Expected fields: 13")
print("Output: VALID / REJECTED + reject_reason")

query.awaitTermination()
