import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    DoubleType,
    StringType,
)

# ============================================================
# 1. Spark session
# ============================================================

spark = (
    SparkSession.builder
    .appName("ASHRAE-Raw-to-Bronze")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Python:", sys.executable)
print("Spark :", spark.version)

# ============================================================
# 2. Path
# ============================================================

RAW_DIR = "data/raw"
BRONZE_DIR = "data/bronze"

TRAIN_PATH = f"{RAW_DIR}/train.csv"
BUILDING_PATH = f"{RAW_DIR}/building_metadata.csv"
WEATHER_PATH = f"{RAW_DIR}/weather_train.csv"

TRAIN_OUT = f"{BRONZE_DIR}/train"
BUILDING_OUT = f"{BRONZE_DIR}/building_metadata"
WEATHER_OUT = f"{BRONZE_DIR}/weather_train"

# ============================================================
# 3. Schema
# ============================================================

train_schema = StructType([
    StructField("building_id", IntegerType(), True),
    StructField("meter", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("meter_reading", DoubleType(), True),
])

building_schema = StructType([
    StructField("site_id", IntegerType(), True),
    StructField("building_id", IntegerType(), True),
    StructField("primary_use", StringType(), True),
    StructField("square_feet", LongType(), True),
    StructField("year_built", IntegerType(), True),
    StructField("floor_count", IntegerType(), True),
])

weather_schema = StructType([
    StructField("site_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("air_temperature", DoubleType(), True),
    StructField("cloud_coverage", DoubleType(), True),
    StructField("dew_temperature", DoubleType(), True),
    StructField("precip_depth_1_hr", DoubleType(), True),
    StructField("sea_level_pressure", DoubleType(), True),
    StructField("wind_direction", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
])

# ============================================================
# 4. Read RAW
# ============================================================

train_raw = (
    spark.read
    .option("header", True)
    .schema(train_schema)
    .csv(TRAIN_PATH)
)

building_raw = (
    spark.read
    .option("header", True)
    .schema(building_schema)
    .csv(BUILDING_PATH)
)

weather_raw = (
    spark.read
    .option("header", True)
    .schema(weather_schema)
    .csv(WEATHER_PATH)
)

print("\nRAW loaded.")

# ============================================================
# 5. RAW -> BRONZE transformation
# ============================================================

train_bronze = (
    train_raw
    .withColumn(
        "timestamp",
        F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "_ingested_at",
        F.current_timestamp()
    )
    .withColumn(
        "_source_file",
        F.input_file_name()
    )
)

building_bronze = (
    building_raw
    .withColumn(
        "_ingested_at",
        F.current_timestamp()
    )
    .withColumn(
        "_source_file",
        F.input_file_name()
    )
)

weather_bronze = (
    weather_raw
    .withColumn(
        "timestamp",
        F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "_ingested_at",
        F.current_timestamp()
    )
    .withColumn(
        "_source_file",
        F.input_file_name()
    )
)

# ============================================================
# 6. Kiểm tra nhanh
# ============================================================

print("\n===== TRAIN BRONZE =====")
train_bronze.printSchema()
train_bronze.show(5, truncate=False)

print("\n===== BUILDING BRONZE =====")
building_bronze.printSchema()
building_bronze.show(5, truncate=False)

print("\n===== WEATHER BRONZE =====")
weather_bronze.printSchema()
weather_bronze.show(5, truncate=False)

# ============================================================
# 7. Data quality check cơ bản
# ============================================================

print("\n===== DATA QUALITY CHECK =====")

print("Train rows   :", train_bronze.count())
print("Building rows:", building_bronze.count())
print("Weather rows :", weather_bronze.count())

print("\nNull timestamp train:")
train_bronze.select(
    F.sum(F.col("timestamp").isNull().cast("int")).alias("null_timestamp")
).show()

print("\nNull timestamp weather:")
weather_bronze.select(
    F.sum(F.col("timestamp").isNull().cast("int")).alias("null_timestamp")
).show()

print("\nNull meter_reading:")
train_bronze.select(
    F.sum(F.col("meter_reading").isNull().cast("int")).alias("null_meter_reading")
).show()

# ============================================================
# 8. Write BRONZE as Parquet
# ============================================================

(
    train_bronze.write
    .mode("overwrite")
    .parquet(TRAIN_OUT)
)

(
    building_bronze.write
    .mode("overwrite")
    .parquet(BUILDING_OUT)
)

(
    weather_bronze.write
    .mode("overwrite")
    .parquet(WEATHER_OUT)
)

print("\n===== WRITE COMPLETE =====")
print("Train    ->", TRAIN_OUT)
print("Building ->", BUILDING_OUT)
print("Weather  ->", WEATHER_OUT)

spark.stop()