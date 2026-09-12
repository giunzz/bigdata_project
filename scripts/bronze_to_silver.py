import os
import sys
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# 1. Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("ASHRAE-Bronze-to-Silver")
    .master("local[4]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Python:", sys.executable)
print("Spark :", spark.version)


# ============================================================
# 2. Paths
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"

TRAIN_PATH = str(BRONZE_DIR / "train")
BUILDING_PATH = str(BRONZE_DIR / "building_metadata")
WEATHER_PATH = str(BRONZE_DIR / "weather_train")

SILVER_OUT = str(SILVER_DIR / "ashrae_joined")


# ============================================================
# 3. Read Bronze
# ============================================================

train = spark.read.parquet(TRAIN_PATH)
building = spark.read.parquet(BUILDING_PATH)
weather = spark.read.parquet(WEATHER_PATH)

print("\n===== BRONZE LOADED =====")

print("\nTRAIN")
train.printSchema()

print("\nBUILDING")
building.printSchema()

print("\nWEATHER")
weather.printSchema()


# ============================================================
# 4. Chọn business columns
# ============================================================

train = train.select(
    "building_id",
    "meter",
    "timestamp",
    "meter_reading",
)

building = building.select(
    "site_id",
    "building_id",
    "primary_use",
    "square_feet",
    "year_built",
    "floor_count",
)

weather = weather.select(
    "site_id",
    "timestamp",
    "air_temperature",
    "cloud_coverage",
    "dew_temperature",
    "precip_depth_1_hr",
    "sea_level_pressure",
    "wind_direction",
    "wind_speed",
)


# ============================================================
# 5. Basic cleaning
# ============================================================

train_clean = (
    train
    .filter(F.col("building_id").isNotNull())
    .filter(F.col("meter").isNotNull())
    .filter(F.col("timestamp").isNotNull())
    .filter(F.col("meter_reading").isNotNull())
    .dropDuplicates(["building_id", "meter", "timestamp"])
)

building_clean = (
    building
    .filter(F.col("building_id").isNotNull())
    .filter(F.col("site_id").isNotNull())
    .dropDuplicates(["building_id"])
)

weather_clean = (
    weather
    .filter(F.col("site_id").isNotNull())
    .filter(F.col("timestamp").isNotNull())
    .dropDuplicates(["site_id", "timestamp"])
)


# ============================================================
# 6. Join train + building
# ============================================================

train_building = (
    train_clean
    .join(
        F.broadcast(building_clean),
        on="building_id",
        how="left"
    )
)

print("\n===== TRAIN + BUILDING JOIN DONE =====")


# ============================================================
# 7. Join weather
# ============================================================

silver = (
    train_building
    .join(
        weather_clean,
        on=["site_id", "timestamp"],
        how="left"
    )
)


# ============================================================
# 8. Add time features
# ============================================================

silver = (
    silver
    .withColumn("hour", F.hour("timestamp"))
    .withColumn("day_of_week", F.dayofweek("timestamp"))
    .withColumn("day_of_month", F.dayofmonth("timestamp"))
    .withColumn("month", F.month("timestamp"))
    .withColumn(
        "is_weekend",
        F.when(
            F.dayofweek("timestamp").isin([1, 7]),
            1
        ).otherwise(0)
    )
)


# ============================================================
# 9. Sample + schema
# ============================================================

print("\n===== SILVER SAMPLE =====")
silver.show(10, truncate=False)

print("\n===== SILVER SCHEMA =====")
silver.printSchema()


# ============================================================
# 10. Data Quality
# ============================================================

print("\n===== DATA QUALITY =====")

silver.select(
    F.count("*").alias("total_rows"),

    F.sum(
        F.col("site_id").isNull().cast("int")
    ).alias("missing_building_join"),

    F.sum(
        F.col("air_temperature").isNull().cast("int")
    ).alias("missing_air_temperature"),

    F.sum(
        F.col("primary_use").isNull().cast("int")
    ).alias("missing_primary_use"),
).show()


print("\n===== WEATHER NULL SUMMARY =====")

weather_cols = [
    "air_temperature",
    "cloud_coverage",
    "dew_temperature",
    "precip_depth_1_hr",
    "sea_level_pressure",
    "wind_direction",
    "wind_speed",
]

null_exprs = [
    F.sum(
        F.col(c).isNull().cast("int")
    ).alias(c)
    for c in weather_cols
]

silver.select(*null_exprs).show(truncate=False)


# ============================================================
# 11. Write Silver
# ============================================================

(
    silver.write
    .mode("overwrite")
    .parquet(SILVER_OUT)
)

print("\n===== SILVER WRITE COMPLETE =====")
print("Output ->", SILVER_OUT)


spark.stop()