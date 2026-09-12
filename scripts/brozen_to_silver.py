import os
import sys
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# 1. Spark session
# ============================================================

spark = (
    SparkSession.builder
    .appName("ASHRAE-Bronze-to-Silver")
    .master("local[*]")
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
# 3. Read BRONZE
# ============================================================

train = spark.read.parquet(TRAIN_PATH)
building = spark.read.parquet(BUILDING_PATH)
weather = spark.read.parquet(WEATHER_PATH)

print("\n===== BRONZE LOADED =====")

print("Train schema:")
train.printSchema()

print("Building schema:")
building.printSchema()

print("Weather schema:")
weather.printSchema()


# ============================================================
# 4. Select business columns
#    Không mang metadata ingest vào bảng join chính
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
# 5. Data quality / cleaning
# ============================================================

# Những khóa này không được null
train_clean = train.filter(
    F.col("building_id").isNotNull()
    & F.col("timestamp").isNotNull()
    & F.col("meter").isNotNull()
    & F.col("meter_reading").isNotNull()
)

building_clean = building.filter(
    F.col("building_id").isNotNull()
    & F.col("site_id").isNotNull()
)

weather_clean = weather.filter(
    F.col("site_id").isNotNull()
    & F.col("timestamp").isNotNull()
)

# Loại duplicate theo khóa nghiệp vụ
train_clean = train_clean.dropDuplicates(
    ["building_id", "meter", "timestamp"]
)

building_clean = building_clean.dropDuplicates(
    ["building_id"]
)

weather_clean = weather_clean.dropDuplicates(
    ["site_id", "timestamp"]
)


# ============================================================
# 6. Optional scope: electricity only
# ============================================================

# Nếu project chỉ phân tích điện năng thì bật dòng dưới:
#
# train_clean = train_clean.filter(F.col("meter") == 0)
#
# Nếu muốn giữ toàn bộ loại meter thì để nguyên.
#
# meter:
# 0 = electricity
# 1 = chilled water
# 2 = steam
# 3 = hot water


# ============================================================
# 7. Join train + building metadata
# ============================================================

train_building = (
    train_clean
    .join(
        F.broadcast(building_clean),
        on="building_id",
        how="left"
    )
)

print("\n===== AFTER TRAIN + BUILDING JOIN =====")
train_building.printSchema()


# ============================================================
# 8. Join weather
#    Join key: site_id + timestamp
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
# 9. Feature cơ bản cho Silver
# ============================================================

silver = (
    silver
    .withColumn("hour", F.hour("timestamp"))
    .withColumn("day_of_week", F.dayofweek("timestamp"))
    .withColumn("month", F.month("timestamp"))
    .withColumn("day_of_month", F.dayofmonth("timestamp"))
    .withColumn("is_weekend",
                F.when(F.dayofweek("timestamp").isin([1, 7]), 1).otherwise(0))
)


# ============================================================
# 10. Data-quality report
# ============================================================

print("\n===== SILVER SAMPLE =====")
silver.show(10, truncate=False)

print("\n===== SILVER SCHEMA =====")
silver.printSchema()

print("\n===== ROW COUNTS =====")

train_count = train_clean.count()
building_count = building_clean.count()
weather_count = weather_clean.count()
silver_count = silver.count()

print("Train clean   :", train_count)
print("Building clean:", building_count)
print("Weather clean :", weather_count)
print("Silver rows   :", silver_count)


# ============================================================
# 11. Check join quality
# ============================================================

print("\n===== JOIN QUALITY =====")

silver.select(
    F.sum(F.col("site_id").isNull().cast("int"))
    .alias("missing_building_match"),

    F.sum(F.col("air_temperature").isNull().cast("int"))
    .alias("missing_air_temperature"),

    F.sum(F.col("primary_use").isNull().cast("int"))
    .alias("missing_primary_use")
).show()


# ============================================================
# 12. Missing-value summary
# ============================================================

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
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in weather_cols
]

silver.select(*null_exprs).show(truncate=False)


# ============================================================
# 13. Write SILVER
# ============================================================

(
    silver.write
    .mode("overwrite")
    .parquet(SILVER_OUT)
)

print("\n===== SILVER WRITE COMPLETE =====")
print("Output ->", SILVER_OUT)

spark.stop()