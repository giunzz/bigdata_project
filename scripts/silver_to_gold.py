import os
import sys
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# 1. Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("ASHRAE-Silver-to-Gold")
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

SILVER_PATH = str(
    PROJECT_ROOT / "data" / "silver" / "ashrae_joined"
)

GOLD_PATH = str(
    PROJECT_ROOT / "data" / "gold" / "ashrae_ml_ready"
)


# ============================================================
# 3. Read Silver
# ============================================================

df = spark.read.parquet(SILVER_PATH)

print("\n===== SILVER LOADED =====")
df.printSchema()

print("\n===== INPUT SAMPLE =====")
df.show(5, truncate=False)


# ============================================================
# 4. Building metadata missing-value handling
# ============================================================

print("\n===== BUILDING NULL CHECK =====")

df.select(
    F.count("*").alias("total_rows"),
    F.sum(F.col("year_built").isNull().cast("int")).alias("null_year_built"),
    F.sum(F.col("floor_count").isNull().cast("int")).alias("null_floor_count"),
).show()


# ------------------------------------------------------------
# 4.1 Calculate building medians
# ------------------------------------------------------------

year_built_median = (
    df.select(
        F.expr(
            "percentile_approx(year_built, 0.5)"
        ).alias("median")
    )
    .first()["median"]
)

floor_count_median = (
    df.select(
        F.expr(
            "percentile_approx(floor_count, 0.5)"
        ).alias("median")
    )
    .first()["median"]
)

print("\n===== BUILDING MEDIANS =====")
print("year_built median :", year_built_median)
print("floor_count median:", floor_count_median)


# ------------------------------------------------------------
# 4.2 Safe fill
# ------------------------------------------------------------

building_fill_values = {}

if year_built_median is not None:
    building_fill_values["year_built"] = year_built_median
else:
    print("WARNING: year_built median is None -> skip fill")

if floor_count_median is not None:
    building_fill_values["floor_count"] = floor_count_median
else:
    print(
        "WARNING: floor_count median is None. "
        "Column may contain only NULL values -> skip fill"
    )

if building_fill_values:
    print("Building fill values:", building_fill_values)
    df = df.fillna(building_fill_values)


# ============================================================
# 5. Weather missing-value handling
# ============================================================

weather_cols = [
    "air_temperature",
    "cloud_coverage",
    "dew_temperature",
    "precip_depth_1_hr",
    "sea_level_pressure",
    "wind_direction",
    "wind_speed",
]


# ------------------------------------------------------------
# 5.1 Check weather missing values before fill
# ------------------------------------------------------------

print("\n===== WEATHER NULL BEFORE FILL =====")

weather_null_exprs = [
    F.sum(
        F.col(c).isNull().cast("int")
    ).alias(c)
    for c in weather_cols
]

df.select(*weather_null_exprs).show(truncate=False)


# ------------------------------------------------------------
# 5.2 Forward / backward fill by site_id
# ------------------------------------------------------------

window_prev = (
    Window
    .partitionBy("site_id")
    .orderBy("timestamp")
    .rowsBetween(
        Window.unboundedPreceding,
        0
    )
)

window_next = (
    Window
    .partitionBy("site_id")
    .orderBy("timestamp")
    .rowsBetween(
        0,
        Window.unboundedFollowing
    )
)

for col_name in weather_cols:

    print(f"Applying forward/backward fill: {col_name}")

    previous_value = F.last(
        F.col(col_name),
        ignorenulls=True
    ).over(window_prev)

    next_value = F.first(
        F.col(col_name),
        ignorenulls=True
    ).over(window_next)

    df = df.withColumn(
        col_name,
        F.coalesce(
            F.col(col_name),
            previous_value,
            next_value
        )
    )


# ============================================================
# 6. Weather median fallback
# ============================================================

print("\n===== WEATHER MEDIANS =====")

weather_medians = {}

for col_name in weather_cols:

    median_value = (
        df.select(
            F.expr(
                f"percentile_approx({col_name}, 0.5)"
            ).alias("median")
        )
        .first()["median"]
    )

    if median_value is not None:
        weather_medians[col_name] = median_value
        print(f"{col_name}: {median_value}")
    else:
        print(
            f"{col_name}: None "
            f"-> WARNING: skip median fill"
        )


if weather_medians:
    df = df.fillna(weather_medians)


# ============================================================
# 7. Check weather null after fill
# ============================================================

print("\n===== WEATHER NULL AFTER FILL =====")

weather_null_after = [
    F.sum(
        F.col(c).isNull().cast("int")
    ).alias(c)
    for c in weather_cols
]

df.select(*weather_null_after).show(truncate=False)


# ============================================================
# 8. Feature Engineering
# ============================================================


# ------------------------------------------------------------
# 8.1 Target log transform
# ------------------------------------------------------------

df = df.withColumn(
    "meter_reading_log1p",
    F.log1p(F.col("meter_reading"))
)


# ------------------------------------------------------------
# 8.2 Square feet log transform
# ------------------------------------------------------------

df = df.withColumn(
    "square_feet_log1p",
    F.log1p(F.col("square_feet"))
)


# ------------------------------------------------------------
# 8.3 Building age
# ------------------------------------------------------------

df = df.withColumn(
    "building_age",
    F.year(F.col("timestamp")) - F.col("year_built")
)

df = df.withColumn(
    "building_age",
    F.when(
        F.col("building_age") >= 0,
        F.col("building_age")
    ).otherwise(None)
)


# ============================================================
# 9. Cyclical Time Features
# ============================================================

PI = 3.141592653589793


df = (
    df
    .withColumn(
        "hour_sin",
        F.sin(
            2 * PI * F.col("hour") / 24
        )
    )
    .withColumn(
        "hour_cos",
        F.cos(
            2 * PI * F.col("hour") / 24
        )
    )
    .withColumn(
        "month_sin",
        F.sin(
            2 * PI * F.col("month") / 12
        )
    )
    .withColumn(
        "month_cos",
        F.cos(
            2 * PI * F.col("month") / 12
        )
    )
)


# ============================================================
# 10. Derived Weather Features
# ============================================================

df = df.withColumn(
    "temperature_diff",
    F.col("air_temperature")
    - F.col("dew_temperature")
)


# ============================================================
# 11. Optional additional features
# ============================================================

# Weekend already exists from Silver.
#
# Có thể thêm:
#
# - day/night flag
# - season
# - temperature bins
# - building size category
#
# nhưng chưa cần ở phase hiện tại.


# ============================================================
# 12. Optional electricity-only
# ============================================================

# Nếu project chỉ tập trung điện:
#
# df = df.filter(F.col("meter") == 0)


# ============================================================
# 13. Prepare final Gold columns
# ============================================================

gold_columns = [
    "building_id",
    "site_id",
    "timestamp",
    "meter",

    # Target
    "meter_reading",
    "meter_reading_log1p",

    # Building
    "primary_use",
    "square_feet",
    "square_feet_log1p",
    "year_built",
    "building_age",

    # Weather
    "air_temperature",
    "cloud_coverage",
    "dew_temperature",
    "temperature_diff",
    "precip_depth_1_hr",
    "sea_level_pressure",
    "wind_direction",
    "wind_speed",

    # Time
    "hour",
    "day_of_week",
    "day_of_month",
    "month",
    "is_weekend",

    # Cyclical time features
    "hour_sin",
    "hour_cos",
    "month_sin",
    "month_cos",
]


# floor_count chỉ giữ nếu cột có dữ liệu hữu ích
if floor_count_median is not None:
    gold_columns.insert(
        gold_columns.index("air_temperature"),
        "floor_count"
    )
else:
    print(
        "\nINFO: floor_count excluded from Gold "
        "because median could not be calculated."
    )


gold = df.select(*gold_columns)


# ============================================================
# 14. Gold Sample
# ============================================================

print("\n===== GOLD SAMPLE =====")

gold.show(
    10,
    truncate=False
)


# ============================================================
# 15. Gold Schema
# ============================================================

print("\n===== GOLD SCHEMA =====")

gold.printSchema()


# ============================================================
# 16. Gold Null Check
# ============================================================

print("\n===== GOLD NULL CHECK =====")

quality_check_cols = [
    "year_built",
    "air_temperature",
    "cloud_coverage",
    "dew_temperature",
    "precip_depth_1_hr",
    "sea_level_pressure",
    "wind_direction",
    "wind_speed",
]

if floor_count_median is not None:
    quality_check_cols.append(
        "floor_count"
    )


null_check_exprs = [
    F.sum(
        F.col(c).isNull().cast("int")
    ).alias(c)
    for c in quality_check_cols
]

gold.select(
    *null_check_exprs
).show(
    truncate=False
)


# ============================================================
# 17. Total Rows
# ============================================================

print("\n===== GOLD ROW COUNT =====")

gold.select(
    F.count("*").alias("total_rows")
).show()


# ============================================================
# 18. Target Statistics
# ============================================================

print("\n===== TARGET STATISTICS =====")

gold.select(
    F.min("meter_reading").alias("min"),
    F.max("meter_reading").alias("max"),
    F.avg("meter_reading").alias("mean"),
    F.stddev("meter_reading").alias("stddev"),
).show()


# ============================================================
# 19. Meter Distribution
# ============================================================

print("\n===== METER DISTRIBUTION =====")

gold.groupBy(
    "meter"
).count().orderBy(
    "meter"
).show()


# ============================================================
# 20. Building Use Distribution
# ============================================================

print("\n===== PRIMARY USE DISTRIBUTION =====")

gold.groupBy(
    "primary_use"
).count().orderBy(
    F.desc("count")
).show(
    truncate=False
)


# ============================================================
# 21. Write Gold
# ============================================================

print("\n===== WRITING GOLD =====")

(
    gold.write
    .mode("overwrite")
    .partitionBy("meter")
    .parquet(GOLD_PATH)
)


print("\n===== GOLD WRITE COMPLETE =====")
print("Output ->", GOLD_PATH)


# ============================================================
# 22. Stop Spark
# ============================================================

spark.stop()