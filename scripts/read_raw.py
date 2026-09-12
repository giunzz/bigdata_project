# E — Extract: đọc dữ liệu từ train.csv, building_meta.csv, weather_train.csv
# T — Transform: filter meter=0, xử lý null/duplicate, convert timestamp, join, tạo feature, aggregate
# L — Load: ghi kết quả ra Parquet hoặc Delta Lake


import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ASHRAE-Read-Raw")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Python:", sys.executable)
print("Spark :", spark.version)

# =========================
# 1. Đọc dữ liệu RAW
# =========================

train = spark.read.csv(
    "data/raw/train.csv",
    header=True,
    inferSchema=True
)

building = spark.read.csv(
    "data/raw/building_metadata.csv",
    header=True,
    inferSchema=True
)

weather = spark.read.csv(
    "data/raw/weather_train.csv",
    header=True,
    inferSchema=True
)

# =========================
# 2. Kiểm tra từng bảng
# =========================

print("\n===== TRAIN =====")
train.printSchema()
train.show(5, truncate=False)
print("Rows:", train.count())

print("\n===== BUILDING METADATA =====")
building.printSchema()
building.show(5, truncate=False)
print("Rows:", building.count())

print("\n===== WEATHER TRAIN =====")
weather.printSchema()
weather.show(5, truncate=False)
print("Rows:", weather.count())

spark.stop()