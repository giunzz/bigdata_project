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
    .appName("ASHRAE-ETL-Test")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Python executable:", sys.executable)
print("Spark version:", spark.version)

df = spark.createDataFrame(
    [
        (1, "Building A", 100.5),
        (2, "Building B", 200.2),
    ],
    ["building_id", "name", "meter_reading"]
)

df.show()
df.printSchema()

spark.stop()