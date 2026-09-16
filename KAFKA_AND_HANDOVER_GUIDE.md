# BÁO CÁO CÔNG VIỆC KAFKA PRODUCER & TÀI LIỆU BÀN GIAO PIPELINE
**Người thực hiện**: Hoàng Mạnh Hùng  
**Vai trò**: Data Streaming Engineer (Apache Kafka Broker & Python Producer)  
**Đề tài**: Xây dựng pipeline Big Data thời gian thực để giám sát và phát hiện bất thường trong tiêu thụ năng lượng tòa nhà  
**Dataset**: ASHRAE - Great Energy Predictor III / Building Data Genome 2 (BDG2)

---

## MỤC LỤC
1. [Tổng kết công việc của Hùng (Đã hoàn thành 100%)](#1-tổng-kết-công-việc-của-hùng)
2. [Chi tiết hạ tầng Kafka & Định dạng dữ liệu (JSON Schema)](#2-chi-tiết-hạ-tầng-kafka--định-dạng-dữ-liệu)
3. [Hướng dẫn kỹ thuật tiếp theo cho DƯƠNG (Spark Structured Streaming)](#3-hướng-dẫn-kỹ-thuật-cho-dương-spark-structured-streaming)
4. [Hướng dẫn kỹ thuật tiếp theo cho PHÚC (Streaming Analytics, Anomaly Detection & Benchmark)](#4-hướng-dẫn-kỹ-thuật-cho-phúc-analytics-anomaly--benchmark)
5. [Quy trình chạy thử nghiệm End-to-End cho cả nhóm](#5-quy-trình-chạy-thử-nghiệm-end-to-end)

---

## 1. TỔNG KẾT CÔNG VIỆC CỦA HÙNG

Hùng đã hoàn thành trước hạn toàn bộ các deliverable theo kế hoạch 8 tuần của nhóm (bao gồm Tuần 1, Tuần 2, Tuần 3, Tuần 4):

| STT | Hạng mục công việc | Trạng thái | Chi tiết kỹ thuật |
| :--- | :--- | :---: | :--- |
| 1 | **Cài đặt môi trường runtime** | **DONE** | Java OpenJDK 17 LTS, Python 3.11 (`.venv`), `uv`, thư viện `kafka-python`, `pyarrow`. |
| 2 | **Cài đặt & Vận hành Apache Kafka** | **DONE** | Kafka 4.2.1 chạy chế độ **KRaft** độc lập (không cần ZooKeeper), lắng nghe tại cổng `localhost:9092`. Đã fix lỗi giới hạn độ dài dòng lệnh trên Windows. |
| 3 | **Khởi tạo Topic streaming** | **DONE** | Topic **`energy_raw`** với **3 partitions**, hỗ trợ xử lý song song. |
| 4 | **Xây dựng Python Producer (`scripts/producer.py`)** | **DONE** | Replay dữ liệu trực tiếp từ `data/silver/ashrae_joined` (20.2 triệu bản ghi đã được Dung join sẵn dữ liệu tòa nhà + thời tiết). Hỗ trợ điều chỉnh tốc độ (`--rate`), kích thước lô (`--batch-size`), giới hạn bản ghi (`--limit`). |
| 5 | **Phân vùng dữ liệu (Partition Key)** | **DONE** | Bản tin gửi vào Kafka được gắn `key = building_id`. Đảm bảo toàn bộ dữ liệu của cùng 1 tòa nhà đi vào cùng 1 partition, duy trì đúng thứ tự thời gian. |
| 6 | **Công cụ đo lường hiệu năng (Benchmark)** | **DONE** | Tự động tính toán Throughput (msg/s) và ghi log vào `benchmark_producer.csv` theo chu kỳ 2 giây. Tốc độ test thực tế đạt từ **200 msg/s đến hơn 4.000 msg/s**. |
| 7 | **Script kiểm thử độc lập (`scripts/test_consumer.py`)** | **DONE** | Giúp theo dõi trực quan các sự kiện được bắn vào Kafka theo thời gian thực. |
| 8 | **Tiện ích khởi động nhanh 1-click** | **DONE** | File `start-kafka.bat` và `create-topic.bat` giúp khởi động Kafka ngay lập tức mà không cần gõ lệnh dài. |

---

## 2. CHI TIẾT HẠ TẦNG KAFKA & ĐỊNH DẠNG DỮ LIỆU

### 2.1. Thông số kết nối Kafka Broker
* **Bootstrap Server**: `localhost:9092`
* **Topic Name**: `energy_raw`
* **Số lượng Partition**: `3`
* **Replication Factor**: `1`
* **Partition Key**: `building_id` (kiểu String)

### 2.2. Cấu trúc JSON Event chuẩn phát ra từ Producer
Mỗi bản tin được serialized dưới dạng chuỗi UTF-8 JSON có cấu trúc như sau:

```json
{
  "building_id": 0,
  "site_id": 0,
  "meter": 0,
  "timestamp": "2016-01-01 06:00:00",
  "meter_reading": 242.309,
  "primary_use": "Education",
  "square_feet": 7432,
  "floor_count": null,
  "air_temperature": 25.0,
  "dew_temperature": 21.1,
  "cloud_coverage": 6.0,
  "sea_level_pressure": 1019.4,
  "wind_speed": 3.1
}
```

### 2.3. Bảng đặc tả trường dữ liệu (Data Dictionary)
| Tên trường | Kiểu dữ liệu | Ý nghĩa |
| :--- | :--- | :--- |
| `building_id` | `Integer` | Mã định danh tòa nhà (dùng làm Partition Key). |
| `site_id` | `Integer` | Khuôn viên / địa điểm đặt cụm tòa nhà (0 - 15). |
| `meter` | `Integer` | Loại công tơ (`0`: Điện năng - Electricity). |
| `timestamp` | `String (Timestamp)` | Thời điểm đo (`YYYY-MM-DD HH:MM:SS`). |
| `meter_reading` | `Double` | Chỉ số tiêu thụ điện (kW hoặc kWh). |
| `primary_use` | `String` | Mục đích sử dụng (Education, Office, Lodging...). |
| `square_feet` | `Long` | Diện tích mặt sàn của tòa nhà (foot vuông). |
| `floor_count` | `Integer / Null` | Số tầng của tòa nhà. |
| `air_temperature` | `Double / Null` | Nhiệt độ môi trường bên ngoài (°C). |
| `dew_temperature` | `Double / Null` | Nhiệt độ điểm sương (°C). |
| `cloud_coverage` | `Double / Null` | Độ che phủ của mây (oktas: 0 - 9). |
| `sea_level_pressure` | `Double / Null` | Áp suất khí quyển mực nước biển (hPa). |
| `wind_speed` | `Double / Null` | Tốc độ gió (m/s). |

---

## 3. HƯỚNG DẪN KỸ THUẬT CHO DƯƠNG (SPARK STRUCTURED STREAMING)

Dương sẽ tiếp nhận luồng dữ liệu từ Kafka topic `energy_raw` để xây dựng luồng xử lý thời gian thực bằng PySpark.

### 3.1. Nhiệm vụ cốt lõi của Dương
1. Kết nối PySpark với Kafka Broker (`localhost:9092`).
2. Parse chuỗi JSON thành các cột dữ liệu theo đúng schema định nghĩa.
3. Ép kiểu cột `timestamp` sang `TimestampType` và cấu hình cơ chế **Watermark** để xử lý dữ liệu đến trễ.
4. Thiết lập **Checkpoint directory** để đảm bảo khả năng phục hồi lỗi (Fault Tolerance) theo ngữ nghĩa *At-least-once* / *Exactly-once*.
5. Đo đạc các chỉ số streaming: `inputRowsPerSecond`, `processedRowsPerSecond`, `batchDuration` để chuyển cho Phúc làm benchmark.

### 3.2. Đoạn mã PySpark mẫu khởi tạo dành cho Dương
Dương có thể tạo file `scripts/streaming_consumer.py` dựa trên khung mẫu sau:

```python
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType
)

# 1. Khởi tạo Spark Session với gói thư viện Kafka
spark = (
    SparkSession.builder
    .appName("ASHRAE-Spark-Streaming-Consumer")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 2. Định nghĩa Schema cho JSON event từ Producer của Hùng
json_schema = StructType([
    StructField("building_id", IntegerType(), True),
    StructField("site_id", IntegerType(), True),
    StructField("meter", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("meter_reading", DoubleType(), True),
    StructField("primary_use", StringType(), True),
    StructField("square_feet", LongType(), True),
    StructField("floor_count", IntegerType(), True),
    StructField("air_temperature", DoubleType(), True),
    StructField("dew_temperature", DoubleType(), True),
    StructField("cloud_coverage", DoubleType(), True),
    StructField("sea_level_pressure", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
])

# 3. Đọc dữ liệu từ Kafka Topic
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "energy_raw")
    .option("startingOffsets", "latest")
    .load()
)

# 4. Parse JSON và chuyển đổi kiểu dữ liệu
parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_payload")
    .select(from_json(col("json_payload"), json_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("timestamp").cast(TimestampType()))
    # Cấu hình Watermark 10 phút để xử lý dữ liệu đến trễ
    .withWatermark("event_time", "10 minutes")
)

# 5. Xuất ra console để kiểm tra luồng (Dương có thể đổi sang ghi Parquet hoặc Memory sink)
query = (
    parsed_stream.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "checkpoints/streaming_raw")
    .start()
)

query.awaitTermination()
```

---

## 4. HƯỚNG DẪN KỸ THUẬT CHO PHÚC (ANALYTICS, ANOMALY & BENCHMARK)

Phúc sẽ làm việc trực tiếp trên luồng DataFrame mà Dương cung cấp để tính toán chỉ số thống kê, phát hiện tiêu thụ điện bất thường và phụ trách phần đánh giá hiệu năng hệ thống.

### 4.1. Nhiệm vụ 1: Window Aggregation (Tính toán cửa sổ trượt)
Phúc thực hiện tính toán tổng và trung bình tiêu thụ điện theo từng nhóm tòa nhà/khuôn viên:
* Cửa sổ thời gian: **10 phút** trượt mỗi **5 phút** (Sliding Window) hoặc **1 giờ** (Tumbling Window).
* Phép tính: `avg(meter_reading)`, `max(meter_reading)`, `min(meter_reading)`, `count(*)`.

```python
from pyspark.sql.functions import window, avg, max, min, col

aggregated_stream = (
    parsed_stream
    .groupBy(
        window(col("event_time"), "10 minutes", "5 minutes"),
        col("site_id"),
        col("building_id")
    )
    .agg(
        avg("meter_reading").alias("avg_reading"),
        max("meter_reading").alias("max_reading"),
        min("meter_reading").alias("min_reading"),
    )
)
```

### 4.2. Nhiệm vụ 2: Thuật toán Anomaly Detection (Phát hiện bất thường)
Do hệ thống streaming cần phản hồi tức thì với độ trễ thấp, Phúc áp dụng 2 quy tắc phát hiện:

1. **Quy tắc Thống kê (Statistical Dynamic Threshold / Z-Score)**:
   * Nếu mức tiêu thụ hiện tại lệch quá $k \times \sigma$ ($k = 3$) so với mức trung bình di động (Moving Average).
   * $\text{Residual} = |y_t - \mu_t| > 3\sigma_t \rightarrow \text{Cảnh báo BẤT THƯỜNG}$.
2. **Quy tắc Nghiệp vụ (Domain Rules)**:
   * **Đột biến tiêu thụ (Spike)**: Mức tiêu thụ tăng vọt gấp 2-3 lần so với công suất thiết kế hoặc giờ trước đó.
   * **Tụt điện / Mất kết nối (Drop to Zero)**: Giờ cao điểm mà `meter_reading == 0.0`.
   * **Đơ cảm biến (Stuck / Frozen Sensor)**: Chỉ số giữ nguyên chính xác một giá trị trong nhiều giờ liên tục.

### 4.3. Nhiệm vụ 3: Benchmark & Báo cáo hiệu năng (Mục 3.8 trong Báo cáo)
Phúc tổng hợp 2 nguồn số liệu:
1. **Số liệu từ Producer của Hùng**: Lấy file `benchmark_producer.csv` mà Hùng đã tạo sẵn để vẽ biểu đồ so sánh:
   * Throughput tại các mức tốc độ gửi: 200 msg/s, 500 msg/s, 1.000 msg/s, 2.000 msg/s và Max Speed (> 4.000 msg/s).
2. **Số liệu từ Spark Streaming của Dương**:
   * Truy xuất `query.lastProgress` để lấy: `inputRowsPerSecond`, `processedRowsPerSecond`, `batchDurationMs`.
   * Đánh giá độ trễ đầu cuối (End-to-End Latency) = Thời gian nhận tại Spark - Thời gian phát từ Producer.

---

## 5. QUY TRÌNH CHẠY THỬ NGHIỆM END-TO-END

Bất kỳ thành viên nào trong nhóm muốn chạy thử hệ thống chỉ cần làm theo 3 bước sau:

### Bước 1: Khởi động Kafka Broker
Click đúp chuột vào file:
```text
start-kafka.bat
```
*(Hoặc mở terminal gõ `.\start-kafka.bat`). Cửa sổ sẽ báo Kafka đang lắng nghe tại `localhost:9092`.*

### Bước 2: Bật Consumer (Dương / Phúc)
Mở terminal, kích hoạt môi trường và lắng nghe:
```powershell
.\.venv\Scripts\activate
python scripts/test_consumer.py
```

### Bước 3: Phát dữ liệu bằng Producer (Hùng)
Mở một terminal khác và chạy:
```powershell
.\.venv\Scripts\activate

# Phát thử 1.000 tin nhắn với tốc độ 200 tin/giây:
python scripts/producer.py --limit 1000 --rate 200

# Hoặc phát tốc độ tối đa (để đo Benchmark tải cao):
python scripts/producer.py --limit 5000 --rate 0
```

---
*Tài liệu này được lưu tại: `bigdata_project/KAFKA_AND_HANDOVER_GUIDE.md` để toàn bộ nhóm tiện tra cứu.*
