#### Update 
Phase 1
 Read RAW CSV

Phase 2
→ RAW → BRONZE
   ép schema
   timestamp → TimestampType
   kiểm tra null
   ghi Parquet

Phase 3
→ BRONZE → SILVER
   train JOIN building_metadata
   JOIN weather_train
   clean missing values

Phase 4
→ SILVER → GOLD
   feature engineering
   aggregate
   dữ liệu sẵn sàng cho ML / Kafka

RAW
= file gốc chưa đụng vào

BRONZE
= dữ liệu đã được đưa vào hệ thống xử lý
  nhưng vẫn giữ gần nguyên bản nhất có thể

SILVER
= dữ liệu đã clean, join, chuẩn hóa

GOLD
= dữ liệu cuối dùng cho dashboard / ML / analytics

### Environment Setup
Install java,uv 


### DONE : DUNG 
Phase 0 — Setup môi trường
✅ DONE
- WSL2 + Ubuntu
- Python
- Java 17
- uv
- PySpark 4.2.0
- JVM ↔ Python worker hoạt động
- Không còn phụ thuộc winutils.exe trên Windows
Phase 1 — Spark Smoke Test
✅ DONE
- Tạo SparkSession
- Tạo DataFrame
- df.show()
- printSchema()
- Spark action chạy thành công
Phase 2 — RAW → BRONZE
✅ DONE
Input:
- train.csv
- building_metadata.csv
- weather_train.csv

Đã làm:
- Explicit schema
- Convert timestamp
- Thêm _ingested_at
- Thêm _source_file
- Data-quality check
- CSV → Parquet

Output:
data/bronze/train/
data/bronze/building_metadata/
data/bronze/weather_train/
Phase 3,4 — BRONZE → SILVER
✅ DONE
Phase 4 SILVER → GOLD -> process (dang lam)

