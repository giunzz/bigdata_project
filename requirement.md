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
