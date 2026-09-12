# ASHRAE Energy Prediction Dataset

## 1. Dataset Overview

Project sử dụng **ASHRAE Great Energy Predictor III** làm bộ dữ liệu cơ sở.
https://www.kaggle.com/competitions/ashrae-energy-prediction/data

Dataset chứa dữ liệu tiêu thụ năng lượng theo giờ của hơn 1.000 tòa nhà tại nhiều địa điểm khác nhau, kết hợp với:

- Thông tin tòa nhà
- Dữ liệu thời tiết
- Dữ liệu tiêu thụ năng lượng theo thời gian

Trong project này, dataset được sử dụng để xây dựng pipeline:

Dataset → Kafka → Spark Structured Streaming → Delta Lake → Machine Learning → Anomaly Detection → MongoDB → Dashboard

Mục tiêu chính:

1. Giả lập dữ liệu cảm biến năng lượng realtime từ dữ liệu lịch sử.
2. Dự báo mức tiêu thụ năng lượng của từng tòa nhà.
3. So sánh giá trị thực tế và giá trị dự báo.
4. Phát hiện các mức tiêu thụ bất thường.


## 2. Dataset Files

Dataset gồm các file chính:

```text
dataset/
├── train.csv
├── building_meta.csv
├── weather_train.csv
├── weather_test.csv
├── test.csv
└── sample_submission.csv


nền tảng → lưu trữ → Spark → streaming → NoSQL/pipeline → ML → end-to-end