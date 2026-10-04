# Phần analytics và benchmark của Phúc

Mã nguồn của Phạm Quang Phúc cho đề tài phân tích điện năng tòa nhà bằng
Apache Spark. Module chuẩn bị dữ liệu Building Data Genome 2, huấn luyện
Random Forest, đánh giá hồi quy, gán cảnh báo residual và đo pipeline
Kafka → Spark Structured Streaming → MongoDB.

Thư mục này có thể đặt tại gốc repository của nhóm. Phần tích hợp với job
streaming của nhóm dùng `AnalyticsBundle` và ví dụ `streaming_hooks.py`.

## Các file chính

| File | Chức năng |
|---|---|
| `prepare_bdg2.py` | Tải dữ liệu BDG2 chính thức, chuyển bảng rộng thành Gold và ghép metadata/weather |
| `phuc_analytics.py` | Làm sạch, tạo đặc trưng, chia tập theo thời gian, train/predict và thử bất thường |
| `streaming_hooks.py` | Ví dụ `foreachBatch` cho dữ liệu đã enrichment; sink mẫu là Parquet |
| `prepare_benchmark.py` | Chuẩn bị Kafka, MongoDB và Spark Kafka JAR cho pilot Linux |
| `run_benchmark.py` | Producer, scoring streaming, MongoDB sink và telemetry |
| `supervise_benchmark.py` | Chạy benchmark cùng các dịch vụ do phiên đó khởi động |
| `process_metrics.py` | Đo tiến trình, xử lý PID namespace |
| `benchmark.py` | Tổng hợp throughput, latency, backlog từ log |
| `summarize_benchmark.py` | Kiểm tra số lượng, receipts, tạo CSV và biểu đồ |
| `Phuc_BigData.ipynb` | Notebook Colab tự chứa code analytics và tạo Python 3.12 riêng nếu cần |
| `example_results/` | Các bảng và biểu đồ của lượt đo đã hoàn thành |

## Môi trường

Đã chạy analytics và pilot streaming trên Linux x86_64, Python 3.12,
Java 17, PySpark 3.5.6. Môi trường pilot có RAM 8 GiB, CPU quota 8 core;
Spark dùng `local[2]`. Các dịch vụ Kafka/MongoDB của pilot chạy cùng máy.
Windows có thể dùng WSL2 hoặc notebook Colab cho analytics.
Dockerfile là cấu hình hỗ trợ, chưa được kiểm thử bằng Docker trong lượt đo.

Ví dụ dưới đây dành cho terminal Linux/WSL, Java 17 đã cài:

```bash
cd phuc_analytics
python3.12 -m venv .phuc_env
source .phuc_env/bin/activate
python -m pip install -r requirements.txt
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PYSPARK_PYTHON="$(pwd)/.phuc_env/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export MPLBACKEND=Agg
export TZ=UTC
```

## Chạy analytics

Thử code nhanh bằng dữ liệu giả:

```bash
python phuc_analytics.py demo-data --output data/demo.csv --days 14 --buildings 3
python phuc_analytics.py train --input data/demo.csv --output results/demo_01 --origin synthetic --trees 12 --depth 5
```

Chạy dữ liệu BDG2 thật, pilot 10 tòa nhà tại site Robin trong 180 ngày:

```bash
python prepare_bdg2.py --download --raw-dir data/bdg2 --output data/bdg2_gold.csv --site-id Robin --building-limit 10 --max-days 180
python phuc_analytics.py train --input data/bdg2_gold.csv --output results/bdg2_real_01 --origin real --unit kWh --trees 60 --depth 8
python phuc_analytics.py injection-test --input results/bdg2_real_01/heldout_test --model results/bdg2_real_01/model --output results/injection_01
```

`train` ghi model, ngưỡng, metrics, predictions test và biểu đồ. Đổi tên thư
mục kết quả khi chạy lại. Chương trình bảo vệ các kết quả huấn luyện đã tồn tại.
BDG2 RAW electricity dùng kWh. Timestamp giữ giờ địa phương gốc của site.
Chia tập theo thời gian 70%/15%/15%; tham số điền thiếu lấy từ train và
ngưỡng cảnh báo lấy từ validation. Mô hình ước lượng tiêu thụ kỳ vọng tại
giờ đang quan sát với thời tiết cùng giờ. Nhãn injection là các thay đổi có
kiểm soát, chưa phải nhãn sự cố thực tế.

## Chạy lại benchmark

Chạy các lệnh BDG2 và train phía trên để tạo đầu vào. Từ cùng thư mục
`phuc_analytics`, trong một thư mục làm việc mới chưa có `model/`:

```bash
cp -a results/bdg2_real_01/model model
cp results/bdg2_real_01/predictions_test_full.csv data/predictions_test_full.csv
python -u supervise_benchmark.py --runtime runtime --output results_benchmark_01
python summarize_benchmark.py --results results_benchmark_01
```

Cần Internet để tải runtime Kafka/MongoDB và JAR; các cổng loopback
19092, 19093, 27019 phải còn trống. Chạy chính thức khoảng 15–20 phút.
Kiểm tra nhanh bằng cách thêm `--smoke` và chọn output khác; smoke không
thay thế số liệu chính thức. Dữ liệu và model được sinh tại máy chạy;
`.gitignore` giữ chúng ngoài commit. Notebook analytics chỉ chạy phần AI,
benchmark streaming dùng các script terminal ở trên.

## Kết quả đã đo

Lượt `phuc_20261004T131302Z_68cc49` ngày 04/10/2026 có 9 run, 288.000
event đo và 144.000 event warm-up. Mỗi tải lặp 3 lần, warm-up 30 giây,
phát đo 60 giây rồi chờ xử lý hết. Nguồn là 6.480 dòng test BDG2 được replay
và gán event_id mới. Các run ghi đủ event, backlog sau drain bằng 0.

| Tải msg/s | Delivered event/s, mean ± SD | Latency p95 giây |
|---:|---:|---:|
| 100 | 92,89 ± 0,88 | 6,410 |
| 500 | 465,12 ± 1,92 | 6,427 |
| 1.000 | 927,89 ± 1,83 | 6,622 |

Delivered throughput tính cả drain. Latency kết thúc sau MongoDB ACK với
`w=1, j=true`, trên cùng đồng hồ máy. Phân vị gộp tất cả event đo theo tải;
SD thông lượng là SD mẫu của ba run. Kết quả thuộc pilot một máy, chưa gồm
API/dashboard, Delta ETL, Airflow, retraining hoặc mạng liên máy.

`example_results/KET_QUA_DO.md` và CSV giữ số đo chi tiết. Để tính lại lượt
đã đo từ raw log, dùng gói `Phuc_Benchmark_Kafka_Spark_Mongo.zip` bàn giao
riêng; gói đó có receipts, model, input, cấu hình và log runtime gốc.

## Tích hợp với nhóm

Nạp `AnalyticsBundle(model_dir)` một lần trước khi tạo callback.
`bundle.score(batch_df)` nhận Gold đã có metadata/weather; đầu ra thêm
prediction, residual, threshold, is_anomaly và severity. Giữ event_id,
run_id, produced_at_ms để nối kết quả với Producer và log đo.
Ví dụ Parquet trong `streaming_hooks.py` cần điều chỉnh sink theo job thực
của nhóm. Các cột đầu vào tối thiểu là building_id, timestamp, meter_reading;
meter=0 biểu thị điện. Đối chiếu schema và quy tắc timestamp của job chung
trước khi ghép module vào pipeline của nhóm.

Nguồn dữ liệu: https://github.com/buds-lab/building-data-genome-project-2
Tài liệu Spark Kafka: https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html
