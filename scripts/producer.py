"""
Producer script for ASHRAE Smart Building Energy Consumption stream simulation.
Replays historic sensor readings into Apache Kafka topic 'energy_raw'.

Author: Hoang Manh Hung
Project: Real-time Big Data Pipeline for Smart Buildings
"""

import argparse
import csv
import json
import os
import sys
import time
import warnings
from datetime import datetime
from pathlib import Path

# Suppress kafka-python serializer deprecation warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

from kafka import KafkaProducer
import pyarrow.parquet as pq

if sys.stdout.encoding != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka Producer for ASHRAE Smart Meter Data")
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="energy_raw",
        help="Kafka topic to send data to (default: energy_raw)",
    )
    parser.add_argument(
        "--input",
        type=str,
        default="data/silver/ashrae_joined",
        help="Input data path (.parquet folder or .csv file)",
    )
    parser.add_argument(
        "--meter",
        type=int,
        default=0,
        help="Meter type filter (0 = electricity, -1 = all meters, default: 0)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=100.0,
        help="Target messages per second (0 = send as fast as possible for max throughput benchmark)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Buffer flush batch size (default: 500)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Total messages to send (0 = send all records, default: 5000 for smoke test)",
    )
    parser.add_argument(
        "--benchmark-file",
        type=str,
        default="benchmark_producer.csv",
        help="Output CSV file to log throughput benchmark metrics",
    )
    return parser.parse_args()


def get_data_iterator(input_path, meter_filter=0):
    """
    Stream records from parquet or csv iteratively to maintain minimal memory footprint.
    """
    p = Path(input_path)
    if not p.exists():
        raise FileNotFoundError(f"Input path does not exist: {input_path}")

    # Case 1: Parquet directory or file
    if p.is_dir() or p.suffix == ".parquet":
        if p.is_dir():
            files = sorted(p.glob("*.parquet"))
        else:
            files = [p]

        for fpath in files:
            parquet_file = pq.ParquetFile(fpath)
            for batch in parquet_file.iter_batches(batch_size=2048):
                pydict = batch.to_pydict()
                cols = list(pydict.keys())
                num_rows = len(pydict[cols[0]])
                for i in range(num_rows):
                    meter_val = pydict.get("meter", [None])[i]
                    if meter_filter >= 0 and meter_val is not None and meter_val != meter_filter:
                        continue
                    
                    ts = pydict.get("timestamp", [None])[i]
                    if isinstance(ts, datetime):
                        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
                    elif hasattr(ts, "to_pydatetime"):
                        ts_str = ts.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        ts_str = str(ts)

                    record = {
                        "building_id": pydict.get("building_id", [None])[i],
                        "site_id": pydict.get("site_id", [None])[i],
                        "meter": meter_val,
                        "timestamp": ts_str,
                        "meter_reading": float(pydict.get("meter_reading", [0.0])[i] or 0.0),
                        "primary_use": pydict.get("primary_use", [None])[i],
                        "square_feet": pydict.get("square_feet", [None])[i],
                        "floor_count": pydict.get("floor_count", [None])[i],
                        "air_temperature": float(pydict.get("air_temperature", [0.0])[i]) if pydict.get("air_temperature", [None])[i] is not None else None,
                        "dew_temperature": float(pydict.get("dew_temperature", [0.0])[i]) if pydict.get("dew_temperature", [None])[i] is not None else None,
                        "cloud_coverage": float(pydict.get("cloud_coverage", [0.0])[i]) if pydict.get("cloud_coverage", [None])[i] is not None else None,
                        "sea_level_pressure": float(pydict.get("sea_level_pressure", [0.0])[i]) if pydict.get("sea_level_pressure", [None])[i] is not None else None,
                        "wind_speed": float(pydict.get("wind_speed", [0.0])[i]) if pydict.get("wind_speed", [None])[i] is not None else None,
                    }
                    yield record

    # Case 2: CSV file
    elif p.suffix == ".csv":
        with open(p, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                meter_val = int(row["meter"]) if "meter" in row and row["meter"] else 0
                if meter_filter >= 0 and meter_val != meter_filter:
                    continue
                record = {
                    "building_id": int(row.get("building_id", 0)),
                    "site_id": int(row.get("site_id", 0)) if "site_id" in row else 0,
                    "meter": meter_val,
                    "timestamp": row.get("timestamp", ""),
                    "meter_reading": float(row.get("meter_reading", 0.0)),
                    "primary_use": row.get("primary_use", "Office"),
                    "square_feet": float(row.get("square_feet", 0.0)) if "square_feet" in row else 0.0,
                    "floor_count": int(row.get("floor_count", 1)) if "floor_count" in row and row["floor_count"] else 1,
                    "air_temperature": float(row.get("air_temperature", 0.0)) if "air_temperature" in row and row["air_temperature"] else None,
                    "dew_temperature": float(row.get("dew_temperature", 0.0)) if "dew_temperature" in row and row["dew_temperature"] else None,
                    "cloud_coverage": float(row.get("cloud_coverage", 0.0)) if "cloud_coverage" in row and row["cloud_coverage"] else None,
                    "sea_level_pressure": float(row.get("sea_level_pressure", 0.0)) if "sea_level_pressure" in row and row["sea_level_pressure"] else None,
                    "wind_speed": float(row.get("wind_speed", 0.0)) if "wind_speed" in row and row["wind_speed"] else None,
                }
                yield record


def main():
    args = parse_args()

    print("============================================================")
    print("           ASHRAE SMART BUILDING KAFKA PRODUCER             ")
    print("============================================================")
    print(f"[*] Bootstrap Servers : {args.bootstrap_servers}")
    print(f"[*] Kafka Topic       : {args.topic}")
    print(f"[*] Input Data Source : {args.input}")
    print(f"[*] Meter Filter      : {'Electricity (0)' if args.meter == 0 else f'Type {args.meter}'}")
    print(f"[*] Target Rate       : {args.rate if args.rate > 0 else 'MAX (Unlimited)'} msg/s")
    print(f"[*] Batch Size        : {args.batch_size}")
    print(f"[*] Total Limit       : {args.limit if args.limit > 0 else 'ALL'}")
    print(f"[*] Benchmark Log     : {args.benchmark_file}")
    print("============================================================")

    # Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks=1,
        retries=3,
        batch_size=16384,
        linger_ms=10,
    )

    # Prepare benchmark CSV
    benchmark_path = Path(args.benchmark_file)
    write_header = not benchmark_path.exists()
    bench_file = open(benchmark_path, mode="a", newline="", encoding="utf-8")
    bench_writer = csv.writer(bench_file)
    if write_header:
        bench_writer.writerow(["timestamp", "messages_sent", "batch_size", "target_rate", "actual_throughput_msg_sec", "elapsed_sec"])
        bench_file.flush()

    total_sent = 0
    start_time = time.time()
    last_log_time = start_time
    last_sent_count = 0

    sleep_interval = 1.0 / args.rate if args.rate > 0 else 0.0

    print("\n[>] Starting streaming messages to Kafka... (Press Ctrl+C to cancel)\n")

    try:
        data_stream = get_data_iterator(args.input, meter_filter=args.meter)
        for record in data_stream:
            # Key partition by building_id
            b_id = record.get("building_id", 0)
            producer.send(args.topic, key=b_id, value=record)

            total_sent += 1

            # Batch flush
            if total_sent % args.batch_size == 0:
                producer.flush()

            # Throttle if rate is specified
            if sleep_interval > 0:
                time.sleep(sleep_interval)

            # Periodic performance logging
            now = time.time()
            if now - last_log_time >= 2.0:
                elapsed_period = now - last_log_time
                msgs_period = total_sent - last_sent_count
                curr_throughput = msgs_period / elapsed_period
                overall_throughput = total_sent / (now - start_time)

                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] "
                    f"Sent: {total_sent:7d} msgs | "
                    f"Speed: {curr_throughput:8.1f} msg/s | "
                    f"Avg: {overall_throughput:8.1f} msg/s | "
                    f"Building: {b_id}"
                )

                bench_writer.writerow([
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    total_sent,
                    args.batch_size,
                    args.rate,
                    round(curr_throughput, 2),
                    round(now - start_time, 2),
                ])
                bench_file.flush()

                last_log_time = now
                last_sent_count = total_sent

            if args.limit > 0 and total_sent >= args.limit:
                print(f"\n[+] Reached requested limit of {args.limit} messages.")
                break

    except KeyboardInterrupt:
        print("\n[!] Stream interrupted by user.")
    except Exception as e:
        print(f"\n[ERROR] Streaming failed: {e}")
    finally:
        producer.flush()
        producer.close()
        bench_file.close()

        total_elapsed = time.time() - start_time
        final_speed = total_sent / total_elapsed if total_elapsed > 0 else 0
        print("\n============================================================")
        print("                   PRODUCER SUMMARY                         ")
        print("============================================================")
        print(f"[OK] Total Messages Sent : {total_sent:,}")
        print(f"[OK] Total Time Elapsed  : {total_elapsed:.2f} seconds")
        print(f"[OK] Average Throughput  : {final_speed:.1f} messages/second")
        print(f"[OK] Benchmark saved to  : {args.benchmark_file}")
        print("============================================================\n")


if __name__ == "__main__":
    main()
