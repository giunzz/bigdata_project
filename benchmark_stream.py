"""Measure ASHRAE replay -> Kafka -> saved Spark RF -> committed Parquet batches.

Full enriched events use the group's Silver columns plus measurement IDs.
The latency endpoint is return from the Parquet batch write, not a dashboard,
database acknowledgement, or a claim of power-loss durability.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import threading
import time
import uuid

from confluent_kafka import Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic, OffsetSpec
import pyarrow.dataset as ds
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.streaming import StreamingQueryListener

if __package__:
    from .analytics import AnalyticsBundle
    from .kafka_runtime import KafkaRuntime, SPARK_VERSION, KAFKA_VERSION
    from .prepare_group import COLUMNS
else:
    from analytics import AnalyticsBundle
    from kafka_runtime import KafkaRuntime, SPARK_VERSION, KAFKA_VERSION
    from prepare_group import COLUMNS

ROOT = Path(__file__).resolve().parent


def append(path, record):
    with Path(path).open("a", encoding="utf-8") as output:
        output.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n")


def write_csv(path, rows):
    if not rows:
        return
    with Path(path).open("w", newline="", encoding="utf-8-sig") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
        output.flush()
        os.fsync(output.fileno())


def freeze_jsonl(path, records):
    """Export the complete in-memory log after timing; verify its actual bytes."""
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".complete")
    digest = hashlib.sha256()
    with temporary.open("wb") as output:
        for record in records:
            encoded = (json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")
            output.write(encoded)
            digest.update(encoded)
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)
    actual = path.read_bytes()
    if hashlib.sha256(actual).hexdigest() != digest.hexdigest() or len(actual.splitlines()) != len(records):
        raise RuntimeError(f"Incomplete canonical export: {path}")
    return digest.hexdigest()


def run(args, runtime):
    out = args.output.resolve()
    if out.exists():
        raise ValueError("Choose a new benchmark output directory.")
    out.mkdir(parents=True)
    model_root = args.training.resolve()
    config = json.loads((model_root / "model/config.json").read_text(encoding="utf-8"))
    if (config.get("dataset") != "ASHRAE_GROUP_SILVER" or config.get("data_origin") != "real"
            or not config.get("source_provenance")):
        raise ValueError("Use the separately trained ASHRAE model; BDG2 models are not reused here.")
    replay_dataset = ds.dataset(model_root / "replay_test", format="parquet")
    if replay_dataset.count_rows() > args.max_replay_rows:
        raise ValueError("Replay subset exceeds the declared driver limit; select fewer buildings/days.")
    table = replay_dataset.to_table()
    original = sorted(table.to_pylist(), key=lambda r: (r["timestamp"], str(r["building_id"])))
    if not original:
        raise ValueError("Held-out replay input is empty.")
    reference = {(str(r["building_id"]), r["timestamp"]): float(r["prediction"]) for r in original}
    energy = []
    for row in original:
        event = {c: row[c] for c in COLUMNS}
        for c in ("building_id", "site_id"):
            event[c] = str(event[c])
        event["meter"] = int(event["meter"])
        for c in set(COLUMNS) - {"building_id", "site_id", "meter", "timestamp", "primary_use"}:
            value = event[c]
            event[c] = float(value) if value is not None and math.isfinite(float(value)) else None
        energy.append(event)
    os.environ["PYSPARK_PYTHON"] = os.sys.executable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    jars = ",".join(str(p) for p in sorted((runtime.root / "jars").glob("*.jar")))
    spark = (SparkSession.builder.master("local[2]").appName("PhucASHRAEParquetBenchmark")
             .config("spark.jars", jars).config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4").config("spark.ui.showConsoleProgress", "false")
             .config("spark.sql.streaming.noDataMicroBatches.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    if spark.version != SPARK_VERSION:
        raise ValueError(f"This isolated experiment requires Spark {SPARK_VERSION}; got {spark.version}.")
    exp = "ashrae_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]
    topic = exp
    admin = AdminClient({"bootstrap.servers": runtime.bootstrap})
    for future in admin.create_topics([NewTopic(topic, num_partitions=args.partitions, replication_factor=1)]).values():
        future.result(timeout=30)
    experiment = {"experiment_id": exp, "dataset": config["dataset"], "source_provenance": config["source_provenance"],
                  "model": str(model_root), "replay_source_rows": len(energy), "rates": args.rates,
                  "repeats": args.repeats, "seconds": args.seconds, "warmup_seconds": args.warmup,
                  "trigger_seconds": args.trigger, "partitions": args.partitions,
                  "spark_version": spark.version, "kafka_version": KAFKA_VERSION,
                  "python_version": platform.python_version(), "platform": platform.platform(),
                  "scope": "single-machine ASHRAE Kafka-Spark-RF-Parquet experiment",
                  "latency_endpoint": "Parquet write returned successfully for the micro-batch",
                  "excluded": ["dashboard rendering", "MongoDB", "network between physical machines"],
                  "delivery": "checkpoint plus overwrite at stable batch_id path; no universal exactly-once claim"}
    (out / "experiment.json").write_text(json.dumps(experiment, ensure_ascii=False, indent=2), encoding="utf-8")
    bundle = AnalyticsBundle(model_root / "model")
    lock = threading.RLock()
    state = {"phase": "initializing", "completed": {i: 0 for i in range(args.partitions)},
             "delivered": defaultdict(dict), "batch_run": {}, "fatal": None}
    stop = threading.Event()
    checks, batches, samples, progress_rows = [], [], [], []
    schema = T.StructType([T.StructField(c, T.StringType() if c in {"building_id", "site_id", "timestamp", "primary_use"}
                          else T.IntegerType() if c == "meter" else T.DoubleType()) for c in COLUMNS]
                         + [T.StructField("event_id", T.StringType()), T.StructField("run_id", T.StringType()),
                            T.StructField("produced_at_ms", T.LongType())])

    class Listener(StreamingQueryListener):
        def onQueryStarted(self, event):
            pass

        def onQueryIdle(self, event):
            pass

        def onQueryTerminated(self, event):
            if event.exception:
                with lock:
                    state["fatal"] = str(event.exception)

        def onQueryProgress(self, event):
            progress = json.loads(event.progress.json)
            with lock:
                progress["run_id"] = state["batch_run"].get(progress["batchId"])
                for source in progress.get("sources", []):
                    offsets = source.get("endOffset")
                    if isinstance(offsets, str):
                        offsets = json.loads(offsets)
                    if offsets and topic in offsets:
                        state["completed"] = {int(k): int(v) for k, v in offsets[topic].items()}
                progress_rows.append(progress)
            append(out / "progress.jsonl", progress)

    def callback(batch, batch_id):
        start = time.perf_counter()
        batch = batch.cache()
        scored = None
        try:
            n = batch.count()
            if n == 0:
                return
            runs = [r[0] for r in batch.select("run_id").distinct().collect()]
            if len(runs) != 1 or not runs[0]:
                raise ValueError("Benchmark batches must contain exactly one run_id.")
            rid = runs[0]
            with lock:
                state["batch_run"][batch_id] = rid
            score_start = time.perf_counter()
            scored = bundle.score(batch.dropDuplicates(["event_id"])).cache()
            receipts = scored.select("event_id", "run_id", "produced_at_ms", "building_id", "prediction",
                                     F.date_format("timestamp", "yyyy-MM-dd HH:mm:ss").alias("timestamp")).collect()
            score_seconds = time.perf_counter() - score_start
            if len(receipts) != n or any(r.event_id is None for r in receipts):
                raise ValueError("Invalid/dropped/duplicate replay events; benchmark is not accepted.")
            for row in receipts[:5]:
                expected = reference[(row.building_id, row.timestamp)]
                error = abs(row.prediction - expected)
                if error > 1e-8:
                    raise ValueError(f"Offline/online prediction mismatch: {error}")
                checks.append({"run_id": rid, "batch_id": batch_id, "event_id": row.event_id, "absolute_error": error})
            write_start = time.perf_counter()
            batch_path = out / "parquet" / f"batch_{batch_id:08d}"
            scored.write.mode("overwrite").parquet(str(batch_path))
            ready = time.time_ns() // 1_000_000
            write_seconds = time.perf_counter() - write_start
            with lock:
                for row in receipts:
                    receipt = {"event_id": row.event_id, "run_id": rid, "batch_id": batch_id,
                               "produced_at_ms": row.produced_at_ms, "ready_at_ms": ready,
                               "latency_ms": ready - row.produced_at_ms}
                    state["delivered"][rid].setdefault(row.event_id, receipt)
            record = {"run_id": rid, "batch_id": batch_id, "input_rows": n, "scored_rows": len(receipts),
                      "score_seconds": score_seconds, "parquet_write_seconds": write_seconds,
                      "callback_seconds": time.perf_counter() - start}
            batches.append(record)
            append(out / "batches.jsonl", record)
            print(f"BATCH {batch_id}: {rid}, {n} events, {record['callback_seconds']:.2f}s", flush=True)
        except Exception as error:
            with lock:
                state["fatal"] = repr(error)
            raise
        finally:
            if scored is not None:
                scored.unpersist()
            batch.unpersist()

    listener = Listener()
    spark.streams.addListener(listener)
    source = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", runtime.bootstrap)
              .option("subscribe", topic).option("startingOffsets", "earliest").load())
    events = source.select(F.from_json(F.col("value").cast("string"), schema).alias("event")).select("event.*")
    query = (events.writeStream.queryName(exp).foreachBatch(callback)
             .option("checkpointLocation", str(out / "checkpoint"))
             .trigger(processingTime=f"{args.trigger} seconds").start())

    def snapshot(marker):
        requested = {TopicPartition(topic, i): OffsetSpec.latest() for i in range(args.partitions)}
        latest = {key.partition: future.result(timeout=10).offset for key, future in admin.list_offsets(requested).items()}
        with lock:
            completed = dict(state["completed"])
            rid = state["phase"]
        sample = {"run_id": rid, "sampled_at_ms": time.time_ns() // 1_000_000,
                  "marker": marker, "latest_next_offsets": latest, "completed_next_offsets": completed,
                  "backlog": sum(max(0, latest[i] - completed.get(i, 0)) for i in latest)}
        with lock:
            samples.append(sample)
        append(out / "backlog.jsonl", sample)
        return sample

    def monitor():
        while not stop.wait(1):
            try:
                snapshot("periodic")
            except Exception as error:
                append(out / "monitor_errors.jsonl", {"error": repr(error)})

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    producer = Producer({"bootstrap.servers": runtime.bootstrap, "acks": "all", "enable.idempotence": True, "linger.ms": 5})
    run_rows = []

    def check():
        with lock:
            error = state["fatal"]
        if error or not query.isActive:
            raise RuntimeError(error or "Streaming query stopped.")

    def phase(rid, rate, seconds, warmup):
        ack = {"ok": 0, "errors": []}
        with lock:
            state["phase"] = rid
        snapshot("before_send")
        started = time.perf_counter()
        start_ms = time.time_ns() // 1_000_000
        n = rate * seconds

        def delivered(error, message):
            if error:
                ack["errors"].append(str(error))
            else:
                ack["ok"] += 1

        for i in range(n):
            due = started + i / rate
            while time.perf_counter() < due:
                producer.poll(0)
                time.sleep(min(.002, max(0, due - time.perf_counter())))
            if i % rate == 0:
                check()
            event = dict(energy[i % len(energy)])
            event.update(event_id=f"{rid}:{i:09d}", run_id=rid, produced_at_ms=time.time_ns() // 1_000_000)
            payload = json.dumps(event, allow_nan=False).encode("utf-8")
            while True:
                try:
                    producer.produce(topic, key=event["building_id"], value=payload, on_delivery=delivered)
                    break
                except BufferError:
                    producer.poll(.01)
            producer.poll(0)
        while time.perf_counter() < started + seconds:
            producer.poll(0)
            time.sleep(.001)
        pending = producer.flush(30)
        sent_at = time.perf_counter()
        if pending or ack["errors"] or ack["ok"] != n:
            raise RuntimeError(f"Producer delivery failed: {ack}, pending={pending}")
        backlog_send = snapshot("after_send")["backlog"]
        deadline = time.monotonic() + args.drain_timeout
        while time.monotonic() < deadline:
            check()
            with lock:
                done = len(state["delivered"][rid])
            if done == n and snapshot("drain_check")["backlog"] == 0:
                break
            time.sleep(.2)
        else:
            raise RuntimeError(f"Drain timeout for {rid}.")
        snapshot("after_delivery")
        with lock:
            receipts = sorted(state["delivered"][rid].values(), key=lambda r: r["event_id"])
        for receipt in receipts:
            append(out / "receipts.jsonl", receipt)
        last_ready = max(r["ready_at_ms"] for r in receipts)
        total_seconds = (last_ready - start_ms) / 1000
        row = {"run_id": rid, "rate": rate, "warmup": warmup, "seconds": seconds,
               "sent": ack["ok"], "delivered": len(receipts), "producer_ack_per_second": n / (sent_at - started),
               "delivered_per_second": n / total_seconds, "backlog_at_send_end": backlog_send,
               "backlog_after_drain": 0, "drain_seconds": max(0, (last_ready - start_ms) / 1000 - (sent_at - started))}
        run_rows.append(row)
        write_csv(out / "runs.csv", run_rows)
        print("RUN", json.dumps(row), flush=True)

    succeeded = False
    try:
        for rate in args.rates:
            for repeat in range(1, args.repeats + 1):
                prefix = f"{exp}_r{rate}_n{repeat}"
                if args.warmup:
                    phase(prefix + "_warmup", rate, args.warmup, True)
                phase(prefix, rate, args.seconds, False)
        final_offsets = snapshot("experiment_complete")
        query.stop()
        stop.set()
        thread.join(timeout=12)
        paths = sorted((out / "parquet").glob("batch_*"))
        stored = spark.read.parquet(*map(str, paths))
        stored_rows = stored.count()
        unique_rows = stored.select("event_id").distinct().count()
        sent = sum(r["sent"] for r in run_rows)
        if stored_rows != unique_rows or stored_rows != sent:
            raise RuntimeError(f"Parquet verification failed: rows={stored_rows}, unique={unique_rows}, sent={sent}")
        if (final_offsets["backlog"] != 0 or sum(final_offsets["latest_next_offsets"].values()) != sent
                or sum(final_offsets["completed_next_offsets"].values()) != sent):
            raise RuntimeError("Final source offsets do not match all delivered events.")
        with lock:
            canonical_receipts = [receipt for delivered in state["delivered"].values() for receipt in delivered.values()]
            frozen_progress = list(progress_rows)
            frozen_samples = list(samples)
        canonical_hashes = {
            "receipts.jsonl": freeze_jsonl(out / "receipts.jsonl", canonical_receipts),
            "backlog.jsonl": freeze_jsonl(out / "backlog.jsonl", frozen_samples),
            "batches.jsonl": freeze_jsonl(out / "batches.jsonl", batches),
            "progress.jsonl": freeze_jsonl(out / "progress.jsonl", frozen_progress),
        }
        write_csv(out / "runs.csv", run_rows)
        preview = stored.orderBy(F.desc("is_anomaly"), "timestamp").limit(1000).toPandas()
        preview.to_csv(out / "alerts_preview.csv", index=False)
        (out / "prediction_checks.json").write_text(json.dumps(checks, indent=2), encoding="utf-8")
        (out / "verified.json").write_text(json.dumps({"status": "passed", "stored_rows": stored_rows,
            "unique_event_ids": unique_rows, "sent_including_warmup": sent, "offline_online_checks": len(checks),
            "maximum_prediction_error": max(c["absolute_error"] for c in checks),
            "measured_runs": sum(not r["warmup"] for r in run_rows),
            "final_kafka_next_offsets": final_offsets["latest_next_offsets"],
            "final_completed_next_offsets": final_offsets["completed_next_offsets"],
            "final_backlog": final_offsets["backlog"],
            "canonical_log_sha256": canonical_hashes,
            "log_export": "complete in-memory logs exported after timing with fsync and byte verification"}, indent=2), encoding="utf-8")
        succeeded = True
    finally:
        stop.set()
        if query.isActive:
            query.stop()
        thread.join(timeout=12)
        spark.streams.removeListener(listener)
        spark.stop()
        if not succeeded:
            (out / "INCOMPLETE.txt").write_text("This run did not pass verification. Do not report its performance as final.\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--training", type=Path, default=ROOT / "results/ashrae_model")
    parser.add_argument("--output", type=Path, default=ROOT / "results/ashrae_benchmark")
    parser.add_argument("--runtime", type=Path, default=ROOT / "runtime")
    parser.add_argument("--rates", default="100,500,1000")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--warmup", type=int, default=30)
    parser.add_argument("--trigger", type=int, default=5)
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--drain-timeout", type=int, default=300)
    parser.add_argument("--max-replay-rows", type=int, default=200000)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    args.rates = [int(x) for x in args.rates.split(",")]
    if args.smoke:
        args.rates, args.repeats, args.seconds, args.warmup = [10], 1, 10, 5
    if not args.rates or min(args.rates) < 1 or min(args.repeats, args.seconds, args.trigger, args.partitions) < 1 or args.warmup < 0:
        parser.error("Use positive rates/repeats/seconds/trigger/partitions and nonnegative warmup.")
    runtime = KafkaRuntime(args.runtime)
    try:
        runtime.start()
        run(args, runtime)
    finally:
        runtime.stop()


if __name__ == "__main__":
    main()
