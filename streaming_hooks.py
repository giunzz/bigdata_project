"""Small integration hooks for Duong's existing Structured Streaming job.

This example measures producer -> durable Parquet sink, not browser rendering.
Replace the sink function only after defining the desired latency endpoint.
"""
import json
import time
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from phuc_analytics import AnalyticsBundle


class ProgressRecorder(StreamingQueryListener):
    def __init__(self, path, run_id, query_name="phuc_scoring"):
        super().__init__()
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.run_id = run_id
        self.query_name = query_name

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        value = json.loads(event.progress.json)
        if value.get("name") != self.query_name:
            return
        value["benchmark_run_id"] = self.run_id
        with self.path.open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(value) + "\n")

    def onQueryTerminated(self, event):
        pass

    def onQueryIdle(self, event):
        pass


def scoring_callback(model_dir, sink_root, receipt_file, run_id):
    """Return a foreachBatch callback. Load once; stable output for retries.

    Input must already have metadata/weather joined and be deduplicated by
    Duong's event-time policy. Require unique event_id and produced_at_ms
    (wall clock epoch milliseconds set by Hung before Kafka send).
    """
    bundle = AnalyticsBundle(model_dir)
    receipt_path = Path(receipt_file)
    receipt_path.parent.mkdir(parents=True, exist_ok=True)

    def callback(batch_df, batch_id):
        needed = {"event_id", "produced_at_ms", "run_id"}
        if needed - set(batch_df.columns):
            raise ValueError("For measured replay, add event_id, produced_at_ms, and run_id at producer.")
        # Materialize the input once. Without this, multiple actions in
        # foreachBatch can inflate Spark's numInputRows and rate metrics.
        batch_df = batch_df.cache()
        scored = None
        try:
            if batch_df.count() == 0:
                return
            mixed = batch_df.filter(F.col("run_id").isNull() | (F.col("run_id") != F.lit(run_id))).limit(1).count()
            if mixed:
                raise ValueError("This measured query must contain one run_id, excluding warm-up and other runs.")
            scored = bundle.score(batch_df).cache()
            if not scored.take(1):
                return
            # A per-batch path makes a retry replace the same output. In MongoDB
            # use an event_id upsert and record time AFTER successful write ACK.
            path = str(Path(sink_root) / str(run_id) / ("batch-" + str(batch_id)))
            scored.write.mode("overwrite").parquet(path)
            ready = time.time_ns() // 1_000_000
            with receipt_path.open("a", encoding="utf-8") as fp:
                for row in scored.select("event_id", "produced_at_ms").toLocalIterator():
                    fp.write(json.dumps({"run_id": run_id, "batch_id": batch_id,
                        "event_id": row["event_id"], "produced_at_ms": row["produced_at_ms"],
                        "ready_at_ms": ready, "endpoint": "durable_parquet_sink"}) + "\n")
        finally:
            if scored is not None:
                scored.unpersist()
            batch_df.unpersist()
    return callback


# In Duong's existing job, AFTER Kafka parsing/enrichment/deduplication:
# listener = ProgressRecorder('measurements/progress.jsonl', run_id='rate100_repeat1')
# spark.streams.addListener(listener)
# callback = scoring_callback('results/real/model', 'scored_sink',
#                             'measurements/receipts.jsonl', 'rate100_repeat1')
# query = (clean_enriched_df.writeStream.queryName('phuc_scoring').foreachBatch(callback)
#          .option('checkpointLocation', 'checkpoints/analytics')
#          .trigger(processingTime='5 seconds').start())
# query.awaitTermination()
# Use the same run_id in runs.csv. This file does not create a Kafka consumer.
