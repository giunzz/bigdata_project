"""Combine real producer, Spark progress, and sink receipts. Missing stays blank."""
import argparse
import csv
import json
import math
import os
from collections import defaultdict
from pathlib import Path


def number(value):
    try:
        x = float(value)
        return x if math.isfinite(x) else None
    except (ValueError, TypeError):
        return None


def mean(values):
    values = [v for v in values if v is not None]
    return sum(values)/len(values) if values else None


def percentile(values, p):
    if not values:
        return None
    a = sorted(values); pos = (len(a)-1)*p
    lo, hi = math.floor(pos), math.ceil(pos)
    return a[lo] + (a[hi]-a[lo])*(pos-lo)


def json_lines(path):
    if not path or not Path(path).is_file():
        return
    for index, line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        if line.strip():
            try:
                yield json.loads(line)
            except json.JSONDecodeError as err:
                raise ValueError(f"Bad JSON in {path} line {index}: {err}") from err


def summarize(runs_file, progress_file, receipts_file):
    with Path(runs_file).open(encoding="utf-8-sig", newline="") as fp:
        runs = list(csv.DictReader(fp))
    seen_run_ids = [r["run_id"] for r in runs]
    if len(seen_run_ids) != len(set(seen_run_ids)):
        raise ValueError("runs.csv needs unique run_id per repeated experiment.")
    progress = defaultdict(dict)
    for item in json_lines(progress_file):
        run_id = item.get("benchmark_run_id")
        if run_id:
            # Repeated callbacks for the same query batch do not double count.
            progress[run_id][(item.get("id"), item.get("batchId"))] = item
    receipts, bad = defaultdict(dict), defaultdict(int)
    for item in json_lines(receipts_file):
        rid = item.get("run_id")
        produced, ready = number(item.get("produced_at_ms")), number(item.get("ready_at_ms"))
        key = str(item.get("event_id") or "")
        if produced is None or ready is None or not key or ready < produced:
            bad[rid] += 1; continue
        if key not in receipts[rid] or ready < receipts[rid][key][0]:
            receipts[rid][key] = (ready, ready-produced, item.get("endpoint", "unspecified"))
    output = []
    for run in runs:
        rid = run["run_id"]
        attempted, sent, seconds = [number(run.get(k)) for k in
                                  ["attempted_messages", "sent_messages", "send_seconds"]]
        if attempted is not None and sent is not None and not 0 <= sent <= attempted:
            raise ValueError(f"Run {rid}: need 0 <= sent_messages <= attempted_messages.")
        window = number(run.get("observation_seconds"))
        batches = [b for b in progress[rid].values() if (number(b.get("numInputRows")) or 0) > 0]
        if len({b.get("id") for b in batches}) > 1:
            raise ValueError(f"Run {rid}: progress mixes multiple Spark queries; select the scoring query only.")
        durations = [number(b.get("durationMs", {}).get("triggerExecution")) for b in batches]
        rows = [number(b.get("numInputRows")) for b in batches]
        active_seconds = sum(d or 0 for d in durations)/1000
        run_receipts = receipts.get(rid, {})
        latencies = [entry[1] for entry in run_receipts.values()]
        endpoints = sorted({entry[2] for entry in run_receipts.values()})
        # Never combine distinct timing endpoints in a single latency statistic.
        if len(endpoints) > 1:
            raise ValueError(f"Run {rid} mixes latency endpoints: {endpoints}")
        record = dict(run)
        record.update({"producer_mps": sent/seconds if sent is not None and seconds and seconds > 0 else None,
            "send_success_pct": 100*sent/attempted if sent is not None and attempted and attempted > 0 else None,
            "spark_input_rps_mean": mean([number(b.get("inputRowsPerSecond")) for b in batches]),
            "spark_processing_rps_mean": mean([number(b.get("processedRowsPerSecond")) for b in batches]),
            "spark_processing_capacity_rps": sum(r or 0 for r in rows)/active_seconds if active_seconds > 0 else None,
            "batch_duration_ms_mean": mean(durations), "batch_duration_ms_p95": percentile([d for d in durations if d is not None], .95),
            "measured_nonempty_batches": len(batches),
            "unique_delivered_events": len(latencies) if run_receipts else None,
            "delivered_events_per_second": len(latencies)/window if latencies and window and window > 0 else None,
            "latency_endpoint": endpoints[0] if endpoints else None,
            "latency_ms_mean": mean(latencies), "latency_ms_p50": percentile(latencies, .5),
            "latency_ms_p95": percentile(latencies, .95), "latency_ms_p99": percentile(latencies, .99),
            "invalid_receipt_rows": bad[rid],
            "backlog_end_messages": number(run.get("backlog_end_messages"))})
        output.append(record)
    return output


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--runs", required=True)
    p.add_argument("--progress"); p.add_argument("--receipts")
    p.add_argument("--output", required=True)
    args = p.parse_args()
    result = summarize(args.runs, args.progress, args.receipts)
    if not result:
        raise ValueError("No run rows.")
    out = Path(args.output); out.mkdir(parents=True, exist_ok=True)
    fields = list(dict.fromkeys(k for row in result for k in row))
    with (out / "benchmark_summary.csv").open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields); writer.writeheader(); writer.writerows(result)
    print("Saved", out / "benchmark_summary.csv")
    # Leave the chart absent if there are no actual delivered-rate measurements.
    plot_rows = [r for r in result if number(r.get("target_rate_mps")) is not None
                 and r["delivered_events_per_second"] is not None]
    if plot_rows:
        os.environ["MPLBACKEND"] = "Agg"
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(8, 4))
        plot_rows.sort(key=lambda r: float(r["target_rate_mps"]))
        ax.scatter([float(r["target_rate_mps"]) for r in plot_rows],
                   [r["delivered_events_per_second"] for r in plot_rows], label="Measured delivered throughput")
        ax.set(xlabel="Target producer rate (messages/s)", ylabel="Delivered events/s")
        ax.legend(); fig.tight_layout(); fig.savefig(out / "throughput.png", dpi=180); plt.close(fig)


if __name__ == "__main__":
    main()
