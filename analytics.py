"""Additional ASHRAE Spark MLlib model; the original BDG2 module is unchanged.

Run ``python analytics.py --help``. Synthetic data is only a code check.
Prediction estimates expected consumption at the observed hour, using that
hour's weather. It is not a validated future-weather forecasting system.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession, functions as F

NUMERIC = ["air_temperature", "dew_temperature", "cloud_coverage",
           "sea_level_pressure", "wind_speed", "log_square_feet",
           "floor_count", "year_built"]
CATEGORIES = ["building_key", "site_key", "primary_use"]
CALENDAR = ["hour_sin", "hour_cos", "weekday_sin", "weekday_cos",
            "month_sin", "month_cos", "is_weekend"]


def write_json(path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2,
                               allow_nan=False), encoding="utf-8")


def spark_session(master="local[2]"):
    return (SparkSession.builder.master(master).appName("PhucEnergyAnalytics")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())


def finite(col):
    return col.isNotNull() & ~F.isnan(col) & (F.abs(col) != float("inf"))


def read_input(spark, path, fmt="auto"):
    path = str(path)
    if fmt == "auto":
        fmt = "csv" if path.lower().endswith(".csv") else "parquet"
    if fmt == "csv":
        return spark.read.option("header", True).csv(path)
    if fmt == "delta":
        raise ValueError("Export Gold as Parquet first, or configure the group's "
                         "matching Delta/Spark environment before changing this reader.")
    return spark.read.parquet(path)


def normalize(df, require_target=True):
    required = {"building_id", "timestamp"}
    if require_target:
        required.add("meter_reading")
    missing = required - set(df.columns)
    if missing:
        raise ValueError("Missing columns: " + ", ".join(sorted(missing)))
    for name, default in [("meter", 0), ("site_id", -1),
                          ("primary_use", "UNKNOWN")]:
        if name not in df.columns:
            df = df.withColumn(name, F.lit(default))
    if "meter_reading" not in df.columns:
        df = df.withColumn("meter_reading", F.lit(None).cast("double"))
    df = (df.withColumn("timestamp", F.to_timestamp("timestamp"))
          .withColumn("building_id", F.trim(F.col("building_id").cast("string")))
          .withColumn("meter", F.col("meter").cast("int"))
          .withColumn("site_id", F.trim(F.col("site_id").cast("string")))
          .withColumn("meter_reading", F.col("meter_reading").cast("double")))
    # Zero is valid and can indicate a drop. Extreme positive readings are kept.
    df = df.filter(F.col("timestamp").isNotNull()
                   & F.col("building_id").isNotNull()
                   & (F.length("building_id") > 0)
                   & ~F.lower(F.col("building_id")).isin("nan", "none", "null")
                   & (F.col("meter") == 0))
    if require_target:
        df = df.filter(finite(F.col("meter_reading")) & (F.col("meter_reading") >= 0))
    for name in set(NUMERIC + ["square_feet"]) - {"log_square_feet"}:
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast("double"))
        df = df.withColumn(name, F.col(name).cast("double"))
        df = df.withColumn(name, F.when(finite(F.col(name)), F.col(name)))
    df = df.withColumn("log_square_feet", F.when(F.col("square_feet") >= 0,
                                                F.log1p("square_feet")))
    return (df.withColumn("building_key", F.col("building_id").cast("string"))
            .withColumn("site_key", F.coalesce(F.col("site_id").cast("string"),
                                              F.lit("UNKNOWN")))
            .withColumn("primary_use", F.coalesce(F.col("primary_use"),
                                                 F.lit("UNKNOWN"))))


def feature_frame(df, medians):
    for name in NUMERIC:
        df = df.withColumn(name + "_missing", F.col(name).isNull().cast("double"))
        df = df.withColumn(name, F.coalesce(F.col(name), F.lit(medians[name])))
    for prefix, expr, period in [
            ("hour", F.hour("timestamp"), 24.0),
            ("weekday", F.dayofweek("timestamp") - 1, 7.0),
            ("month", F.month("timestamp") - 1, 12.0)]:
        angle = expr * (2 * math.pi / period)
        df = df.withColumn(prefix + "_sin", F.sin(angle))
        df = df.withColumn(prefix + "_cos", F.cos(angle))
    return df.withColumn("is_weekend", F.dayofweek("timestamp").isin(1, 7).cast("double"))


def chronological_split(df):
    # Split complete timestamps, never scatter buildings from the same hour.
    # Collect formatted wall-clock strings so the Python host timezone cannot
    # change split boundaries or the timestamps written to metrics.json.
    times = (df.select("timestamp").distinct().orderBy("timestamp")
             .select(F.date_format("timestamp", "yyyy-MM-dd HH:mm:ss.SSSSSS")
                     .alias("timestamp_string")).collect())
    if len(times) < 24:
        raise ValueError("Need at least 24 distinct timestamps; use several weeks in practice.")
    a, b = int(len(times) * .70), int(len(times) * .85)
    s1, s2 = times[a][0], times[b][0]
    t1, t2 = F.to_timestamp(F.lit(s1)), F.to_timestamp(F.lit(s2))
    return (df.filter(F.col("timestamp") < t1),
            df.filter((F.col("timestamp") >= t1) & (F.col("timestamp") < t2)),
            df.filter(F.col("timestamp") >= t2),
            datetime.fromisoformat(s1), datetime.fromisoformat(s2))


def regression_metrics(df, prediction="prediction"):
    y, p = F.col("meter_reading"), F.col(prediction)
    row = df.agg(F.count("*").alias("n"),
                 F.avg(F.abs(y - p)).alias("mae"),
                 F.avg((y - p) ** 2).alias("mse"),
                 F.var_pop(y).alias("target_variance")).first()
    variance = row["target_variance"]
    return {"n": row["n"], "MAE": row["mae"],
            "RMSE": math.sqrt(row["mse"]) if row["mse"] is not None else None,
            "R2": 1 - row["mse"] / variance if variance and variance > 0 else None}


def baseline_prediction(train, test):
    # Only training labels contribute to the baseline.
    def keys(df):
        return (df.withColumn("baseline_hour", F.hour("timestamp"))
                .withColumn("baseline_weekday", F.dayofweek("timestamp")))
    train, test = keys(train), keys(test)
    cols = ["building_id", "baseline_hour", "baseline_weekday"]
    lookup = train.groupBy(*cols).agg(F.avg("meter_reading").alias("schedule_mean"))
    building = train.groupBy("building_id").agg(F.avg("meter_reading").alias("building_mean"))
    mean = train.agg(F.avg("meter_reading")).first()[0]
    return (test.join(lookup, cols, "left").join(building, "building_id", "left")
            .withColumn("baseline_prediction", F.coalesce("schedule_mean", "building_mean", F.lit(mean))))


def calibrate_thresholds(validation, quantile=.99, minimum_rows=48):
    val = validation.withColumn("abs_residual", F.abs(F.col("meter_reading") - F.col("prediction")))
    pooled = max(float(val.approxQuantile("abs_residual", [quantile], .001)[0]), 1e-6)
    rows = val.groupBy("building_id").agg(
        F.count("*").alias("n"),
        F.percentile_approx("abs_residual", quantile, 10000).alias("threshold")).collect()
    by_building = {str(r["building_id"]): max(float(r["threshold"]), 1e-6)
                   for r in rows if r["n"] >= minimum_rows}
    return {"quantile": quantile, "minimum_validation_rows": minimum_rows,
            "pooled_threshold": pooled, "by_building": by_building,
            "calibration": "validation only; normality of historical rows is unverified"}


def add_alerts(predictions, thresholds):
    entries = [(str(k), float(v)) for k, v in thresholds["by_building"].items()]
    if entries:
        lookup = predictions.sparkSession.createDataFrame(entries,
                    "building_id string, calibrated_threshold double")
        predictions = predictions.join(F.broadcast(lookup), "building_id", "left")
        th = F.coalesce(F.col("calibrated_threshold"), F.lit(thresholds["pooled_threshold"]))
    else:
        th = F.lit(thresholds["pooled_threshold"])
    residual = F.col("meter_reading") - F.col("prediction")
    return (predictions.withColumn("threshold", th)
            .withColumn("residual", residual)
            .withColumn("abs_residual", F.abs(residual))
            .withColumn("is_anomaly", F.abs(residual) > F.col("threshold"))
            .withColumn("severity", F.when(F.col("abs_residual") > 2 * F.col("threshold"), "CRITICAL")
                        .when(F.col("is_anomaly"), "WARNING").otherwise("NORMAL")))


class AnalyticsBundle:
    """Load once on the driver; call .score(batch_df) inside foreachBatch."""
    def __init__(self, model_dir):
        self.root = Path(model_dir)
        self.config = json.loads((self.root / "config.json").read_text(encoding="utf-8"))
        self.thresholds = json.loads((self.root / "thresholds.json").read_text(encoding="utf-8"))
        self.model = PipelineModel.load(str(self.root / "pipeline"))

    def score(self, batch_df):
        if batch_df.isStreaming:
            raise ValueError("Pass the batch DataFrame inside foreachBatch, not a streaming DataFrame.")
        prepared = feature_frame(normalize(batch_df), self.config["medians"])
        predicted = self.model.transform(prepared).withColumn("prediction", F.greatest(F.lit(0.0), F.col("prediction")))
        predicted = add_alerts(predicted, self.thresholds)
        columns = ["building_id", "site_id", "meter", "timestamp", "meter_reading",
                   "prediction", "residual", "abs_residual", "threshold", "is_anomaly", "severity"]
        columns += [c for c in ["event_id", "produced_at_ms", "run_id", "scenario", "is_injected"]
                    if c in predicted.columns]
        columns += [c for c in ["kafka_partition", "kafka_offset", "kafka_created_at",
                                "event_time", "validation_status"] if c in predicted.columns]
        return predicted.select(*columns)


def demo_data(path, days=45, buildings=4):
    rng, start = random.Random(42), datetime(2016, 1, 1)
    fields = ["building_id", "site_id", "meter", "timestamp", "meter_reading",
              "primary_use", "square_feet", "year_built", "floor_count",
              "air_temperature", "dew_temperature", "cloud_coverage", "sea_level_pressure", "wind_speed"]
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields); writer.writeheader()
        for hour in range(days * 24):
            ts = start + timedelta(hours=hour)
            temp = 23 + 7 * math.sin((ts.hour - 9) * math.pi / 12) + rng.gauss(0, .5)
            for b in range(buildings):
                work = 8 <= ts.hour < 18 and ts.weekday() < 5
                y = max(0, 25 + b * 15 + (35 + b * 3) * work + max(temp - 24, 0) * 2 + rng.gauss(0, 2))
                writer.writerow(dict(building_id=b, site_id=1, meter=0, timestamp=str(ts),
                    meter_reading=round(y, 3), primary_use="Education", square_feet=15000 + 5000*b,
                    year_built=1990+b, floor_count=3+b, air_temperature=round(temp, 3),
                    dew_temperature=round(temp-5, 3), cloud_coverage=2,
                    sea_level_pressure=1012, wind_speed=2.5))
    write_json(path.with_suffix(".provenance.json"), {"data_origin": "SYNTHETIC_CODE_CHECK", "days": days, "buildings": buildings})
    print("Created synthetic code-check data:", path)


def prepare_ashrae(spark, args):
    root = Path(args.raw_dir)
    metadata = next((root / n for n in ["building_metadata.csv", "building_meta.csv"] if (root / n).is_file()), None)
    if metadata is None:
        raise ValueError("Need building_metadata.csv (or building_meta.csv), train.csv, weather_train.csv.")
    meta = spark.read.option("header", True).csv(str(metadata))
    meta = meta.withColumn("building_id", F.col("building_id").cast("long")).withColumn("site_id", F.col("site_id").cast("long"))
    if args.site_id is not None:
        meta = meta.filter(F.col("site_id") == args.site_id)
    chosen = meta.orderBy("building_id").limit(args.building_limit).cache()
    if chosen.count() == 0:
        raise ValueError("No buildings selected.")
    ids = [r[0] for r in chosen.select("building_id").collect()]
    meter = (spark.read.option("header", True).csv(str(root / "train.csv"))
             .withColumn("building_id", F.col("building_id").cast("long"))
             .withColumn("meter", F.col("meter").cast("int"))
             .withColumn("timestamp", F.to_timestamp("timestamp"))
             .filter((F.col("meter") == 0) & F.col("building_id").isin(ids)))
    if args.max_days > 0:
        first = meter.agg(F.min("timestamp")).first()[0]
        if first is None:
            raise ValueError("No electricity readings for selected buildings.")
        meter = meter.filter(F.col("timestamp") < F.lit(first + timedelta(days=args.max_days)))
    weather = (spark.read.option("header", True).csv(str(root / "weather_train.csv"))
               .withColumn("site_id", F.col("site_id").cast("long"))
               .withColumn("timestamp", F.to_timestamp("timestamp")))
    if chosen.groupBy("building_id").count().filter("count > 1").count():
        raise ValueError("Duplicate building metadata keys; resolve before joining.")
    if weather.groupBy("site_id", "timestamp").count().filter("count > 1").count():
        raise ValueError("Duplicate weather keys; resolve before joining.")
    joined = meter.join(chosen, "building_id", "left").join(weather, ["site_id", "timestamp"], "left")
    # Kaggle's site-0 electricity is kBTU; only this raw ASHRAE route converts it.
    joined = joined.withColumn("meter_reading", F.col("meter_reading").cast("double"))
    joined = joined.withColumn("meter_reading", F.when(F.col("site_id") == 0,
                             F.col("meter_reading") * .2931).otherwise(F.col("meter_reading")))
    valid = normalize(joined).cache()
    if valid.groupBy("building_id", "meter", "timestamp").agg(
            F.countDistinct("meter_reading").alias("values")).filter("values > 1").limit(1).count():
        raise ValueError("Conflicting raw readings for the same building/meter/hour; resolve before preparing Gold.")
    clean = valid.dropDuplicates(["building_id", "meter", "timestamp"]).cache()
    n = clean.count()
    clean.write.mode("errorifexists").parquet(str(args.output))
    write_json(str(args.output) + "_provenance.json", {"source": "ASHRAE train.csv",
        "building_ids": ids, "rows": n, "site_0_conversion": "kBTU * 0.2931 -> kWh",
        "unit": "kWh", "max_days": args.max_days})
    print("Prepared Gold:", args.output, "rows:", n, "buildings:", ids)


def train_model(spark, args):
    out = Path(args.output)
    if out.exists() and any(out.iterdir()):
        raise ValueError("Output already contains files. Choose a new --output directory.")
    out.mkdir(parents=True, exist_ok=True)
    raw = read_input(spark, args.input, args.format)
    raw_n = raw.count()
    valid = normalize(raw).cache(); valid_n = valid.count()
    conflict = valid.groupBy("building_id", "meter", "timestamp").agg(
        F.countDistinct("meter_reading").alias("values")).filter("values > 1").limit(1).count()
    if conflict:
        raise ValueError("Conflicting readings for the same building/meter/hour. Resolve in ETL before training.")
    # Count valid duplicate keys before deduplication. Conflicting duplicates need ETL review.
    df = valid.dropDuplicates(["building_id", "meter", "timestamp"]).cache()
    n = df.count()
    train, val, test, t1, t2 = chronological_split(df)
    sizes = [x.count() for x in [train, val, test]]
    if min(sizes) == 0:
        raise ValueError("Empty split; check time coverage.")
    medians = {c: float(values[0]) if values else 0.0
               for c, values in zip(NUMERIC, train.approxQuantile(NUMERIC, [.5], .001))}
    train_f = feature_frame(train, medians).cache()
    val_f, test_f = feature_frame(val, medians), feature_frame(test, medians)
    index_cols, encoded_cols = [c+"_index" for c in CATEGORIES], [c+"_encoded" for c in CATEGORIES]
    index = StringIndexer(inputCols=CATEGORIES, outputCols=index_cols, handleInvalid="keep")
    encode = OneHotEncoder(inputCols=index_cols, outputCols=encoded_cols, dropLast=False, handleInvalid="keep")
    inputs = NUMERIC + [c+"_missing" for c in NUMERIC] + CALENDAR + encoded_cols
    assemble = VectorAssembler(inputCols=inputs, outputCol="features")
    rf = RandomForestRegressor(labelCol="meter_reading", featuresCol="features",
        predictionCol="prediction", numTrees=args.trees, maxDepth=args.depth,
        maxBins=64, seed=42, minInstancesPerNode=3)
    started = time.perf_counter()
    model = Pipeline(stages=[index, encode, assemble, rf]).fit(train_f)
    duration = time.perf_counter() - started
    val_p = model.transform(val_f).withColumn("prediction", F.greatest(F.lit(0.0), F.col("prediction"))).cache()
    test_p = model.transform(test_f).withColumn("prediction", F.greatest(F.lit(0.0), F.col("prediction"))).cache()
    metrics = {"data_origin": args.origin, "unit": args.unit, "rows_raw": raw_n,
        "rows_invalid_or_not_electricity": raw_n-valid_n, "duplicate_rows_removed": valid_n-n,
        "split_rows": dict(zip(["train", "validation", "test"], sizes)),
        "validation_start": str(t1), "test_start": str(t2),
        "RF_validation": regression_metrics(val_p), "RF_test": regression_metrics(test_p),
        "schedule_baseline_test": regression_metrics(baseline_prediction(train, test), "baseline_prediction"),
        "training_seconds": duration, "Spark_version": spark.version,
        "prediction_task": "expected consumption at observed hour, contemporaneous weather"}
    provenance = json.loads(Path(args.provenance).read_text(encoding="utf-8")) if args.provenance else None
    metrics.update(dataset=args.dataset, source_provenance=provenance)
    thresholds = calibrate_thresholds(val_p, args.quantile)
    config = {"medians": medians, "feature_inputs": inputs, "numTrees": args.trees,
              "maxDepth": args.depth, "seed": 42, "data_origin": args.origin,
              "unit": args.unit, "Spark_version": spark.version,
              "dataset": args.dataset, "source_provenance": provenance}
    model.write().save(str(out / "model" / "pipeline"))
    write_json(out / "model" / "config.json", config)
    write_json(out / "model" / "thresholds.json", thresholds)
    write_json(out / "metrics.json", metrics)
    test.select(*[c for c in raw.columns if c in test.columns]).write.mode("errorifexists").parquet(str(out / "heldout_test"))
    # Keep original missing features for replay. Saving imputed test_p features
    # would erase missing flags and change online predictions.
    replay_columns = ["building_id", "site_id", "meter", "timestamp", "meter_reading",
                      "primary_use", "square_feet", "year_built", "floor_count",
                      "air_temperature", "dew_temperature", "cloud_coverage",
                      "sea_level_pressure", "wind_speed"]
    keys = ["building_id", "meter", "timestamp"]
    replay = test.select(*replay_columns).join(test_p.select(*keys, "prediction"), keys, "inner")
    (replay.withColumn("timestamp", F.date_format("timestamp", "yyyy-MM-dd HH:mm:ss"))
           .write.mode("errorifexists").parquet(str(out / "replay_test")))
    per_building = test_p.groupBy("building_id").agg(
        F.count("*").alias("rows"), F.avg(F.abs(F.col("meter_reading") - F.col("prediction"))).alias("MAE"),
        F.avg((F.col("meter_reading") - F.col("prediction")) ** 2).alias("MSE"),
        F.var_pop("meter_reading").alias("target_variance"))
    (per_building.withColumn("RMSE", F.sqrt("MSE"))
        .withColumn("R2", F.when(F.col("target_variance") > 0, 1 - F.col("MSE") / F.col("target_variance")))
        .orderBy("building_id").toPandas().to_csv(out / "metrics_by_building.csv", index=False))
    alerts = add_alerts(test_p, thresholds)
    sample_cols = ["building_id", "site_id", "meter", "timestamp", "meter_reading",
                  "prediction", "residual", "abs_residual", "threshold", "is_anomaly", "severity"]
    alerts.select(*sample_cols).write.mode("errorifexists").parquet(str(out / "test_scored"))
    sample = alerts.select(*sample_cols).orderBy("timestamp", "building_id").limit(5000).toPandas()
    sample.to_csv(out / "predictions_preview.csv", index=False)
    attrs = test_p.schema["features"].metadata.get("ml_attr", {}).get("attrs", {})
    names = {a["idx"]: a["name"] for group in attrs.values() for a in group}
    importances = model.stages[-1].featureImportances.toArray()
    with (out / "feature_importance.csv").open("w", newline="", encoding="utf-8") as fp:
        writer = csv.writer(fp); writer.writerow(["feature", "importance"])
        writer.writerows(sorted([(names.get(i, f"feature_{i}"), float(v))
                                 for i, v in enumerate(importances)], key=lambda r: -r[1]))
    plot_results(out, sample, args.origin, args.unit)
    print(json.dumps(metrics, ensure_ascii=False, indent=2))
    print("Saved model and evaluation to", out)


def plot_results(out, sample, origin, unit):
    # Notebook parents may export an inline backend unavailable in this venv.
    # Select a file backend BEFORE importing Matplotlib, which validates it.
    os.environ["MPLBACKEND"] = "Agg"
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd
    suffix = " [SYNTHETIC CODE CHECK]" if origin == "synthetic" else ""
    chosen = sample[sample.building_id == sample.building_id.min()].head(240)
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(pd.to_datetime(chosen.timestamp), chosen.meter_reading, label="Actual", lw=1.2)
    ax.plot(pd.to_datetime(chosen.timestamp), chosen.prediction, label="Random Forest", lw=1.1)
    building = str(chosen.building_id.iloc[0]) if len(chosen) else "empty preview"
    ax.set(title="Held-out consumption: " + building + suffix,
           ylabel=unit, xlabel="Historical timestamp")
    ax.legend(); fig.autofmt_xdate(); fig.tight_layout(); fig.savefig(out / "actual_predicted.png", dpi=180); plt.close(fig)
    importance = pd.read_csv(out / "feature_importance.csv").head(12).iloc[::-1]
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.barh(importance.feature, importance.importance, color="#3576ac")
    ax.set(title="RF feature importance" + suffix, xlabel="Relative split importance")
    fig.tight_layout(); fig.savefig(out / "feature_importance.png", dpi=180); plt.close(fig)
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.hist(sample.residual, bins=40, color="#3576ac")
    ax.set(title="Held-out prediction residuals" + suffix, xlabel="Actual - predicted (" + unit + ")", ylabel="Preview rows")
    fig.tight_layout(); fig.savefig(out / "residuals.png", dpi=180); plt.close(fig)


def injection_test(spark, args):
    bundle = AnalyticsBundle(args.model)
    base = bundle.score(read_input(spark, args.input, args.format)).cache()
    # Labels describe controlled modifications, not verified real-world anomalies.
    scenarios = [base.withColumn("scenario", F.lit("unchanged_reference"))
                 .withColumn("is_injected", F.lit(0))]
    for name, actual in [("spike", F.col("meter_reading") * 3 + 1),
                         ("drop_to_zero", F.lit(0.0)),
                         ("positive_bias", F.col("meter_reading") * 1.2)]:
        changed = base.filter(actual != F.col("meter_reading"))
        changed = changed.withColumn("meter_reading", actual).withColumn("scenario", F.lit(name)).withColumn("is_injected", F.lit(1))
        scenarios.append(changed)
    combined = scenarios[0]
    for other in scenarios[1:]:
        combined = combined.unionByName(other)
    # Features are unchanged and contain no actual reading, so predictions are reused.
    scored = add_alerts(combined.drop("threshold", "residual", "abs_residual", "is_anomaly", "severity", "calibrated_threshold"), bundle.thresholds).cache()
    counts = {(r["is_injected"], bool(r["is_anomaly"])): r["count"]
              for r in scored.groupBy("is_injected", "is_anomaly").count().collect()}
    tp, fp = counts.get((1, True), 0), counts.get((0, True), 0)
    fn, tn = counts.get((1, False), 0), counts.get((0, False), 0)
    precision = tp/(tp+fp) if tp+fp else None
    recall = tp/(tp+fn) if tp+fn else None
    f1 = 2*tp/(2*tp+fp+fn) if 2*tp+fp+fn else None
    per_scenario = [r.asDict() for r in scored.groupBy("scenario").agg(F.count("*").alias("rows"),
                   F.sum(F.col("is_anomaly").cast("int")).alias("alerts")).collect()]
    result = {"evaluation_type": "CONTROLLED_PERTURBATION_ONLY", "source_origin": bundle.config["data_origin"],
        "label_definition": "1=modified measurement; 0=unchanged unverified reference",
        "TP": tp, "FP": fp, "FN": fn, "TN": tn, "precision": precision, "recall": recall,
        "F1": f1, "per_scenario": per_scenario,
        "limitation": "Unchanged real data are not certified normal; this is not real-anomaly accuracy."}
    output = Path(args.output); output.mkdir(parents=True, exist_ok=True)
    write_json(output / "injection_metrics.json", result)
    scored.select("building_id", "timestamp", "scenario", "is_injected", "meter_reading", "prediction",
                  "threshold", "is_anomaly", "severity").orderBy("scenario", "timestamp").limit(5000).toPandas().to_csv(output / "injection_preview.csv", index=False)
    os.environ["MPLBACKEND"] = "Agg"
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.imshow([[tn, fp], [fn, tp]], cmap="Blues")
    for (i, j), value in {(0, 0): tn, (0, 1): fp, (1, 0): fn, (1, 1): tp}.items():
        ax.text(j, i, str(value), ha="center", va="center")
    title = "Controlled perturbation confusion matrix"
    if bundle.config["data_origin"] == "synthetic":
        title += "\n[SYNTHETIC CODE CHECK]"
    ax.set(xticks=[0, 1], yticks=[0, 1], xticklabels=["No alert", "Alert"],
           yticklabels=["Unchanged reference", "Injected"], title=title)
    fig.tight_layout(); fig.savefig(output / "confusion_matrix.png", dpi=180); plt.close(fig)
    print(json.dumps(result, indent=2))


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--master", default="local[2]")
    sub = p.add_subparsers(dest="command", required=True)
    demo = sub.add_parser("demo-data"); demo.add_argument("--output", default="data/demo.csv")
    demo.add_argument("--days", type=int, default=45); demo.add_argument("--buildings", type=int, default=4)
    prep = sub.add_parser("prepare"); prep.add_argument("--raw-dir", required=True); prep.add_argument("--output", required=True)
    prep.add_argument("--site-id", type=int, default=1); prep.add_argument("--building-limit", type=int, default=10)
    prep.add_argument("--max-days", type=int, default=180)
    train = sub.add_parser("train"); train.add_argument("--input", required=True); train.add_argument("--output", required=True)
    train.add_argument("--format", choices=["auto", "csv", "parquet"], default="auto")
    train.add_argument("--origin", choices=["synthetic", "real"], required=True)
    train.add_argument("--unit", default="original meter_reading unit")
    train.add_argument("--dataset", default="ASHRAE_GROUP_SILVER")
    train.add_argument("--provenance", help="provenance.json written by prepare_group.py")
    train.add_argument("--trees", type=int, default=60); train.add_argument("--depth", type=int, default=8)
    train.add_argument("--quantile", type=float, default=.99)
    pred = sub.add_parser("predict"); pred.add_argument("--input", required=True); pred.add_argument("--model", required=True)
    pred.add_argument("--output", required=True); pred.add_argument("--format", choices=["auto", "csv", "parquet"], default="auto")
    inject = sub.add_parser("injection-test"); inject.add_argument("--input", required=True); inject.add_argument("--model", required=True)
    inject.add_argument("--output", required=True); inject.add_argument("--format", choices=["auto", "csv", "parquet"], default="auto")
    return p


def main():
    args = parser().parse_args()
    if args.command == "demo-data":
        if args.days < 2 or args.buildings < 1:
            raise ValueError("Use days >= 2 and buildings >= 1.")
        demo_data(args.output, args.days, args.buildings); return
    if args.command == "train" and not 0 < args.quantile < 1:
        raise ValueError("Quantile must be between 0 and 1.")
    spark = spark_session(args.master); spark.sparkContext.setLogLevel("ERROR")
    try:
        if args.command == "prepare": prepare_ashrae(spark, args)
        elif args.command == "train": train_model(spark, args)
        elif args.command == "injection-test": injection_test(spark, args)
        else:
            scored = AnalyticsBundle(args.model).score(read_input(spark, args.input, args.format))
            scored.write.mode("errorifexists").parquet(args.output)
            scored.show(10, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
