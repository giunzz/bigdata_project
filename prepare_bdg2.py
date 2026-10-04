"""Prepare a declared BDG2 RAW electricity subset for Phuc's Spark model.

Uses the official repository's CSVs, including Git LFS file contents. Keeps
native building/site IDs, local wall-clock timestamps, zeros and positive spikes.
No conversion is applied: BDG2 RAW energy is already measured in kWh.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

REPOSITORY = "https://github.com/buds-lab/building-data-genome-project-2"
RELATIVE_FILES = {
    "metadata": "data/metadata/metadata.csv",
    "weather": "data/weather/weather.csv",
    "electricity": "data/meters/raw/electricity.csv",
}
FIELDS = ["building_id", "site_id", "meter", "timestamp", "meter_reading",
          "primary_use", "square_feet", "year_built", "floor_count",
          "air_temperature", "dew_temperature", "cloud_coverage",
          "sea_level_pressure", "wind_speed", "timezone"]
WEATHER = {"air_temperature": "airTemperature",
           "dew_temperature": "dewTemperature", "cloud_coverage": "cloudCoverage",
           "sea_level_pressure": "seaLvlPressure", "wind_speed": "windSpeed"}


def is_lfs(path):
    with Path(path).open("rb") as stream:
        return stream.read(128).startswith(b"version https://git-lfs.github.com/spec/")


def download_csv(relative, destination):
    destination = Path(destination)
    if destination.is_file() and destination.stat().st_size and not is_lfs(destination):
        print("Dùng file đã tải:", destination, flush=True)
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".part")
    errors = []
    for base in ["https://media.githubusercontent.com/media/",
                 "https://raw.githubusercontent.com/"]:
        url = base + "buds-lab/building-data-genome-project-2/master/" + relative
        try:
            print("Đang tải:", relative, flush=True)
            request = urllib.request.Request(url, headers={"User-Agent": "Phuc-BDG2-Analytics"})
            with urllib.request.urlopen(request, timeout=90) as response, temporary.open("wb") as stream:
                shutil.copyfileobj(response, stream, length=1024 * 1024)
            if not temporary.stat().st_size or is_lfs(temporary):
                raise ValueError("URL chỉ trả con trỏ Git LFS hoặc file rỗng.")
            with temporary.open(encoding="utf-8-sig", newline="") as stream:
                header = next(csv.reader(stream))
            expected = "building_id" if "/metadata/" in relative else "timestamp"
            if expected not in header:
                raise ValueError("File tải về thiếu cột " + expected)
            temporary.replace(destination)
            return
        except Exception as exc:
            errors.append(str(exc))
            temporary.unlink(missing_ok=True)
    raise RuntimeError("Không tải được " + relative + ": " + "; ".join(errors))


def locate(root, relative):
    candidates = [root / relative, root / relative.removeprefix("data/"),
                  root / Path(relative).name]
    for candidate in candidates:
        if candidate.is_file():
            if is_lfs(candidate):
                raise ValueError(f"{candidate} là con trỏ Git LFS. Chạy lại với --download để tải CSV thật.")
            return candidate
    raise FileNotFoundError("Thiếu " + relative + ". Dùng --download hoặc điền đúng --raw-dir.")


def rows(path):
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        for row in reader:
            yield row


def number(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def timestamp(value):
    result = datetime.fromisoformat(value.strip())
    if result.tzinfo is not None:
        raise ValueError("BDG2 dùng giờ địa phương không kèm offset; kiểm tra dữ liệu đầu vào.")
    return result


def prepare(args):
    root, output = Path(args.raw_dir), Path(args.output)
    if output.exists() or output.with_suffix(".provenance.json").exists():
        raise ValueError("Đầu ra đã tồn tại; chọn --output mới để giữ kết quả cũ.")
    if args.building_limit < 1 or args.max_days < 2:
        raise ValueError("building-limit phải >=1 và max-days phải >=2.")
    if args.download:
        for relative in RELATIVE_FILES.values():
            download_csv(relative, root / relative)
    files = {name: locate(root, relative) for name, relative in RELATIVE_FILES.items()}
    metadata = {}
    for row in rows(files["metadata"]):
        key = row.get("building_id", "").strip()
        if not key or not row.get("site_id", "").strip():
            continue
        if key in metadata and row != metadata[key]:
            raise ValueError("Metadata mâu thuẫn cho " + key)
        metadata[key] = row
    with files["electricity"].open(encoding="utf-8-sig", newline="") as stream:
        header = next(csv.reader(stream))
    if "timestamp" not in header:
        raise ValueError("Electricity CSV phải có timestamp và mỗi tòa nhà là một cột.")
    common = set(header) & set(metadata)
    sites = sorted({metadata[key]["site_id"].strip() for key in common})
    if not sites:
        raise ValueError("Không ghép được building_id trong metadata với header electricity.")
    requested = args.site_id.strip()
    preferred = requested or "Robin"
    site = next((value for value in sites if value.casefold() == preferred.casefold()), None)
    if site is None and requested:
        raise ValueError("Site không tồn tại. Các site có điện: " + ", ".join(sites))
    site = site or sites[0]
    buildings = sorted(key for key in common if metadata[key]["site_id"].strip() == site)[:args.building_limit]
    start = timestamp(args.start)
    end = start + timedelta(days=args.max_days)
    weather = {}
    for row in rows(files["weather"]):
        if row.get("site_id", "").strip() != site:
            continue
        ts = timestamp(row["timestamp"])
        if not start <= ts < end:
            continue
        values = {name: number(row.get(native, row.get(name))) for name, native in WEATHER.items()}
        if ts in weather and weather[ts] != values:
            raise ValueError("Weather mâu thuẫn ở " + str(ts))
        weather[ts] = values
    indices = [(key, header.index(key)) for key in buildings]
    time_index = header.index("timestamp")
    counts = {"selected_raw_readings": 0, "invalid_readings_removed": 0,
              "duplicate_rows_removed": 0, "rows_without_weather_record": 0, "rows_gold": 0}
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".partial")
    seen, observed = {}, []
    try:
        with files["electricity"].open(encoding="utf-8-sig", newline="") as source, temporary.open("w", encoding="utf-8", newline="") as sink:
            reader = csv.reader(source); next(reader)
            writer = csv.DictWriter(sink, fieldnames=FIELDS); writer.writeheader()
            for record in reader:
                if not record:
                    continue
                ts = timestamp(record[time_index])
                if not start <= ts < end:
                    continue
                for key, index in indices:
                    counts["selected_raw_readings"] += 1
                    value = number(record[index])
                    if value is None or value < 0:
                        counts["invalid_readings_removed"] += 1
                        continue
                    duplicate_key = (key, ts)
                    if duplicate_key in seen:
                        if seen[duplicate_key] != value:
                            raise ValueError("Phép đo mâu thuẫn cho " + key + " ở " + str(ts))
                        counts["duplicate_rows_removed"] += 1
                        continue
                    seen[duplicate_key] = value
                    meta = metadata[key]
                    area = number(meta.get("sqft", meta.get("square_feet")))
                    if area is None:
                        sqm = number(meta.get("sqm"))
                        area = sqm * 10.7639104167 if sqm is not None else None
                    row = dict(building_id=key, site_id=site, meter=0, timestamp=ts.isoformat(sep=" "),
                               meter_reading=value, primary_use=meta.get("primaryspaceusage") or "UNKNOWN",
                               square_feet=area, year_built=number(meta.get("yearbuilt")),
                               floor_count=number(meta.get("numberoffloors")), timezone=meta.get("timezone", ""))
                    row.update(weather.get(ts, {name: None for name in WEATHER}))
                    if ts not in weather:
                        counts["rows_without_weather_record"] += 1
                    writer.writerow(row)
                    counts["rows_gold"] += 1
                    observed.append(ts)
        if counts["rows_gold"] == 0:
            raise ValueError("Không có phép đo hợp lệ trong site/khoảng ngày đã chọn.")
        temporary.replace(output)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    provenance = {"data_origin": "real", "dataset": "BDG2 RAW electricity", "source": REPOSITORY,
                  "source_files": {name: str(path) for name, path in files.items()},
                  "unit": "kWh", "conversion": "none; BDG2 RAW energy is already kWh",
                  "site_id": site, "building_ids": buildings, "building_limit": args.building_limit,
                  "requested_start": start.isoformat(sep=" "), "max_days": args.max_days,
                  "observed_start": min(observed).isoformat(sep=" "),
                  "observed_end": max(observed).isoformat(sep=" "),
                  "timestamp_basis": "original site local wall-clock time; no timezone conversion",
                  "zero_and_positive_spikes": "retained; upstream cleaned/outlier-filtered files are not used",
                  **counts}
    output.with_suffix(".provenance.json").write_text(json.dumps(provenance, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(provenance, ensure_ascii=False, indent=2), flush=True)
    print("Đã tạo Gold:", output, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", default="data/bdg2")
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--output", required=True)
    parser.add_argument("--site-id", default="")
    parser.add_argument("--building-limit", type=int, default=10)
    parser.add_argument("--max-days", type=int, default=180)
    parser.add_argument("--start", default="2016-01-01")
    prepare(parser.parse_args())


if __name__ == "__main__":
    main()
