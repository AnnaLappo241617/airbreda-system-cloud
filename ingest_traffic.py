"""
ingest_traffic.py - AirBreda (Day 1 + Day 2)

Every POLL_INTERVAL_SECONDS: download NDW's site-config and measured-data feeds, extract
the four A27 sites near NL10240 (hrl, hrr, vwd, vwa), and for each site:
  - save a CSV  ndw/YYYY-MM-DD/HH-<site>.csv  and upload it (plus the raw feeds) to S3
  - write FLOW and SPEED rows to sensor_readings (unless the site sent bad data)
  - publish one message per site to the Redis queue
Every step is logged as structured JSON. Serves GET /health.
Parsing reuses build_index_map / extract_measurements / download_and_decompress from the
course's getTrafficReadings.py.

WHY BOTH A DATABASE AND A BUCKET? (Day 1 required comment)
- The database holds PARSED readings in a fixed schema: fast indexed time-range queries
  and joins between air quality and traffic. It only contains what the current parser
  extracted, in the shape we chose at the time.
- The bucket holds FILES - cheap, durable, never modified: the audit trail of what we
  actually received. It cannot answer queries or join datasets efficiently.
- When we retrain the ML model in six months, the parser will have changed (bug fixes,
  new features such as per-lane counts). The database only has the old parser's output.
  With the RAW feeds in the bucket (UPLOAD_RAW=1) we can re-run the new parser over the
  whole history and rebuild a correct training set.

DATA QUALITY POLICY (Day 2 Lab 2)
NDW uses speed = -1 as a sentinel ("no valid value"). If any lane of a site reports -1:
  -> WARNING DATA_QUALITY_ERROR, ndw bad-data count +1,
  -> NO database rows and NO queue message for that site this run (don't store sentinels),
  -> the CSV and raw feed still go to S3, because the bucket is the unmodified record.
The check runs on the RAW per-lane values: report_site() in getTrafficReadings.py drops
speeds <= 0 before averaging, which would hide a -1 completely.
"""
import csv
import gzip
import io
import logging
import os
import sys
from pathlib import Path

import boto3
import pandas as pd

import publisher
import scheduler
from db import db_configured, get_connection, save_rows
from getTrafficReadings import (CONFIG_URL, MEASURED_URL, TARGET_SITE_IDS, build_index_map,
                                download_and_decompress, extract_measurements)
from quality import BadDataTracker, HealthState, log_event, setup_logging, start_health_server

SOURCE = "NDW"
SITE_CODES = ("hrl", "hrr", "vwd", "vwa")
CSV_FIELDS = ["site_id", "site_code", "timestamp", "total_flow_veh_per_hour", "avg_speed_kmh"]
SENTINEL_SPEED = -1.0

ndw_tracker = BadDataTracker(SOURCE)   # .bad_data_count = ndw_bad_data_count
health = HealthState(SOURCE, ndw_tracker)


def site_code(site_id):
    for code in SITE_CODES:
        if code in site_id:
            return code
    raise ValueError(f"Unknown site code in {site_id}")


def build_key(timestamp, code):
    """Measurement timestamp + site code -> 'ndw/YYYY-MM-DD/HH-code.csv' (UTC)."""
    ts = pd.to_datetime(timestamp, utc=True)
    return f"ndw/{ts:%Y-%m-%d}/{ts:%H}-{code}.csv"


def summarise_site(site_id, index_map, readings):
    """Same totals as report_site(), plus the raw speed values for the sentinel check."""
    flow_idx = {i for i, info in index_map.items()
                if info["type"] == "trafficFlow" and info["vehicle"] == "anyVehicle"}
    speed_idx = {i for i, info in index_map.items()
                 if info["type"] == "trafficSpeed" and info["vehicle"] == "anyVehicle"}
    flow_vals = [float(r["value"]) for r in readings if r["index"] in flow_idx]
    raw_speeds = [float(r["value"]) for r in readings if r["index"] in speed_idx]
    valid_speeds = [v for v in raw_speeds if v > 0]
    return {
        "site_id": site_id,
        "timestamp": readings[0].get("timestamp") if readings else None,
        "total_flow": sum(flow_vals),
        "avg_speed": sum(valid_speeds) / len(valid_speeds) if valid_speeds else None,
        "raw_speeds": raw_speeds,
    }


def process_site(summary, tracker=ndw_tracker):
    """Quality-check one site. Returns the DB rows to write ([] if the site is rejected)."""
    if SENTINEL_SPEED in summary["raw_speeds"]:
        log_event(logging.WARNING, "DATA_QUALITY_ERROR", source=SOURCE,
                  location=summary["site_id"], field="speed", value=-1,
                  timestamp=publisher.to_utc_z(summary["timestamp"]))
        tracker.record()
        return []
    ts = publisher.to_utc_z(summary["timestamp"])
    return [(summary["site_id"], ts, "FLOW", summary["total_flow"], False),
            (summary["site_id"], ts, "SPEED", summary["avg_speed"], False)]


def build_message(summary, code):
    return {"station_id": summary["site_id"], "site_code": code,
            "timestamp": publisher.to_utc_z(summary["timestamp"]), "component": "TRAFFIC",
            "total_flow_veh_per_hour": summary["total_flow"],
            "avg_speed_kmh": summary["avg_speed"]}


def write_csv(path, summary, code):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerow({"site_id": summary["site_id"], "site_code": code,
                         "timestamp": summary["timestamp"],
                         "total_flow_veh_per_hour": summary["total_flow"],
                         "avg_speed_kmh": summary["avg_speed"]})


def run_once():
    bucket = os.environ.get("S3_BUCKET")
    s3 = boto3.client("s3") if bucket else None
    try:
        config_bytes = download_and_decompress(CONFIG_URL).read()
        measured_bytes = download_and_decompress(MEASURED_URL).read()
    except Exception as exc:
        log_event(logging.ERROR, "fetch_failed", source=SOURCE, error=str(exc))
        return 1

    status, db_rows, messages, first_ts = 0, [], [], None
    for site_id in TARGET_SITE_IDS:
        code = site_code(site_id)
        index_map = build_index_map(io.BytesIO(config_bytes), site_id)
        readings = extract_measurements(io.BytesIO(measured_bytes), site_id)
        summary = summarise_site(site_id, index_map, readings)
        if not summary["timestamp"]:
            log_event(logging.WARNING, "DATA_QUALITY_ERROR", source=SOURCE, location=site_id,
                      field="all", reason="missing", value=None)
            ndw_tracker.record()
            status = 1
            continue
        first_ts = first_ts or summary["timestamp"]
        log_event(logging.INFO, "fetch_success", source=SOURCE, location=site_id,
                  site_code=code, total_flow=summary["total_flow"],
                  avg_speed=summary["avg_speed"],
                  timestamp=publisher.to_utc_z(summary["timestamp"]))

        # S3 always gets the file: the bucket is the unmodified record
        key = build_key(summary["timestamp"], code)
        write_csv(Path(key), summary, code)
        if s3:
            try:
                s3.upload_file(key, bucket, key)
                log_event(logging.INFO, "s3_upload_success", source=SOURCE, key=key)
            except Exception as exc:
                log_event(logging.ERROR, "s3_upload_failed", source=SOURCE, key=key, error=str(exc))
                status = 1

        rows = process_site(summary)
        if rows:
            db_rows.extend(rows)
            messages.append(build_message(summary, code))

    health.mark_success()

    if s3 and first_ts and os.environ.get("UPLOAD_RAW") == "1":
        ts = pd.to_datetime(first_ts, utc=True)
        for name, data in (("config", config_bytes), ("measured", measured_bytes)):
            raw_key = f"ndw/raw/{ts:%Y-%m-%d}/{ts:%H}-{name}.xml.gz"
            try:
                s3.put_object(Bucket=bucket, Key=raw_key, Body=gzip.compress(data))
                log_event(logging.INFO, "s3_upload_success", source=SOURCE, key=raw_key)
            except Exception as exc:
                log_event(logging.ERROR, "s3_upload_failed", source=SOURCE, key=raw_key, error=str(exc))
                status = 1

    if db_rows and db_configured():
        try:
            conn = get_connection()
            try:
                inserted = save_rows(conn, db_rows)
            finally:
                conn.close()
            log_event(logging.INFO, "db_write_success", source=SOURCE,
                      rows_inserted=inserted, rows_already_present=len(db_rows) - inserted)
        except Exception as exc:
            log_event(logging.ERROR, "db_write_failed", source=SOURCE, error=str(exc))
            status = 1

    publisher.publish(messages, source=SOURCE)
    return status


def main():
    setup_logging()
    if os.environ.get("RUN_ONCE") != "1":
        start_health_server(health, int(os.environ.get("HEALTH_PORT", "8000")))
    return scheduler.run(run_once, SOURCE)


if __name__ == "__main__":
    sys.exit(main())
