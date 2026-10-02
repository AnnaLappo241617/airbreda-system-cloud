"""
build_training_data.py - Day 4: join NO2 (database) with traffic (S3 CSVs) into training_data.csv

Run on your LAPTOP (venv active). Uses:
  - .env for the database (DB_HOST, DB_USER, DB_PASSWORD, ...)
  - your `aws configure` credentials for S3 (needs s3:ListBucket + s3:GetObject; the VM
    role is deliberately NOT given ListBucket)

HOW THE JOIN WORKS (important - read before changing it)
- A Luchtmeetnet NO2 row is an hourly AVERAGE, stamped with the END of its hour:
  timestamp 11:00 = average over 10:00-11:00 (seen on Day 1: timestamp_measured_start/end).
- An NDW CSV ndw/YYYY-MM-DD/HH-site.csv holds ONE per-minute snapshot taken during hour HH
  (e.g. 10:43). That snapshot lies inside the window 10:00-11:00.
- So traffic file hour HH is joined to the NO2 row stamped HH+1. "Round to the nearest
  hour" (the lab's suggestion) would join a 10:20 snapshot to the 10:00 NO2 value, i.e. to
  the hour BEFORE the traffic happened.
- hour_of_day = HH in UTC (the traffic hour). predict() must be called with the same
  definition, or the model sees a different feature at serving time than in training.

NO2 BACKFILL: ingest_air.py stores only the newest reading per run, so hours can be missing
in the database even though traffic files exist for them (e.g. when a run was late or the VM
was off). Luchtmeetnet keeps history, so missing NO2 hours are fetched from the API, written to
sensor_readings (idempotent) and used for the join. NDW keeps NO history - traffic can't be
backfilled, which is why only time adds rows.

Other rules: rows with is_flagged = TRUE (stale/null NO2) are excluded from training, and an
hour is only kept if all four sites have a file (otherwise the total would be too low).
"""
import io
import os
import re
import sys

import boto3
import pandas as pd
from dotenv import load_dotenv

import requests

from db import get_connection, save_rows

load_dotenv()
BUCKET = os.environ.get("S3_BUCKET", "airbreda-testnight-2026")
SITES = ["hrl", "hrr", "vwd", "vwa"]
KEY_RE = re.compile(r"^ndw/(\d{4}-\d{2}-\d{2})/(\d{2})-(hrl|hrr|vwd|vwa)\.csv$")  # skips ndw/raw/...


def load_no2():
    sql = """SELECT timestamp, value AS no2_ug_m3, is_flagged
             FROM sensor_readings
             WHERE station_id = 'NL10240' AND component = 'NO2'
             ORDER BY timestamp"""
    with get_connection() as conn:
        df = pd.read_sql(sql, conn)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    n_all = len(df)
    df = df[~df["is_flagged"].fillna(False) & df["no2_ug_m3"].notna()]
    print(f"NO2 rows: {n_all} in database, {len(df)} usable (not flagged, not null)")
    return df[["timestamp", "no2_ug_m3"]]


def load_traffic(s3=None):
    s3 = s3 or boto3.client("s3")
    rows = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="ndw/"):
        for obj in page.get("Contents", []):
            m = KEY_RE.match(obj["Key"])
            if not m:
                continue
            day, hour, site = m.groups()
            body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
            csv = pd.read_csv(io.BytesIO(body))
            rows.append({"traffic_hour": pd.Timestamp(f"{day}T{hour}:00:00Z"), "site": site,
                         "intensity": float(csv.loc[0, "total_flow_veh_per_hour"])})
    df = pd.DataFrame(rows, columns=["traffic_hour", "site", "intensity"])
    print(f"NDW CSV files: {len(df)}")
    return df


def backfill_no2(no2, traffic, max_pages=10):
    """Fetch NO2 hours that traffic needs but the database lacks. Returns the extended frame."""
    needed = set(traffic["traffic_hour"].unique() + pd.Timedelta(hours=1)) - set(no2["timestamp"])
    if not needed:
        return no2
    oldest = min(needed)
    url = "https://api.luchtmeetnet.nl/open_api/stations/NL10240/measurements"
    found = []
    for page in range(1, max_pages + 1):
        r = requests.get(url, params={"formula": "NO2", "order_by": "timestamp_measured",
                                      "order_direction": "desc", "page": page}, timeout=15)
        r.raise_for_status()
        data = r.json().get("data", [])
        if not data:
            break
        for rec in data:
            ts = pd.Timestamp(rec["timestamp_measured"]).tz_convert("UTC")
            if ts in needed and rec.get("value") is not None:
                found.append({"timestamp": ts, "no2_ug_m3": float(rec["value"])})
        if pd.Timestamp(data[-1]["timestamp_measured"]).tz_convert("UTC") < oldest:
            break
    if found:
        rows = [("NL10240", r["timestamp"].isoformat(), "NO2", r["no2_ug_m3"], False) for r in found]
        with get_connection() as conn:
            inserted = save_rows(conn, rows)
        print(f"NO2 backfill: {len(found)} missing hours found in the API, {inserted} written to the database")
    else:
        print(f"NO2 backfill: {len(needed)} hours missing, none available from the API")
    return pd.concat([no2, pd.DataFrame(found, columns=["timestamp", "no2_ug_m3"])], ignore_index=True)


def build(no2, traffic):
    wide = traffic.pivot_table(index="traffic_hour", columns="site", values="intensity", aggfunc="last")
    wide = wide.reindex(columns=SITES)
    complete = wide.dropna()
    print(f"Traffic hours: {len(wide)} with any site, {len(complete)} with all four sites")
    wide = complete.add_prefix("intensity_").reset_index()
    wide["total_intensity_veh_per_hr"] = wide[[f"intensity_{s}" for s in SITES]].sum(axis=1)
    wide["hour_of_day"] = wide["traffic_hour"].dt.hour
    wide["no2_timestamp"] = wide["traffic_hour"] + pd.Timedelta(hours=1)   # see docstring
    out = wide.merge(no2, left_on="no2_timestamp", right_on="timestamp", how="inner").drop(columns="timestamp")
    return out.sort_values("traffic_hour").reset_index(drop=True)


def main():
    no2, traffic = load_no2(), load_traffic()
    df = build(backfill_no2(no2, traffic), traffic)
    df.to_csv("training_data.csv", index=False)
    print(f"\nJoined rows written to training_data.csv: {len(df)}")
    if len(df) < 24:
        print("WARNING: fewer than 24 rows - treat any model as a demonstration, not a "
              "reliable predictor (note it in ADR-006).")
    return 0 if len(df) else 1


if __name__ == "__main__":
    sys.exit(main())
