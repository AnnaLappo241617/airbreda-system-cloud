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

from db import get_connection

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
    df = build(load_no2(), load_traffic())
    df.to_csv("training_data.csv", index=False)
    print(f"\nJoined rows written to training_data.csv: {len(df)}")
    if len(df) < 24:
        print("WARNING: fewer than 24 rows - treat any model as a demonstration, not a "
              "reliable predictor (note it in ADR-006).")
    return 0 if len(df) else 1


if __name__ == "__main__":
    sys.exit(main())
