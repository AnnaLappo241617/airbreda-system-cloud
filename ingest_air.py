"""
ingest_air.py - AirBreda (Day 1 + Day 2)

Every POLL_INTERVAL_SECONDS: fetch recent NO2 readings for Luchtmeetnet station NL10240,
check data quality, write the latest reading to sensor_readings, publish it to the
Redis queue, and log every step as structured JSON. Serves GET /health.

CAP TRADE-OFF (Day 1 required comment)
The Luchtmeetnet sensor network behaves like an AP system: when a station loses
connectivity the API still answers (Availability) but the data may be null, missing or
stale (Consistency with the real air is sacrificed). The API even returns HTTP 200 with
an empty list when it has no data, so "no data" does not look like an error.

DATA QUALITY POLICY (Day 2 Lab 2)
- null value                                   -> DATA_QUALITY_ERROR, reason "null"
- same value for 3+ consecutive hourly readings -> DATA_QUALITY_ERROR, reason "stale"
  (a frozen sensor or an interpolated/carried-over value)
- no record at all                             -> DATA_QUALITY_ERROR, reason "missing"
Flagged readings are STILL WRITTEN to sensor_readings with is_flagged = TRUE, never
dropped and never filled in: for an air-quality time series a visible, flagged value is
better than a silent gap, and downstream users (dashboard, ML) can decide to exclude it.
Known limitation: only the newest reading is flagged; the two earlier readings of a
stale run were written unflagged when they arrived.
"""
import logging
import os
import sys
import time

import pandas as pd
import requests

import publisher
import scheduler
from db import db_configured, get_connection, save_rows
from getNO2Readings import STATION
from quality import BadDataTracker, HealthState, log_event, setup_logging, start_health_server

SOURCE = "Luchtmeetnet"
COLUMNS = ["station_id", "timestamp", "component", "value"]
MAX_ATTEMPTS = 3
STALE_RUN = 3  # this many identical consecutive hourly values = stale

luchtmeetnet_tracker = BadDataTracker(SOURCE)   # .bad_data_count = luchtmeetnet_bad_data_count
health = HealthState(SOURCE, luchtmeetnet_tracker)


def fetch_recent_no2(station=STATION, formula="NO2", timeout=10):
    """Same API call as get_latest_no2() in getNO2Readings.py, but returns the whole first
    page (newest first) instead of one record - we need history to detect stale values."""
    url = f"https://api.luchtmeetnet.nl/open_api/stations/{station}/measurements"
    params = {"formula": formula, "order_by": "timestamp_measured",
              "order_direction": "desc", "page": 1}
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json().get("data", [])


def with_retry(fn, attempts=MAX_ATTEMPTS, sleep=time.sleep):
    """Retry timeouts, connection errors and HTTP 5xx with backoff (2 s, 4 s).
    HTTP 4xx is not retried. 3 attempts stay far below the fair-use limit."""
    error = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status is not None and status < 500:
                raise
            error = exc
        except (requests.Timeout, requests.ConnectionError) as exc:
            error = exc
        log_event(logging.WARNING, "fetch_retry", source=SOURCE,
                  attempt=attempt, attempts=attempts, error=str(error))
        if attempt < attempts:
            sleep(2 ** attempt)
    raise RuntimeError(f"Luchtmeetnet API unreachable after {attempts} attempts") from error


def to_dataframe(records, station=STATION):
    """API records -> our schema, newest first."""
    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(columns=COLUMNS)
    df = df.rename(columns={"formula": "component", "timestamp_measured": "timestamp"})
    df["station_id"] = station
    df = df.reindex(columns=COLUMNS)
    return df.sort_values("timestamp", ascending=False, key=lambda s: pd.to_datetime(s, utc=True))


def filter_no2_readings(df):
    """Keep NO2 rows only. Null values are kept on purpose."""
    return df[df["component"] == "NO2"].copy()


def assess_quality(recent):
    """recent: NO2 rows, newest first. Returns (is_flagged, reason) for the NEWEST row."""
    latest = recent.iloc[0]
    if pd.isna(latest["value"]):
        return True, "null"
    window = recent.head(STALE_RUN)
    if len(window) == STALE_RUN and window["value"].notna().all() and window["value"].nunique() == 1:
        times = pd.to_datetime(window["timestamp"], utc=True)
        gaps = times.diff().dropna().abs()
        if (gaps == pd.Timedelta(hours=1)).all():  # truly consecutive hours
            return True, "stale"
    return False, None


def save_readings(df, conn):
    """Write rows (with is_flagged, default False). Returns how many rows were new."""
    flags = df["is_flagged"] if "is_flagged" in df else [False] * len(df)
    rows = [(r.station_id, str(r.timestamp), r.component,
             None if pd.isna(r.value) else float(r.value), bool(flag))
            for r, flag in zip(df.itertuples(index=False), flags)]
    return save_rows(conn, rows)


def build_messages(df):
    """Rows -> queue messages in the lab's JSON format."""
    return [{"station_id": r.station_id, "timestamp": publisher.to_utc_z(r.timestamp),
             "component": r.component,
             "value": None if pd.isna(r.value) else float(r.value)}
            for r in df.itertuples(index=False)]


def process(recent, conn=None):
    """Quality-check the newest reading, log it, write it. Returns the 1-row DataFrame."""
    latest = recent.head(1).copy()
    flagged, reason = assess_quality(recent)
    latest["is_flagged"] = flagged
    row = latest.iloc[0]
    value = None if pd.isna(row["value"]) else float(row["value"])

    log_event(logging.INFO, "fetch_success", source=SOURCE, station_id=row["station_id"],
              value=value, timestamp=publisher.to_utc_z(row["timestamp"]))
    if flagged:
        log_event(logging.WARNING, "DATA_QUALITY_ERROR", source=SOURCE,
                  station_id=row["station_id"], field="NO2", reason=reason, value=value,
                  timestamp=publisher.to_utc_z(row["timestamp"]))
        luchtmeetnet_tracker.record()

    if conn is not None:
        inserted = save_readings(latest, conn)
        log_event(logging.INFO, "db_write_success", source=SOURCE, rows_inserted=inserted,
                  rows_already_present=len(latest) - inserted, is_flagged=flagged)
    return latest


def run_once():
    try:
        records = with_retry(fetch_recent_no2)
    except (RuntimeError, requests.RequestException) as exc:
        log_event(logging.ERROR, "fetch_failed", source=SOURCE, station_id=STATION, error=str(exc))
        return 1

    recent = filter_no2_readings(to_dataframe(records))
    if recent.empty:  # HTTP 200 but no data: the AP behaviour described above
        log_event(logging.WARNING, "DATA_QUALITY_ERROR", source=SOURCE, station_id=STATION,
                  field="NO2", reason="missing", value=None)
        luchtmeetnet_tracker.record()
        return 1
    health.mark_success()

    status = 0
    conn = None
    if db_configured():
        try:
            conn = get_connection()
        except Exception as exc:
            log_event(logging.ERROR, "db_connect_failed", source=SOURCE, error=str(exc))
            status = 1
    else:
        log_event(logging.WARNING, "db_not_configured", source=SOURCE)

    try:
        latest = process(recent, conn)
    except Exception as exc:
        log_event(logging.ERROR, "db_write_failed", source=SOURCE, error=str(exc))
        latest, status = recent.head(1), 1
    finally:
        if conn is not None:
            conn.close()

    publisher.publish(build_messages(latest), source=SOURCE)
    return status


def main():
    setup_logging()
    if scheduler.loop_mode():  # /health only makes sense for a long-running service
        start_health_server(health, int(os.environ.get("HEALTH_PORT", "8000")))
    return scheduler.run(run_once, SOURCE)


if __name__ == "__main__":
    sys.exit(main())
