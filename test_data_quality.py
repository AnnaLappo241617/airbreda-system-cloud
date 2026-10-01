"""Day 2 Lab 2: data-quality handlers."""
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

import ingest_air
import ingest_traffic
from quality import BadDataTracker
from tests.fakes import FakeConn


def air_rows(values, start="2026-10-01T10:00:00Z"):
    """Newest-first NO2 rows with hourly timestamps."""
    t0 = pd.Timestamp(start)
    return pd.DataFrame({
        "station_id": ["NL10240"] * len(values),
        "timestamp": [(t0 - pd.Timedelta(hours=i)).isoformat() for i in range(len(values))],
        "component": ["NO2"] * len(values),
        "value": values,
    })


# --- Luchtmeetnet: stale / null readings are written AND flagged ----------------------

def test_null_luchtmeetnet_reading_is_written_flagged_not_dropped(caplog):
    tracker = BadDataTracker("Luchtmeetnet")
    ingest_air.luchtmeetnet_tracker = tracker
    conn = FakeConn()
    with caplog.at_level(logging.WARNING):
        ingest_air.process(air_rows([None, 20.0, 19.0]), conn)

    assert len(conn.table) == 1                      # row written, not dropped
    row = next(iter(conn.table.values()))
    assert row["value"] is None and row["is_flagged"] is True
    assert tracker.bad_data_count == 1
    assert "DATA_QUALITY_ERROR" in caplog.text


def test_stale_luchtmeetnet_reading_is_written_flagged():
    ingest_air.luchtmeetnet_tracker = BadDataTracker("Luchtmeetnet")
    conn = FakeConn()
    ingest_air.process(air_rows([18.4, 18.4, 18.4, 17.0]), conn)  # 3 identical hours
    row = next(iter(conn.table.values()))
    assert row["value"] == 18.4 and row["is_flagged"] is True


def test_normal_luchtmeetnet_reading_is_not_flagged():
    ingest_air.luchtmeetnet_tracker = BadDataTracker("Luchtmeetnet")
    conn = FakeConn()
    ingest_air.process(air_rows([18.4, 18.4, 17.0]), conn)        # only 2 identical
    assert next(iter(conn.table.values()))["is_flagged"] is False


def test_identical_values_with_a_gap_are_not_stale():
    df = air_rows([18.4, 18.4, 18.4])
    df.loc[2, "timestamp"] = "2026-10-01T05:00:00+00:00"           # not consecutive hours
    assert ingest_air.assess_quality(df) == (False, None)


# --- NDW: speed = -1 is NOT written and IS counted ------------------------------------

def ndw_summary(raw_speeds):
    return {"site_id": "RWS01_MONIBAS_0271hrl0063ra", "timestamp": "2026-10-01T08:52:00Z",
            "total_flow": 1200.0, "avg_speed": 95.0, "raw_speeds": raw_speeds}


def test_ndw_speed_minus_one_is_not_written_and_increments_count(caplog):
    tracker = BadDataTracker("NDW")
    conn = FakeConn()
    with caplog.at_level(logging.WARNING):
        rows = ingest_traffic.process_site(ndw_summary([95.0, -1.0]), tracker)
        if rows:
            from db import save_rows
            save_rows(conn, rows)

    assert rows == []
    assert conn.table == {}                           # nothing reached sensor_readings
    assert tracker.bad_data_count == 1                # ndw_bad_data_count incremented
    assert '"field": "speed"' in caplog.text


def test_ndw_valid_site_produces_flow_and_speed_rows():
    rows = ingest_traffic.process_site(ndw_summary([95.0, 101.0]), BadDataTracker("NDW"))
    assert [r[2] for r in rows] == ["FLOW", "SPEED"]


def test_summarise_site_keeps_raw_minus_one():
    index_map = {"1": {"type": "trafficFlow", "vehicle": "anyVehicle"},
                 "2": {"type": "trafficSpeed", "vehicle": "anyVehicle"}}
    readings = [{"index": "1", "value": "600", "timestamp": "2026-10-01T08:52:00Z"},
                {"index": "2", "value": "-1", "timestamp": "2026-10-01T08:52:00Z"}]
    s = ingest_traffic.summarise_site("site", index_map, readings)
    assert s["raw_speeds"] == [-1.0] and s["avg_speed"] is None


# --- Threshold: more than 10 bad events in an hour -> ONE error -----------------------

def test_threshold_logs_a_single_error(caplog):
    now = [datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc)]
    tracker = BadDataTracker("NDW", clock=lambda: now[0])
    with caplog.at_level(logging.ERROR):
        for _ in range(15):
            tracker.record()
            now[0] += timedelta(minutes=1)
    assert caplog.text.count("BAD_DATA_THRESHOLD_EXCEEDED") == 1
    assert '"count": 11' in caplog.text


def test_threshold_window_is_one_hour():
    now = [datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc)]
    tracker = BadDataTracker("NDW", clock=lambda: now[0])
    for _ in range(11):                               # 11 events spread over > 1 hour
        tracker.record()
        now[0] += timedelta(minutes=7)
    assert tracker._alerted is False
