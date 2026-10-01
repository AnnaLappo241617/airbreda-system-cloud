import pandas as pd
import pytest
import requests

import ingest_air
from ingest_air import filter_no2_readings


def test_filter_no2_readings_handles_null_value():
    # Test provided by the Day 1 lab
    df = pd.DataFrame({
        "component": ["NO2", "NO2", "PM10"],
        "value": [18.4, None, 22.1],
        "timestamp": ["2024-01-15T08:00:00Z", "2024-01-15T09:00:00Z", "2024-01-15T08:00:00Z"],
    })
    result = filter_no2_readings(df)
    assert len(result) == 2
    assert result["value"].isnull().sum() == 1  # null NO2 rows are kept, not silently dropped


def test_to_dataframe_maps_api_fields_and_sorts_newest_first():
    records = [
        {"value": 14.0, "timestamp_measured": "2026-09-30T22:00:00+00:00", "formula": "NO2"},
        {"value": 15.7, "timestamp_measured": "2026-09-30T23:00:00+00:00", "formula": "NO2"},
    ]
    df = ingest_air.to_dataframe(records)
    assert list(df.columns) == ["station_id", "timestamp", "component", "value"]
    assert df.iloc[0]["value"] == 15.7
    assert (df["station_id"] == "NL10240").all()


def test_retries_then_gives_up_when_api_unreachable():
    calls = []

    def always_timeout():
        calls.append(1)
        raise requests.Timeout("simulated timeout")

    with pytest.raises(RuntimeError):
        ingest_air.with_retry(always_timeout, attempts=3, sleep=lambda s: None)
    assert len(calls) == 3


def test_client_errors_are_not_retried():
    calls = []

    def forbidden():
        calls.append(1)
        resp = requests.Response()
        resp.status_code = 403
        raise requests.HTTPError("403", response=resp)

    with pytest.raises(requests.HTTPError):
        ingest_air.with_retry(forbidden, attempts=3, sleep=lambda s: None)
    assert len(calls) == 1


def test_save_readings_is_idempotent():
    from tests.fakes import FakeConn
    df = pd.DataFrame({"station_id": ["NL10240"], "timestamp": ["2026-09-30T23:00:00+00:00"],
                       "component": ["NO2"], "value": [15.7]})
    conn = FakeConn()
    assert ingest_air.save_readings(df, conn) == 1
    assert ingest_air.save_readings(df, conn) == 0
