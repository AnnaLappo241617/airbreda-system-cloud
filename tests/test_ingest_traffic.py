from ingest_traffic import build_key, site_code


def test_site_code_extracts_suffix():
    assert site_code("RWS01_MONIBAS_0271hrl0063ra") == "hrl"
    assert site_code("RWS01_MONIBAS_0270vwa0063ra") == "vwa"


def test_build_key_uses_measurement_time_in_utc():
    assert build_key("2026-09-30T23:01:00Z", "hrr") == "ndw/2026-09-30/23-hrr.csv"
    # 01:30 Dutch summer time (UTC+2) is 23:30 UTC on the previous day
    assert build_key("2026-10-01T01:30:00+02:00", "vwd") == "ndw/2026-09-30/23-vwd.csv"
