"""Day 4: /site/{id} behaviour, including graceful degradation when predict() fails."""
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

import dashboard


@pytest.fixture
def client(monkeypatch):
    dashboard._cache.clear()
    monkeypatch.setattr(dashboard, "latest_no2", lambda: {
        "timestamp": datetime(2026, 10, 1, 11, tzinfo=timezone.utc), "value": 22.68, "is_flagged": False})
    monkeypatch.setattr(dashboard, "latest_traffic_hour", lambda: {
        "hour": datetime(2026, 10, 1, 10, tzinfo=timezone.utc),
        "sites": {s: {"intensity": v, "timestamp": "2026-10-01T10:43:00Z", "key": f"k-{s}"}
                  for s, v in {"hrl": 1980.0, "hrr": 960.0, "vwd": 360.0, "vwa": 840.0}.items()}})
    monkeypatch.setattr(dashboard.predictor, "predict",
                        lambda total, hour: {"no2_ug_m3_predicted": 30.0, "no2_exceedance_risk": 0.12})
    return TestClient(dashboard.app)


def test_site_returns_contract_fields(client):
    body = client.get("/site/hrl").json()
    assert body["site_id"] == "hrl"
    assert body["no2_ug_m3"] == 22.68
    assert body["intensity_veh_per_hr"] == 1980.0
    assert body["total_intensity_veh_per_hr"] == 4140.0
    assert 0 <= body["no2_exceedance_risk"] <= 1
    assert body["timestamp"] == "2026-10-01T11:00:00Z"


def test_prediction_uses_four_site_total(client, monkeypatch):
    seen = {}
    monkeypatch.setattr(dashboard.predictor, "predict",
                        lambda total, hour: seen.update(total=total, hour=hour) or
                        {"no2_ug_m3_predicted": 1.0, "no2_exceedance_risk": 0.0})
    client.get("/site/vwd")
    assert seen == {"total": 4140.0, "hour": 10}


def test_predict_failure_degrades_instead_of_failing(client, monkeypatch):
    def boom(*a):
        raise RuntimeError("model.pkl missing")
    monkeypatch.setattr(dashboard.predictor, "predict", boom)
    r = client.get("/site/hrr")
    assert r.status_code == 200
    body = r.json()
    assert body["no2_ug_m3"] == 22.68 and body["intensity_veh_per_hr"] == 960.0
    assert body["no2_ug_m3_predicted"] is None and body["no2_exceedance_risk"] is None
    assert "model.pkl missing" in body["prediction_error"]


def test_unknown_site_is_404(client):
    assert client.get("/site/xyz").status_code == 404


def test_index_page_calls_the_api(client):
    html = client.get("/").text
    assert "/site/${s}" in html and "setInterval" in html and "/history" in html


def test_health_has_required_shape(monkeypatch, tmp_path):
    (tmp_path / "air.log").write_text(
        '{"level": "INFO", "logged_at": "2099-01-01T10:10:03Z", "event": "fetch_success"}\n'
        '{"level": "WARNING", "logged_at": "2099-01-01T10:10:03Z", "event": "DATA_QUALITY_ERROR"}\n')
    (tmp_path / "traffic.log").write_text("Found credentials from IAM Role: x\n"
        '{"level": "INFO", "logged_at": "2099-01-01T10:20:51Z", "event": "fetch_success"}\n')
    monkeypatch.setattr(dashboard, "LOG_DIR", tmp_path)
    monkeypatch.setattr(dashboard, "latest_data_time", lambda c: datetime(2026, 10, 1, 12, tzinfo=timezone.utc))
    monkeypatch.setattr(dashboard.predictor, "load_model", lambda *a: object())
    body = TestClient(dashboard.app).get("/health").json()
    assert body["status"] in ("ok", "degraded")
    for key in ("luchtmeetnet", "ndw"):
        assert isinstance(body[key]["bad_data_count"], int)
        assert body[key]["last_successful_fetch"].endswith("Z")
    assert body["luchtmeetnet"]["last_successful_fetch"] == "2099-01-01T10:10:03Z"
    assert body["luchtmeetnet"]["bad_data_count"] == 1 and body["ndw"]["bad_data_count"] == 0


def test_health_falls_back_to_database_without_logs(monkeypatch, tmp_path):
    monkeypatch.setattr(dashboard, "LOG_DIR", tmp_path / "missing")
    monkeypatch.setattr(dashboard, "latest_data_time", lambda c: datetime(2026, 10, 1, 12, tzinfo=timezone.utc))
    monkeypatch.setattr(dashboard.predictor, "load_model", lambda *a: object())
    body = TestClient(dashboard.app).get("/health").json()
    assert body["ndw"]["last_successful_fetch"] == "2026-10-01T12:00:00Z"
    assert body["ndw"]["bad_data_count"] == 0 and body["ndw"]["source_of_truth"] == "database"
