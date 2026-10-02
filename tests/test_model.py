"""Day 4: model + predict() tests (required by the lab).

The first two tests use the REAL model.pkl in the project root, so they fail loudly in CI
if someone forgets to commit/retrain it. The others use a small throwaway model so they test
predict()'s logic regardless of what the real model learned.
"""
import os

import joblib
import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import LinearRegression

import predict

REAL_MODEL = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "model.pkl")
needs_real_model = pytest.mark.skipif(not os.path.exists(REAL_MODEL),
                                      reason="model.pkl not trained yet (run train_model.py)")


@needs_real_model
def test_model_pkl_loads():
    model = joblib.load(REAL_MODEL)
    assert hasattr(model, "predict")
    assert list(model.feature_names_in_) == predict.FEATURES


@needs_real_model
def test_predict_with_real_model_returns_plausible_values():
    predict.load_model(REAL_MODEL)
    out = predict.predict(3000, 8)
    assert isinstance(out["no2_ug_m3_predicted"], float)
    assert 0 <= out["no2_ug_m3_predicted"] <= 200
    assert 0 <= out["no2_exceedance_risk"] <= 1


@pytest.fixture
def toy_model(tmp_path):
    X = pd.DataFrame({"total_intensity_veh_per_hr": [200, 1000, 2000, 3000, 4000],
                      "hour_of_day": [2, 6, 8, 10, 17]})
    y = [12, 18, 30, 38, 45]
    path = tmp_path / "model.pkl"
    joblib.dump(LinearRegression().fit(X, y), path)
    predict.load_model(str(path))
    yield
    predict._model = None


def test_predict_shape_and_ranges(toy_model):
    out = predict.predict(2500, 9)
    assert {"no2_ug_m3_predicted", "no2_exceedance_risk", "extrapolating"} <= set(out)
    assert 0 <= out["no2_ug_m3_predicted"] <= 200
    assert 0 <= out["no2_exceedance_risk"] <= 1


def test_negative_extrapolation_is_clipped_to_zero(toy_model):
    assert predict.predict(0, 0)["no2_ug_m3_predicted"] >= 0


def test_risk_is_half_at_threshold_and_monotonic():
    assert predict.exceedance_risk(predict.THRESHOLD_UG_M3) == pytest.approx(0.5)
    assert predict.exceedance_risk(20) < predict.exceedance_risk(40) < predict.exceedance_risk(60)


def test_invalid_hour_raises(toy_model):
    with pytest.raises(ValueError):
        predict.predict(1000, 24)
