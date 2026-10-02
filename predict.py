"""
predict.py - load model.pkl and turn traffic into a predicted NO2 value + exceedance risk.

FEATURES (must match training exactly - otherwise training-serving skew):
  total_intensity_veh_per_hr : SUM of the four A27 sites (hrl+hrr+vwd+vwa), veh/h.
                               Not a single site's value - the model never saw those.
  hour_of_day                : UTC hour (0-23) of the NDW traffic snapshot.

EXCEEDANCE THRESHOLD: 40 µg/m³
- Legal limits don't fit hourly data well: the EU hourly limit is 200 µg/m³ (far above
  anything NL10240 reports - observed 15-45), the EU annual limit is 40 (20 from 2030),
  WHO's annual guideline is 10 and its 24-hour guideline 25. Annual/daily values are
  AVERAGES, not hourly thresholds.
- 40 is the upper edge of the "good" class for HOURLY NO2 in the European up-to-date air
  quality classification, and equals the current EU annual limit. So "risk" here means
  "this hour is likely to be above good air quality", not "a legal limit is breached".
"""
import json
import os

import joblib
import numpy as np
import pandas as pd

FEATURES = ["total_intensity_veh_per_hr", "hour_of_day"]
THRESHOLD_UG_M3 = 40.0
STEEPNESS = 0.2          # risk 0.12 at 30, 0.5 at 40, 0.88 at 50 µg/m³
MODEL_PATH = os.environ.get("MODEL_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "model.pkl"))

META_PATH = os.environ.get("MODEL_META_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "model_meta.json"))

_model = None
_meta = None


def model_info():
    """Contents of model_meta.json (written by train_model.py), or {} if it is missing."""
    global _meta
    if _meta is None:
        try:
            with open(META_PATH) as f:
                _meta = json.load(f)
        except (OSError, ValueError):
            _meta = {}
    return _meta


def is_extrapolating(total_intensity_veh_per_hr, hour_of_day):
    """True if the inputs lie outside what the model was trained on; None if unknown."""
    ranges = model_info().get("feature_ranges")
    if not ranges:
        return None
    lo_t, hi_t = ranges["total_intensity_veh_per_hr"]
    lo_h, hi_h = ranges["hour_of_day"]
    return not (lo_t <= total_intensity_veh_per_hr <= hi_t and lo_h <= hour_of_day <= hi_h)


def load_model(path=None):
    global _model
    if _model is None or path:
        _model = joblib.load(path or MODEL_PATH)
    return _model


def exceedance_risk(predicted_no2, threshold=THRESHOLD_UG_M3, steepness=STEEPNESS):
    return float(1 / (1 + np.exp(-steepness * (predicted_no2 - threshold))))


def predict(total_intensity_veh_per_hr, hour_of_day):
    if total_intensity_veh_per_hr is None or not (0 <= int(hour_of_day) <= 23):
        raise ValueError("need total_intensity_veh_per_hr and hour_of_day in 0..23")
    X = pd.DataFrame([[float(total_intensity_veh_per_hr), int(hour_of_day)]], columns=FEATURES)
    raw = float(load_model().predict(X)[0])
    no2 = max(raw, 0.0)   # a linear model can extrapolate below zero; concentrations can't be negative
    return {"no2_ug_m3_predicted": round(no2, 2), "no2_exceedance_risk": round(exceedance_risk(no2), 4),
            "extrapolating": is_extrapolating(float(total_intensity_veh_per_hr), int(hour_of_day))}
