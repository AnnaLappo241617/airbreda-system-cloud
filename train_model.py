"""
train_model.py - Day 4 Lab 1: train, evaluate and save the NO2 regression.

Reads training_data.csv, writes model.pkl (+ model_meta.json and no2_vs_traffic.png) and
prints an honest evaluation. Run on the laptop after build_training_data.py.
"""
import json
import sys
from datetime import datetime, timezone

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

from predict import FEATURES, THRESHOLD_UG_M3, exceedance_risk

MIN_ROWS_FOR_SPLIT = 30   # below this a held-out test set is a handful of points - meaningless


def evaluate(model, X, y, label):
    pred = model.predict(X)
    r2 = r2_score(y, pred) if len(y) > 1 else float("nan")
    mae = mean_absolute_error(y, pred)
    print(f"  {label:<26} n={len(y):<4} R²={r2:6.3f}   MAE={mae:6.2f} µg/m³")
    return {"n": int(len(y)), "r2": None if np.isnan(r2) else round(float(r2), 4), "mae": round(float(mae), 3)}


def main(path="training_data.csv"):
    df = pd.read_csv(path)
    n = len(df)
    print(f"Rows: {n}")
    if n < len(FEATURES) + 2:
        print(f"ERROR: need at least {len(FEATURES) + 2} rows to fit {len(FEATURES)} features + "
              "intercept with any residual at all. Let the cron jobs collect more hours first.")
        return 1

    X, y = df[FEATURES], df["no2_ug_m3"]
    metrics = {}
    if n >= MIN_ROWS_FOR_SPLIT:
        X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.25, random_state=42)
        held = LinearRegression().fit(X_tr, y_tr)
        print("Evaluation:")
        metrics["train"] = evaluate(held, X_tr, y_tr, "train split")
        metrics["test"] = evaluate(held, X_te, y_te, "held-out test split")
    else:
        print(f"Evaluation: only {n} rows (< {MIN_ROWS_FOR_SPLIT}) -> NO train/test split.")
        print("  The numbers below are IN-SAMPLE: they show how well the line fits the points it")
        print("  was trained on, not how well it predicts new hours. Expect them to be optimistic.")

    model = LinearRegression().fit(X, y)          # final model always uses all rows
    metrics["in_sample_all_rows"] = evaluate(model, X, y, "in-sample (all rows)")
    baseline_mae = float(np.mean(np.abs(y - y.mean())))
    print(f"  baseline (always predict the mean) MAE={baseline_mae:6.2f} µg/m³")

    print("\nCoefficients:")
    for name, coef in zip(FEATURES, model.coef_):
        print(f"  {name:<28} {coef:+.5f}")
    print(f"  {'intercept':<28} {model.intercept_:+.3f}")
    c_traffic = model.coef_[FEATURES.index("total_intensity_veh_per_hr")]
    print("  -> traffic coefficient is", "POSITIVE (more traffic, more NO2) as expected"
          if c_traffic > 0 else "NOT positive - report this honestly in your reflection")
    print(f"     i.e. +1,000 veh/h changes predicted NO2 by {1000 * c_traffic:+.2f} µg/m³")

    # Stretch goal: logistic regression on 'did this hour exceed the threshold'
    labels = (y > THRESHOLD_UG_M3).astype(int)
    logistic = None
    if labels.nunique() == 2 and labels.value_counts().min() >= 2:
        logistic = LogisticRegression().fit(X, labels)
        print(f"\nLogistic regression trained ({labels.sum()} of {n} hours above {THRESHOLD_UG_M3}).")
        cmp = df[FEATURES + ["no2_ug_m3"]].head(5).copy()
        cmp["risk_from_regression"] = [exceedance_risk(p) for p in model.predict(X.head(5))]
        cmp["risk_from_logistic"] = logistic.predict_proba(X.head(5))[:, 1].round(3)
        print(cmp.to_string(index=False))
        joblib.dump(logistic, "model_logistic.pkl")
    else:
        print(f"\nLogistic regression SKIPPED: {labels.sum()} of {n} hours above {THRESHOLD_UG_M3} µg/m³ "
              "- need at least 2 hours on each side of the threshold.")

    joblib.dump(model, "model.pkl")
    meta = {"trained_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "rows": n, "features": FEATURES, "target": "no2_ug_m3",
            "coefficients": dict(zip(FEATURES, map(float, model.coef_))),
            "intercept": float(model.intercept_), "metrics": metrics,
            "baseline_mae": round(baseline_mae, 3), "threshold_ug_m3": THRESHOLD_UG_M3,
            "sklearn_version": sklearn.__version__, "logistic_trained": logistic is not None,
            "data_range": [str(df["traffic_hour"].min()), str(df["traffic_hour"].max())]}
    with open("model_meta.json", "w") as f:
        json.dump(meta, f, indent=2)
    print("\nSaved model.pkl and model_meta.json")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(7, 4.5))
        sc = ax.scatter(df["total_intensity_veh_per_hr"], y, c=df["hour_of_day"], cmap="viridis")
        fig.colorbar(sc, label="hour of day (UTC)")
        ax.set_xlabel("total intensity, 4 A27 sites (veh/h)")
        ax.set_ylabel("NO₂ at NL10240 (µg/m³)")
        ax.set_title(f"NO₂ vs traffic — {n} joined hours")
        fig.tight_layout()
        fig.savefig("no2_vs_traffic.png", dpi=150)
        print("Saved no2_vs_traffic.png")
    except ImportError:
        print("(matplotlib not installed - skipped the plot: pip install matplotlib)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
