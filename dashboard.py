"""
dashboard.py - AirBreda Day 4: API + dashboard in one FastAPI app.

  GET /site/{site_id}  site_id in hrl | hrr | vwd | vwa
  GET /health          freshness + bad-data counts for both sources
  GET /                HTML page that is just another client of /site/{id}

WHERE EACH /site/{id} FIELD COMES FROM (required comment)
  site_id                     - the URL path, validated against SITES
  no2_ug_m3                   - PostgreSQL sensor_readings: newest NO2 row for station NL10240
                                (value column). The SAME station serves all four sites.
  no2_is_flagged              - sensor_readings.is_flagged of that row (stale/null check, Day 2)
  timestamp                   - sensor_readings.timestamp of that NO2 row (end of its hour, UTC)
  intensity_veh_per_hr        - S3: newest ndw/YYYY-MM-DD/HH-<site>.csv for THIS site
                                (column total_flow_veh_per_hour, written by ingest_traffic.py)
  traffic_timestamp           - the NDW snapshot time inside that CSV
  total_intensity_veh_per_hr  - S3: sum of the four sites' values for the same hour. The model
                                was trained on the four-site TOTAL, so prediction uses the total,
                                not the single site's value (that would be training-serving skew).
  no2_ug_m3_predicted,
  no2_exceedance_risk         - predict.predict(total_intensity, UTC hour of the traffic hour),
                                i.e. model.pkl baked into this image + sigmoid around 40 µg/m³.
                                Because every site uses the same total, all four sites get the same
                                prediction - the model has no per-site information.
  prediction_error            - null, or why there is no prediction

IF predict() RAISES (required comment)
  The request does NOT fail. It degrades: the route still returns HTTP 200 with the real
  no2_ug_m3 and intensity values, sets no2_ug_m3_predicted and no2_exceedance_risk to null,
  and explains why in prediction_error. Reason: the measured values are the trustworthy part of
  the response and come from stores that are working; hiding them because the least reliable
  component (a model trained on a few dozen rows) failed would make the whole page useless.
  Clients must therefore treat the prediction fields as optional. The response is only an
  error when we have no real data at all (503), or for an unknown site (404).
"""
import csv
import io
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

import predict as predictor
from db import get_connection
from quality import log_event, setup_logging

setup_logging()   # one JSON object per log line, same format as the ingestion containers
app = FastAPI(title="AirBreda")
SOURCE = "dashboard"
STALE_AFTER = timedelta(hours=3)   # /health status turns "degraded" if a source is older than this

SITES = {
    "hrl": "RWS01_MONIBAS_0271hrl0063ra",
    "hrr": "RWS01_MONIBAS_0271hrr0063ra",
    "vwd": "RWS01_MONIBAS_0270vwd0063ra",
    "vwa": "RWS01_MONIBAS_0270vwa0063ra",
}
BUCKET = os.environ.get("S3_BUCKET", "")
LOOKBACK_HOURS = int(os.environ.get("TRAFFIC_LOOKBACK_HOURS", "6"))
CACHE_SECONDS = int(os.environ.get("CACHE_SECONDS", "60"))
LOG_DIR = Path(os.environ.get("LOG_DIR", "/logs"))

_s3 = None
_cache = {}


def s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def cached(key, fn):
    """Tiny TTL cache: the page makes 4 calls at once; don't hit RDS/S3 16 times."""
    hit = _cache.get(key)
    if hit and time.monotonic() - hit[0] < CACHE_SECONDS:
        return hit[1]
    value = fn()
    _cache[key] = (time.monotonic(), value)
    return value


def iso_z(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None


# ---------- data access ---------------------------------------------------------------

def latest_no2():
    sql = """SELECT timestamp, value, is_flagged FROM sensor_readings
             WHERE station_id = 'NL10240' AND component = 'NO2'
             ORDER BY timestamp DESC LIMIT 1"""
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    if not row:
        return None
    return {"timestamp": row[0], "value": row[1], "is_flagged": bool(row[2])}


def read_site_csv(day, hour, site):
    """GetObject only (no ListBucket needed): the key is computed from the hour."""
    key = f"ndw/{day}/{hour:02d}-{site}.csv"
    try:
        body = s3().get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8")
    except ClientError as exc:
        # NoSuchKey normally; AccessDenied is what S3 returns for a missing key when the caller
        # lacks s3:ListBucket - both simply mean "not there"
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "AccessDenied", "404"):
            return None
        raise
    row = next(csv.DictReader(io.StringIO(body)))
    return {"intensity": float(row["total_flow_veh_per_hour"]), "timestamp": row["timestamp"], "key": key}


def latest_traffic_hour(now=None):
    """Newest UTC hour (within LOOKBACK_HOURS) for which ALL four site files exist."""
    now = now or datetime.now(timezone.utc)
    for back in range(LOOKBACK_HOURS + 1):
        hour = (now - timedelta(hours=back)).replace(minute=0, second=0, microsecond=0)
        day = hour.strftime("%Y-%m-%d")
        readings = {site: read_site_csv(day, hour.hour, site) for site in SITES}
        if all(readings.values()):
            return {"hour": hour, "sites": readings}
    return None


# ---------- routes ----------------------------------------------------------------------

@app.get("/site/{site_id}")
def site(site_id: str):
    if site_id not in SITES:
        log_event(logging.WARNING, "site_request_unknown", source=SOURCE, site_id=site_id)
        raise HTTPException(404, f"unknown site '{site_id}', use one of {sorted(SITES)}")

    errors = []
    try:
        no2 = cached("no2", latest_no2)
    except Exception as exc:
        no2 = None
        errors.append(f"database: {exc}")
        log_event(logging.ERROR, "db_read_failed", source=SOURCE, site_id=site_id, error=str(exc))
    try:
        traffic = cached("traffic", latest_traffic_hour)
    except (BotoCoreError, ClientError) as exc:
        traffic = None
        errors.append(f"bucket: {exc}")
        log_event(logging.ERROR, "s3_read_failed", source=SOURCE, site_id=site_id, error=str(exc))
    if no2 is None and traffic is None:
        log_event(logging.ERROR, "site_request_failed", source=SOURCE, site_id=site_id, status=503)
        return JSONResponse(status_code=503, content={"site_id": site_id, "error": "no data available",
                                                      "details": errors})

    body = {
        "site_id": site_id,
        "no2_ug_m3": no2["value"] if no2 else None,
        "no2_is_flagged": no2["is_flagged"] if no2 else None,
        "timestamp": iso_z(no2["timestamp"]) if no2 else None,
        "intensity_veh_per_hr": None, "traffic_timestamp": None, "total_intensity_veh_per_hr": None,
        "no2_ug_m3_predicted": None, "no2_exceedance_risk": None, "prediction_error": None,
    }
    if traffic:
        mine = traffic["sites"][site_id]
        body["intensity_veh_per_hr"] = mine["intensity"]
        body["traffic_timestamp"] = mine["timestamp"]
        total = sum(r["intensity"] for r in traffic["sites"].values())
        body["total_intensity_veh_per_hr"] = total
        try:
            body.update(predictor.predict(total, traffic["hour"].hour))
        except Exception as exc:  # degrade, don't fail - see module docstring
            log_event(logging.ERROR, "predict_failed", source=SOURCE, site_id=site_id, error=str(exc))
            body["prediction_error"] = f"prediction unavailable: {exc}"
    else:
        body["prediction_error"] = "no complete traffic hour in the last %d hours" % LOOKBACK_HOURS
    if errors:
        body["data_errors"] = errors
    log_event(logging.INFO if not errors and not body["prediction_error"] else logging.WARNING,
              "site_request", source=SOURCE, site_id=site_id, no2_ug_m3=body["no2_ug_m3"],
              intensity_veh_per_hr=body["intensity_veh_per_hr"],
              no2_exceedance_risk=body["no2_exceedance_risk"], timestamp=body["timestamp"],
              prediction_error=body["prediction_error"])
    return body


def parse_logs(filename, window=timedelta(hours=24)):
    """From the cron log files (mounted read-only at LOG_DIR): last fetch_success, and how
    many DATA_QUALITY_ERROR events were logged in the last 24 h."""
    path = LOG_DIR / filename
    if not path.exists():
        return None, None
    last_ok, bad, since = None, 0, datetime.now(timezone.utc) - window
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if not line.startswith("{"):
                continue
            try:
                ev = json.loads(line)
                at = datetime.fromisoformat(ev["logged_at"].replace("Z", "+00:00"))
            except (ValueError, KeyError):
                continue
            if ev.get("event") == "fetch_success":
                last_ok = ev["logged_at"]
            if ev.get("event") == "DATA_QUALITY_ERROR" and at >= since:
                bad += 1
    return last_ok, bad


def latest_data_time(component):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT MAX(timestamp) FROM sensor_readings WHERE component = %s", (component,))
        return cur.fetchone()[0]


@app.get("/health")
def health():
    """Aggregated health for both ingestion sources, in the shape the assessment requires:
      {"status": "ok"|"degraded",
       "luchtmeetnet": {"last_successful_fetch": ISO8601, "bad_data_count": int, ...},
       "ndw":          {"last_successful_fetch": ISO8601, "bad_data_count": int, ...}}
    On the VM the ingestion containers run once per hour and exit, so they cannot serve their
    own Day 2 /health. Instead this reads their structured JSON log files (mounted read-only at
    LOG_DIR): last_successful_fetch = logged_at of the newest fetch_success event, bad_data_count =
    DATA_QUALITY_ERROR events in the last 24 h. If the logs are not mounted, last_successful_fetch
    falls back to the newest reading in the database and bad_data_count to 0 (marked by
    "source_of_truth": "database").
    status is "degraded" if either source has no successful fetch in the last 3 hours."""
    out, healthy = {"status": "ok"}, True
    for key, logfile, component in (("luchtmeetnet", "air.log", "NO2"), ("ndw", "traffic.log", "FLOW")):
        last_ok, bad = parse_logs(logfile)
        entry = {"last_successful_fetch": last_ok, "bad_data_count": bad, "bad_data_window": "24h",
                 "source_of_truth": "logs"}
        try:
            entry["latest_data_timestamp"] = iso_z(latest_data_time(component))
        except Exception as exc:
            entry["latest_data_timestamp"] = None
            entry["error"] = f"database: {exc}"
        if last_ok is None:
            entry.update(last_successful_fetch=entry["latest_data_timestamp"],
                         bad_data_count=0, source_of_truth="database")
        when = entry["last_successful_fetch"]
        if not when or datetime.now(timezone.utc) - datetime.fromisoformat(when.replace("Z", "+00:00")) > STALE_AFTER:
            healthy = False
        out[key] = entry
    try:
        predictor.load_model()
        out["model_loaded"] = True
    except Exception:
        out["model_loaded"] = False
        healthy = False
    out["status"] = "ok" if healthy else "degraded"
    log_event(logging.INFO if healthy else logging.WARNING, "health_check", source=SOURCE,
              status=out["status"])
    return out


SITE_LABELS = {"hrl": "A27 mainline, direction 1", "hrr": "A27 mainline, direction 2",
               "vwd": "Entry slip road (leaving Breda)", "vwa": "Exit slip road (into Breda)"}


def load_history(hours):
    """Hourly NO2 (actual) + four-site traffic totals from sensor_readings, joined like training:
    traffic hour HH -> NO2 row stamped HH+1. All times UTC."""
    no2_sql = """SELECT timestamp, value, is_flagged FROM sensor_readings
                 WHERE station_id = 'NL10240' AND component = 'NO2'
                   AND timestamp >= now() - make_interval(hours => %s) ORDER BY timestamp"""
    flow_sql = """SELECT h, SUM(value), COUNT(*) FROM (
                    SELECT DISTINCT ON (station_id, date_trunc('hour', timestamp AT TIME ZONE 'UTC'))
                           station_id, date_trunc('hour', timestamp AT TIME ZONE 'UTC') AS h, value
                    FROM sensor_readings
                    WHERE component = 'FLOW' AND station_id = ANY(%s)
                      AND timestamp >= now() - make_interval(hours => %s)
                    ORDER BY station_id, date_trunc('hour', timestamp AT TIME ZONE 'UTC'), timestamp DESC) t
                  GROUP BY h HAVING COUNT(*) = 4 ORDER BY h"""
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(no2_sql, (hours + 1,))
        no2 = {row[0].astimezone(timezone.utc): (row[1], bool(row[2])) for row in cur.fetchall()}
        cur.execute(flow_sql, (list(SITES.values()), hours + 1))
        traffic = {row[0].replace(tzinfo=timezone.utc): float(row[1]) for row in cur.fetchall()}
    points = {}
    for hour_end, (value, flagged) in no2.items():
        points[hour_end] = {"hour_end": iso_z(hour_end), "actual": value, "flagged": flagged,
                            "total_intensity": None, "predicted": None, "extrapolating": None}
    for hour, total in traffic.items():
        hour_end = hour + timedelta(hours=1)
        p = points.setdefault(hour_end, {"hour_end": iso_z(hour_end), "actual": None, "flagged": False,
                                         "total_intensity": None, "predicted": None, "extrapolating": None})
        p["total_intensity"] = total
        try:
            pred = predictor.predict(total, hour.hour)
            p["predicted"], p["extrapolating"] = pred["no2_ug_m3_predicted"], pred["extrapolating"]
        except Exception:
            pass
    return [points[k] for k in sorted(points)]


@app.get("/history")
def history(hours: int = 48):
    """Hourly series for the dashboard chart: actual NO2, predicted NO2 and traffic total."""
    hours = max(1, min(hours, 24 * 14))
    try:
        data = cached(f"history-{hours}", lambda: load_history(hours))
    except Exception as exc:
        log_event(logging.ERROR, "history_failed", source=SOURCE, error=str(exc))
        return JSONResponse(status_code=503, content={"error": str(exc)})
    log_event(logging.INFO, "history_request", source=SOURCE, hours=hours, points=len(data))
    return {"hours": hours, "points": data}


@app.get("/model")
def model():
    """What the dashboard's model was trained on (from model_meta.json)."""
    meta = predictor.model_info()
    keys = ("trained_at", "rows", "features", "coefficients", "intercept", "metrics", "baseline_mae",
            "threshold_ug_m3", "sklearn_version", "data_range", "feature_ranges")
    return {k: meta.get(k) for k in keys} | {"site_labels": SITE_LABELS}


PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AirBreda — NO₂ & traffic at the A27 / Breda interchange</title>
<style>
 :root { --bg:#f4f2ee; --card:#fffdf9; --ink:#1f2421; --muted:#6b706b; --line:#e3ded5;
         --air:#2f6f5e; --model:#c0662b; --traffic:#8a94a6; --ok:#2f6f5e; --warn:#b7791f; --bad:#b33a3a; }
 * { box-sizing: border-box; }
 body { margin:0; background:var(--bg); color:var(--ink); font: 15px/1.5 "Segoe UI", system-ui, -apple-system, Arial, sans-serif; }
 main { max-width: 1080px; margin: 0 auto; padding: 28px 18px 56px; }
 header { display:flex; flex-wrap:wrap; justify-content:space-between; align-items:flex-end; gap:12px; margin-bottom:22px; }
 h1 { font-size: 1.6rem; margin:0; letter-spacing:-0.01em; }
 .sub { color:var(--muted); margin:4px 0 0; max-width: 640px; }
 .status { display:flex; align-items:center; gap:8px; font-size:.9rem; color:var(--muted); }
 .dot { width:10px; height:10px; border-radius:50%; background:var(--muted); }
 .dot.ok { background:var(--ok); } .dot.degraded { background:var(--warn); } .dot.down { background:var(--bad); }
 .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap:14px; }
 .card { background:var(--card); border:1px solid var(--line); border-radius:14px; padding:18px 20px; }
 .k { font-size:.8rem; text-transform:uppercase; letter-spacing:.06em; color:var(--muted); }
 .v { font-size:2.3rem; font-weight:650; margin:6px 0 2px; font-variant-numeric: tabular-nums; }
 .v small { font-size:1rem; font-weight:500; color:var(--muted); }
 .note { color:var(--muted); font-size:.88rem; }
 .bar-air { border-top:4px solid var(--air); } .bar-model { border-top:4px solid var(--model); } .bar-traffic { border-top:4px solid var(--traffic); }
 .pill { display:inline-block; padding:1px 9px; border-radius:999px; font-size:.8rem; font-weight:600; }
 .low { background:#e2efe9; color:var(--ok);} .mid { background:#f7ebd2; color:var(--warn);} .high { background:#f6dddd; color:var(--bad);}
 .warn { margin-top:8px; font-size:.85rem; color:var(--warn); }
 section { margin-top:22px; }
 h2 { font-size:1.05rem; margin:0 0 10px; }
 #chart { width:100%; height:300px; display:block; }
 .legend { display:flex; gap:18px; flex-wrap:wrap; font-size:.85rem; color:var(--muted); margin-top:6px; }
 .sw { display:inline-block; width:18px; height:3px; vertical-align:middle; margin-right:6px; }
 table { width:100%; border-collapse:collapse; }
 th, td { padding:10px 8px; text-align:left; border-bottom:1px solid var(--line); font-variant-numeric: tabular-nums; }
 th { font-size:.78rem; text-transform:uppercase; letter-spacing:.05em; color:var(--muted); font-weight:600; }
 td .track { height:8px; background:#ece7de; border-radius:4px; min-width:80px; }
 td .fill { height:8px; background:var(--traffic); border-radius:4px; }
 .two { display:grid; grid-template-columns: 1.4fr 1fr; gap:14px; } @media (max-width: 800px) { .two { grid-template-columns: 1fr; } }
 dl { display:grid; grid-template-columns: auto 1fr; gap:4px 14px; margin:0; font-size:.9rem; } dt { color:var(--muted); }
 footer { margin-top:26px; color:var(--muted); font-size:.82rem; }
 #stale { display:none; margin-top:14px; padding:10px 14px; border-radius:10px; background:#f6dddd; color:var(--bad); font-weight:600; }
</style></head>
<body><main>
<header>
  <div>
    <h1>AirBreda · A27 / Breda interchange</h1>
    <p class="sub">Nitrogen dioxide measured at Luchtmeetnet station NL10240 (Breda-Tilburgseweg), next to traffic counted by NDW at four A27 measurement points — and what a simple model trained on this data expects.</p>
  </div>
  <div class="status"><span class="dot" id="dot"></span><span id="health">checking sources…</span></div>
</header>

<div class="grid">
  <div class="card bar-air"><div class="k">Measured NO₂</div><div class="v" id="actual">—</div><div class="note" id="actual_ts"></div><div class="note" id="actual_vs"></div></div>
  <div class="card bar-model"><div class="k">Model expectation</div><div class="v" id="pred">—</div><div class="note" id="risk"></div><div class="warn" id="extra"></div></div>
  <div class="card bar-traffic"><div class="k">Traffic, all four sites</div><div class="v" id="total">—</div><div class="note" id="split"></div><div class="note" id="traffic_ts"></div></div>
</div>
<div id="stale"></div>

<section class="card">
  <h2>Last 48 hours</h2>
  <svg id="chart" role="img" aria-label="NO2 measured, NO2 expected by the model, and traffic over the last 48 hours"></svg>
  <div class="legend"><span><span class="sw" style="background:var(--air)"></span>Measured NO₂ (µg/m³)</span>
    <span><span class="sw" style="background:var(--model)"></span>Model expectation (µg/m³)</span>
    <span><span class="sw" style="background:var(--traffic);height:10px;opacity:.45"></span>Traffic (veh/h, right axis)</span>
    <span><span class="sw" style="border-top:2px dashed var(--bad);height:0"></span>40 µg/m³ threshold</span></div>
</section>

<section class="two">
  <div class="card">
    <h2>Measurement points</h2>
    <table><thead><tr><th>Site</th><th>Location</th><th>Vehicles/h</th><th></th></tr></thead><tbody id="sites"></tbody></table>
    <p class="note">One air-quality station covers all four points; the model uses their total, so its expectation is the same for each.</p>
  </div>
  <div class="card">
    <h2>About the model</h2>
    <dl id="model"></dl>
    <p class="note" id="model_note"></p>
  </div>
</section>

<footer id="foot">Data: Luchtmeetnet (RIVM) and NDW open data, collected hourly. Times shown in your local time zone.</footer>
</main>
<script>
const SITES = ["hrl", "hrr", "vwd", "vwa"];
const STALE_MINUTES = 150;
const $ = id => document.getElementById(id);
const fmt = (v, d = 1) => v === null || v === undefined ? "—" : Number(v).toFixed(d);
const time = ts => ts ? new Date(ts).toLocaleString([], { weekday: "short", hour: "2-digit", minute: "2-digit" }) : "—";
const ageMin = ts => ts ? (Date.now() - new Date(ts).getTime()) / 60000 : Infinity;
function pill(r) {
  if (r === null || r === undefined) return "";
  const c = r < 0.3 ? "low" : r < 0.7 ? "mid" : "high";
  return `<span class="pill ${c}">${Math.round(r * 100)}% chance above 40 µg/m³</span>`;
}

async function loadCurrent(labels) {
  const res = await Promise.all(SITES.map(s => fetch(`/site/${s}`).then(r => r.json()).catch(e => ({ site_id: s, error: String(e) }))));
  const ok = res.filter(r => !r.error), f = ok[0] || {};
  $("actual").innerHTML = f.no2_ug_m3 == null ? "—" : `${fmt(f.no2_ug_m3)} <small>µg/m³</small>`;
  $("actual_ts").textContent = f.timestamp ? `average for the hour ending ${time(f.timestamp)}${f.no2_is_flagged ? " · flagged as suspect" : ""}` : "";
  $("actual_vs").textContent = f.no2_ug_m3 == null ? "" : (f.no2_ug_m3 < 40 ? "below" : "above") + " the 40 µg/m³ “good air” boundary";
  $("pred").innerHTML = f.no2_ug_m3_predicted == null ? "—" : `${fmt(f.no2_ug_m3_predicted)} <small>µg/m³</small>`;
  $("risk").innerHTML = f.no2_exceedance_risk == null ? (f.prediction_error || "") : pill(f.no2_exceedance_risk);
  $("extra").textContent = f.extrapolating ? "Outside the hours/traffic the model was trained on — treat as a rough guess." : "";
  const by = Object.fromEntries(ok.map(r => [r.site_id, r.intensity_veh_per_hr || 0]));
  const total = Object.values(by).reduce((a, b) => a + b, 0);
  $("total").innerHTML = ok.length ? `${fmt(total, 0)} <small>veh/h</small>` : "—";
  $("split").textContent = ok.length ? `mainline ${fmt((by.hrl || 0) + (by.hrr || 0), 0)} · slip roads ${fmt((by.vwd || 0) + (by.vwa || 0), 0)}` : "";
  $("traffic_ts").textContent = f.traffic_timestamp ? `snapshot ${time(f.traffic_timestamp)}` : "";
  const max = Math.max(1, ...Object.values(by));
  $("sites").innerHTML = res.map(r => r.error ? `<tr><td>${r.site_id}</td><td colspan="3">unavailable</td></tr>` :
    `<tr><td><b>${r.site_id}</b></td><td>${labels[r.site_id] || ""}</td><td>${fmt(r.intensity_veh_per_hr, 0)}</td>
     <td><div class="track"><div class="fill" style="width:${100 * (r.intensity_veh_per_hr || 0) / max}%"></div></div></td></tr>`).join("");
  const age = Math.max(ageMin(f.timestamp), ageMin(f.traffic_timestamp));
  $("stale").style.display = age > STALE_MINUTES ? "block" : "none";
  $("stale").textContent = `Data is more than ${STALE_MINUTES} minutes old — the hourly collection may have stopped.`;
}

async function loadHealth() {
  try {
    const h = await (await fetch("/health")).json();
    $("dot").className = "dot " + (h.status === "ok" ? "ok" : "degraded");
    $("health").textContent = `${h.status === "ok" ? "All sources live" : "Degraded"} · air ${time(h.luchtmeetnet?.last_successful_fetch)} · traffic ${time(h.ndw?.last_successful_fetch)}`;
  } catch (e) { $("dot").className = "dot down"; $("health").textContent = "health check failed"; }
}

function drawChart(points) {
  const svg = $("chart"), W = svg.clientWidth || 900, H = 300, m = { l: 44, r: 52, t: 14, b: 30 };
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
  if (!points.length) { svg.innerHTML = `<text x="${W/2}" y="${H/2}" text-anchor="middle" fill="#6b706b">No history yet</text>`; return; }
  const t = points.map(p => new Date(p.hour_end).getTime());
  const t0 = Math.min(...t) - 1800e3, t1 = Math.max(...t) + 1800e3;
  const no2Max = Math.max(50, ...points.map(p => Math.max(p.actual || 0, p.predicted || 0))) * 1.1;
  const trMax = Math.max(1000, ...points.map(p => p.total_intensity || 0)) * 1.1;
  const x = v => m.l + (v - t0) / (t1 - t0) * (W - m.l - m.r);
  const y = v => H - m.b - v / no2Max * (H - m.t - m.b);
  const y2 = v => H - m.b - v / trMax * (H - m.t - m.b);
  const bw = Math.max(3, (W - m.l - m.r) / ((t1 - t0) / 3600e3) * 0.6);
  let g = "";
  for (let i = 0; i <= 4; i++) { const v = no2Max / 4 * i;
    g += `<line x1="${m.l}" x2="${W - m.r}" y1="${y(v)}" y2="${y(v)}" stroke="#e3ded5"/>`
       + `<text x="${m.l - 6}" y="${y(v) + 4}" text-anchor="end" font-size="11" fill="#6b706b">${Math.round(v)}</text>`
       + `<text x="${W - m.r + 6}" y="${y2(trMax / 4 * i) + 4}" font-size="11" fill="#8a94a6">${Math.round(trMax / 4 * i)}</text>`; }
  points.forEach(p => { if (p.total_intensity != null) { const cx = x(new Date(p.hour_end).getTime());
    g += `<rect x="${cx - bw / 2}" y="${y2(p.total_intensity)}" width="${bw}" height="${H - m.b - y2(p.total_intensity)}" fill="#8a94a6" opacity=".35"><title>${time(p.hour_end)}: ${fmt(p.total_intensity, 0)} veh/h</title></rect>`; } });
  g += `<line x1="${m.l}" x2="${W - m.r}" y1="${y(40)}" y2="${y(40)}" stroke="#b33a3a" stroke-dasharray="5 4"/>`;
  const line = (key, color, dash) => {
    const pts = points.filter(p => p[key] != null);
    if (!pts.length) return "";
    const d = pts.map((p, i) => `${i ? "L" : "M"}${x(new Date(p.hour_end).getTime())},${y(p[key])}`).join(" ");
    return `<path d="${d}" fill="none" stroke="${color}" stroke-width="2.5" ${dash ? 'stroke-dasharray="6 4"' : ""}/>`
      + pts.map(p => `<circle cx="${x(new Date(p.hour_end).getTime())}" cy="${y(p[key])}" r="3.5" fill="${color}"><title>${time(p.hour_end)}: ${fmt(p[key])} µg/m³</title></circle>`).join("");
  };
  g += line("predicted", "#c0662b", true) + line("actual", "#2f6f5e", false);
  const span = (t1 - t0) / 3600e3, step = span > 30 ? 6 : span > 12 ? 3 : 1;
  for (let h = Math.ceil(t0 / 3600e3); h * 3600e3 <= t1; h++) { if (h % step) continue;
    const d = new Date(h * 3600e3);
    g += `<text x="${x(h * 3600e3)}" y="${H - 10}" text-anchor="middle" font-size="11" fill="#6b706b">${d.getHours().toString().padStart(2, "0")}:00</text>`; }
  svg.innerHTML = g;
}

async function loadHistory() {
  try { const h = await (await fetch("/history?hours=48")).json(); drawChart(h.points || []); }
  catch (e) { $("chart").innerHTML = ""; }
}

async function loadModel() {
  try {
    const m = await (await fetch("/model")).json();
    const c = m.coefficients || {};
    $("model").innerHTML = `<dt>Type</dt><dd>Linear regression (scikit-learn ${m.sklearn_version || "?"})</dd>
      <dt>Trained on</dt><dd>${m.rows ?? "?"} hours of this station's own data</dd>
      <dt>Trained at</dt><dd>${m.trained_at ? time(m.trained_at) : "—"}</dd>
      <dt>Traffic effect</dt><dd>${c.total_intensity_veh_per_hr != null ? `${(c.total_intensity_veh_per_hr * 1000 >= 0 ? "+" : "")}${fmt(c.total_intensity_veh_per_hr * 1000)} µg/m³ per 1,000 veh/h` : "—"}</dd>
      <dt>Hours seen</dt><dd>${m.feature_ranges ? `${m.feature_ranges.hour_of_day[0]}–${m.feature_ranges.hour_of_day[1]} UTC` : (m.data_range ? m.data_range.map(time).join(" → ") : "—")}</dd>`;
    $("model_note").textContent = (m.rows || 0) < 24
      ? "Very little training data so far — the expectation shows that the pipeline works, not that traffic alone predicts NO₂. Weather is not yet included."
      : "Expectations ignore weather, which strongly affects NO₂; read them as a rough guide.";
    return m.site_labels || {};
  } catch (e) { return {}; }
}

async function refresh() {
  const labels = await loadModel();
  await Promise.all([loadCurrent(labels), loadHealth(), loadHistory()]);
  $("foot").textContent = `Data: Luchtmeetnet (RIVM) and NDW open data, collected hourly · times in your local time zone · page refreshed ${new Date().toLocaleTimeString()}`;
}
refresh();
setInterval(refresh, 5 * 60 * 1000);
window.addEventListener("resize", loadHistory);
</script></body></html>"""


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE
