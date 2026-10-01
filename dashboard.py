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

log = logging.getLogger("dashboard")
app = FastAPI(title="AirBreda")

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
        raise HTTPException(404, f"unknown site '{site_id}', use one of {sorted(SITES)}")

    errors = []
    try:
        no2 = cached("no2", latest_no2)
    except Exception as exc:
        no2 = None
        errors.append(f"database: {exc}")
    try:
        traffic = cached("traffic", latest_traffic_hour)
    except (BotoCoreError, ClientError) as exc:
        traffic = None
        errors.append(f"bucket: {exc}")
    if no2 is None and traffic is None:
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
            log.warning(json.dumps({"event": "predict_failed", "error": str(exc)}))
            body["prediction_error"] = f"prediction unavailable: {exc}"
    else:
        body["prediction_error"] = "no complete traffic hour in the last %d hours" % LOOKBACK_HOURS
    if errors:
        body["data_errors"] = errors
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
    """Same fields as Day 2's per-container /health, for both sources. On the VM the ingestion
    containers run once per hour and exit, so they can't serve /health themselves; this reads
    their log files instead, plus the newest timestamp each source has in the database."""
    out = []
    for source, logfile, component in (("Luchtmeetnet", "air.log", "NO2"), ("NDW", "traffic.log", "FLOW")):
        last_ok, bad = parse_logs(logfile)
        try:
            newest = iso_z(latest_data_time(component))
        except Exception as exc:
            newest = f"error: {exc}"
        out.append({"source": source, "last_successful_fetch": last_ok,
                    "bad_data_count": bad, "bad_data_window": "24h",
                    "latest_data_timestamp": newest})
    try:
        predictor.load_model()
        model_ok = True
    except Exception:
        model_ok = False
    return {"sources": out, "model_loaded": model_ok,
            "logs_mounted": LOG_DIR.exists()}


PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AirBreda — A27 / Breda</title>
<style>
 :root { --bg:#f6f7f9; --card:#fff; --ink:#1d2330; --muted:#667085; --line:#e4e7ec;
         --ok:#1a7f37; --warn:#b54708; --bad:#b42318; }
 body { margin:0; font-family: system-ui, Segoe UI, Arial, sans-serif; background:var(--bg); color:var(--ink); }
 main { max-width: 960px; margin: 0 auto; padding: 24px 16px 48px; }
 h1 { font-size: 1.5rem; margin: 0 0 4px; } .sub { color:var(--muted); margin:0 0 20px; }
 .row { display:grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap:12px; margin-bottom:16px; }
 .card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:16px; }
 .label { color:var(--muted); font-size:.85rem; } .big { font-size:2rem; font-weight:650; margin:4px 0; }
 table { width:100%; border-collapse: collapse; background:var(--card); border:1px solid var(--line); border-radius:12px; overflow:hidden; }
 th, td { padding:10px 12px; text-align:left; border-bottom:1px solid var(--line); font-variant-numeric: tabular-nums; }
 th { font-size:.85rem; color:var(--muted); font-weight:600; }
 .pill { padding:2px 8px; border-radius:999px; font-size:.8rem; font-weight:600; }
 .low { background:#e7f6ec; color:var(--ok);} .mid { background:#fef0c7; color:var(--warn);} .high { background:#fee4e2; color:var(--bad);}
 #status { margin-top:16px; color:var(--muted); font-size:.9rem; } #status.stale { color:var(--bad); font-weight:600; }
</style></head>
<body><main>
 <h1>AirBreda — A27 / Breda interchange</h1>
 <p class="sub">Measured NO₂ at Luchtmeetnet station NL10240 vs. a linear model driven by NDW traffic.</p>
 <div class="row">
  <div class="card"><div class="label">Actual NO₂ (measured)</div><div class="big" id="actual">…</div><div class="label" id="actual_ts"></div></div>
  <div class="card"><div class="label">Predicted NO₂ (model)</div><div class="big" id="pred">…</div><div class="label" id="risk"></div></div>
  <div class="card"><div class="label">Total traffic, 4 sites</div><div class="big" id="total">…</div><div class="label" id="traffic_ts"></div></div>
 </div>
 <table><thead><tr><th>Site</th><th>Intensity (veh/h)</th><th>Predicted NO₂</th><th>Exceedance risk</th></tr></thead>
  <tbody id="sites"></tbody></table>
 <p class="label">All four sites share one air-quality station, and the model uses the four-site total, so the prediction is the same for every site.</p>
 <div id="status">Loading…</div>
</main>
<script>
const SITES = ["hrl", "hrr", "vwd", "vwa"];
const STALE_MINUTES = 150;
const fmt = (v, d = 1) => v === null || v === undefined ? "—" : Number(v).toFixed(d);
function riskPill(r) {
  if (r === null || r === undefined) return "—";
  const cls = r < 0.3 ? "low" : r < 0.7 ? "mid" : "high";
  return `<span class="pill ${cls}">${(r * 100).toFixed(0)}%</span>`;
}
function ageMinutes(ts) { return ts ? (Date.now() - new Date(ts).getTime()) / 60000 : Infinity; }
async function refresh() {
  const results = await Promise.all(SITES.map(s => fetch(`/site/${s}`).then(r => r.json()).catch(e => ({ site_id: s, error: String(e) }))));
  const ok = results.filter(r => !r.error);
  const first = ok[0] || {};
  document.getElementById("actual").textContent = first.no2_ug_m3 == null ? "—" : `${fmt(first.no2_ug_m3)} µg/m³`;
  document.getElementById("actual_ts").textContent = first.timestamp ? `hour ending ${new Date(first.timestamp).toLocaleString()}` + (first.no2_is_flagged ? " · flagged" : "") : "";
  document.getElementById("pred").textContent = first.no2_ug_m3_predicted == null ? "—" : `${fmt(first.no2_ug_m3_predicted)} µg/m³`;
  document.getElementById("risk").innerHTML = first.no2_exceedance_risk == null ? (first.prediction_error || "") : `exceedance risk ${riskPill(first.no2_exceedance_risk)} (threshold 40 µg/m³)`;
  const total = ok.reduce((sum, r) => sum + (r.intensity_veh_per_hr || 0), 0);
  document.getElementById("total").textContent = ok.length ? `${fmt(total, 0)} veh/h` : "—";
  document.getElementById("traffic_ts").textContent = first.traffic_timestamp ? `snapshot ${new Date(first.traffic_timestamp).toLocaleString()}` : "";
  document.getElementById("sites").innerHTML = results.map(r => r.error
    ? `<tr><td>${r.site_id}</td><td colspan="3">error: ${r.error}</td></tr>`
    : `<tr><td>${r.site_id}</td><td>${fmt(r.intensity_veh_per_hr, 0)}</td><td>${fmt(r.no2_ug_m3_predicted)}</td><td>${riskPill(r.no2_exceedance_risk)}</td></tr>`).join("");
  const age = Math.max(ageMinutes(first.timestamp), ageMinutes(first.traffic_timestamp));
  const status = document.getElementById("status");
  status.className = age > STALE_MINUTES ? "stale" : "";
  status.textContent = `Last updated: NO₂ ${first.timestamp ? new Date(first.timestamp).toLocaleTimeString() : "—"}, traffic ${first.traffic_timestamp ? new Date(first.traffic_timestamp).toLocaleTimeString() : "—"}`
    + (age > STALE_MINUTES ? ` — DATA IS STALE (over ${STALE_MINUTES} min old): check the ingestion cron jobs` : "")
    + ` · page refreshed ${new Date().toLocaleTimeString()}`;
}
refresh();
setInterval(refresh, 5 * 60 * 1000);   // pick up new hourly data without a redeploy
</script></body></html>"""


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE
