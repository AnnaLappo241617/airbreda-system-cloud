"""
e2e_check.py - prove the AirBreda pipeline works end to end, with evidence.

Traces the CURRENT reading through every stage and checks that each stage agrees with the
previous one:

  NO2:     Luchtmeetnet API -> air.log (cron run) -> sensor_readings -> /site/{id} -> dashboard
  Traffic: NDW feed -> traffic.log (cron run) -> S3 CSV -> sensor_readings FLOW rows -> /site/{id}
  Model:   /site/{id} prediction == predict() recomputed with the model in this image
  Ops:     /health reports both sources with fresh timestamps; the dashboard page is served

Run it ON THE VM, inside the dashboard image (it already has boto3, psycopg2, the model):

  docker run --rm --network host --env-file .env \
    -v /home/ec2-user/airbreda/logs:/logs:ro \
    -v /home/ec2-user/airbreda/e2e_check.py:/app/e2e_check.py:ro \
    airbreda-dashboard python e2e_check.py

Exit code 0 = every check passed.
"""
import csv
import io
import json
import os
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

import predict
from db import get_connection

DASHBOARD = os.environ.get("DASHBOARD_URL", "http://localhost:8000")
BUCKET = os.environ["S3_BUCKET"]
LOG_DIR = Path(os.environ.get("LOG_DIR", "/logs"))
SITES = {"hrl": "RWS01_MONIBAS_0271hrl0063ra", "hrr": "RWS01_MONIBAS_0271hrr0063ra",
         "vwd": "RWS01_MONIBAS_0270vwd0063ra", "vwa": "RWS01_MONIBAS_0270vwa0063ra"}
results = []


def check(stage, name, ok, evidence):
    results.append((stage, name, bool(ok), evidence))


def get_json(url, timeout=15):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode())


def utc(ts):
    return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(timezone.utc)


def last_event(logfile, event):
    path = LOG_DIR / logfile
    if not path.exists():
        return None
    last = None
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.startswith("{"):
            try:
                ev = json.loads(line)
            except ValueError:
                continue
            if ev.get("event") == event:
                last = ev
    return last


def main():
    now = datetime.now(timezone.utc)
    print(f"AirBreda end-to-end check - {now:%Y-%m-%d %H:%M:%S} UTC\n")

    # ---------- 1. Source: Luchtmeetnet API --------------------------------------------
    src = get_json("https://api.luchtmeetnet.nl/open_api/stations/NL10240/measurements"
                   "?formula=NO2&order_by=timestamp_measured&order_direction=desc&page=1")["data"][0]
    src_ts, src_val = utc(src["timestamp_measured"]), src["value"]
    check("1 source", "Luchtmeetnet API answers for NL10240", True, f"newest NO2 {src_val} at {src_ts:%H:%M}Z")

    # ---------- 2. Ingestion ran (cron logs) ---------------------------------------------
    for logfile, label in (("air.log", "air"), ("traffic.log", "traffic")):
        ev = last_event(logfile, "fetch_success")
        age = (now - utc(ev["logged_at"])) if ev else None
        check("2 ingestion", f"{label} job ran recently (cron)", ev and age < timedelta(hours=2),
              f"last fetch_success {ev['logged_at']} ({int(age.total_seconds() // 60)} min ago)" if ev
              else f"no fetch_success in {LOG_DIR / logfile}")

    # ---------- 3. Database holds the source value ----------------------------------------
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""SELECT timestamp, value, is_flagged FROM sensor_readings
                       WHERE station_id='NL10240' AND component='NO2' ORDER BY timestamp DESC LIMIT 1""")
        db_ts, db_val, db_flag = cur.fetchone()
        cur.execute("SELECT value FROM sensor_readings WHERE station_id='NL10240' AND component='NO2' "
                    "AND timestamp=%s", (src_ts,))
        same_hour = cur.fetchone()
    check("3 database", "API reading is stored in sensor_readings",
          same_hour is not None and abs(same_hour[0] - src_val) < 1e-6 if src_val is not None else same_hour is not None,
          f"DB value for {src_ts:%H:%M}Z: {same_hour[0] if same_hour else 'missing (next :10 run will add it)'}")

    # ---------- 4. S3 holds the newest complete traffic hour ------------------------------
    s3 = boto3.client("s3")
    traffic = None
    for back in range(0, 7):
        hour = (now - timedelta(hours=back)).replace(minute=0, second=0, microsecond=0)
        rows = {}
        for code in SITES:
            key = f"ndw/{hour:%Y-%m-%d}/{hour:%H}-{code}.csv"
            try:
                body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode()
                rows[code] = next(csv.DictReader(io.StringIO(body)))
            except Exception:
                break
        if len(rows) == 4:
            traffic = (hour, rows)
            break
    check("4 object storage", "4 site CSVs for the newest traffic hour in S3", traffic is not None,
          f"ndw/{traffic[0]:%Y-%m-%d}/{traffic[0]:%H}-*.csv, snapshot {traffic[1]['hrl']['timestamp']}"
          if traffic else "no complete hour in the last 6 h")

    # ---------- 5. DB FLOW rows match the S3 CSVs ------------------------------------------
    if traffic:
        hour, rows = traffic
        mismatches = []
        with get_connection() as conn, conn.cursor() as cur:
            for code, row in rows.items():
                cur.execute("SELECT value FROM sensor_readings WHERE station_id=%s AND component='FLOW' "
                            "AND timestamp=%s", (SITES[code], utc(row["timestamp"])))
                got = cur.fetchone()
                if not got or abs(got[0] - float(row["total_flow_veh_per_hour"])) > 1e-6:
                    mismatches.append(code)
        check("5 database", "FLOW rows in DB equal the S3 CSV values", not mismatches,
              "all 4 sites match" if not mismatches else f"mismatch/missing: {mismatches}")

    # ---------- 6. API returns the stored values + the model's prediction ------------------
    api = {code: get_json(f"{DASHBOARD}/site/{code}") for code in SITES}
    a = api["hrl"]
    check("6 API", "/site/{id} NO2 equals the newest DB row",
          a["no2_ug_m3"] == db_val and utc(a["timestamp"]) == db_ts.astimezone(timezone.utc),
          f"API {a['no2_ug_m3']} @ {a['timestamp']} vs DB {db_val} @ {db_ts:%H:%M}Z{' (flagged)' if db_flag else ''}")
    if traffic:
        bad = [c for c in SITES if api[c]["intensity_veh_per_hr"] != float(rows[c]["total_flow_veh_per_hour"])]
        check("6 API", "/site/{id} intensities equal the S3 CSVs", not bad,
              "all 4 sites match" if not bad else f"differ: {bad} (API cache is 60 s - rerun if a new hour just landed)")
    total = sum(api[c]["intensity_veh_per_hr"] or 0 for c in SITES)
    check("6 API", "total_intensity = sum of the 4 sites", abs(total - a["total_intensity_veh_per_hr"]) < 1e-6,
          f"sum {total:.0f} vs total {a['total_intensity_veh_per_hr']:.0f}")

    # ---------- 7. The prediction really comes from the trained model ----------------------
    meta = predict.model_info()
    hour_of_day = utc(a["traffic_timestamp"]).hour
    mine = predict.predict(a["total_intensity_veh_per_hr"], hour_of_day)
    check("7 model", "API prediction == predict() with the model in the image",
          abs(mine["no2_ug_m3_predicted"] - a["no2_ug_m3_predicted"]) < 1e-6
          and abs(mine["no2_exceedance_risk"] - a["no2_exceedance_risk"]) < 1e-6,
          f"{a['no2_ug_m3_predicted']} µg/m³, risk {a['no2_exceedance_risk']} "
          f"(model trained {meta.get('trained_at')} on {meta.get('rows')} rows)")
    check("7 model", "exceedance risk is a float in 0..1", 0 <= a["no2_exceedance_risk"] <= 1,
          f"{a['no2_exceedance_risk']}")

    # ---------- 8. Health + dashboard page -------------------------------------------------
    h = get_json(f"{DASHBOARD}/health")
    check("8 ops", "/health status and both sources present",
          h.get("status") == "ok" and h.get("luchtmeetnet", {}).get("last_successful_fetch")
          and h.get("ndw", {}).get("last_successful_fetch"),
          f"status={h.get('status')}, air={h['luchtmeetnet']['last_successful_fetch']}, "
          f"ndw={h['ndw']['last_successful_fetch']}, bad={h['luchtmeetnet']['bad_data_count']}/{h['ndw']['bad_data_count']}")
    with urllib.request.urlopen(f"{DASHBOARD}/", timeout=15) as r:
        page = r.read().decode()
    check("8 ops", "dashboard page is served and built on /site/{id}",
          r.status == 200 and "/site/" in page, f"HTTP {r.status}, {len(page)} bytes")

    # ---------- report -------------------------------------------------------------------
    width = max(len(n) for _, n, _, _ in results)
    for stage, name, ok, evidence in results:
        print(f"{'PASS' if ok else 'FAIL'}  {stage:<16} {name:<{width}}  | {evidence}")
    failed = [r for r in results if not r[2]]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
