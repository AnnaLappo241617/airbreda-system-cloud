"""
Fetch the latest NO2 reading for RIVM Luchtmeetnet station NL10240
(Breda-Tilburgseweg) using the official open API.

API docs: https://api-docs.luchtmeetnet.nl/
No API key required. Fair use limit: 100 requests / 5 minutes.
"""

import requests

STATION = "NL10240"
FORMULA = "NO2"

BASE_URL = f"https://api.luchtmeetnet.nl/open_api/stations/{STATION}/measurements"


def get_latest_no2(station: str = STATION, formula: str = FORMULA) -> dict:
    """Return the most recent measurement for the given station/formula."""
    params = {
        "formula": formula,
        "order_by": "timestamp_measured",
        "order_direction": "desc",
        "page": 1,
    }
    url = f"https://api.luchtmeetnet.nl/open_api/stations/{station}/measurements"

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()

    measurements = data.get("data", [])
    if not measurements:
        raise ValueError(f"No measurements found for station {station} ({formula})")

    return measurements[0]


if __name__ == "__main__":
    latest = get_latest_no2()
    print(f"Station:    {STATION}")
    print(f"Component:  {latest['formula']}")
    print(f"Value:      {latest['value']} µg/m³")
    print(f"Timestamp:  {latest['timestamp_measured']}")

    # --- Lab 1.4 exploration ---
    print("\nQ1 full record:", latest)

    r = requests.get(BASE_URL, params={"formula": "NO2", "order_by": "timestamp_measured",
                                       "order_direction": "desc", "page": 1}, timeout=10)
    print("Q2 last 5 timestamps:", [m["timestamp_measured"] for m in r.json().get("data", [])[:5]])

    for f in ["SO2", "XYZ"]:
        try:
            print(f"Q3 formula={f}:", get_latest_no2(formula=f))
        except Exception as e:
            print(f"Q3 formula={f}: {type(e).__name__}: {e}")