"""Create / migrate the sensor_readings table. Safe to run more than once."""
from db import get_connection

STATEMENTS = [
    # Day 1
    """
    CREATE TABLE IF NOT EXISTS sensor_readings (
        station_id  VARCHAR(20)   NOT NULL,
        timestamp   TIMESTAMPTZ   NOT NULL,
        component   VARCHAR(10)   NOT NULL,
        value       FLOAT,
        PRIMARY KEY (station_id, timestamp, component)
    );
    """,
    # Day 2 Lab 2: flag stale/null air readings instead of dropping them
    "ALTER TABLE sensor_readings ADD COLUMN IF NOT EXISTS is_flagged BOOLEAN DEFAULT FALSE;",
    # Day 2 Lab 2: NDW site IDs (e.g. RWS01_MONIBAS_0271hrl0063ra = 27 chars) don't fit in 20
    "ALTER TABLE sensor_readings ALTER COLUMN station_id TYPE VARCHAR(40);",
]

if __name__ == "__main__":
    with get_connection() as conn, conn.cursor() as cur:
        for sql in STATEMENTS:
            cur.execute(sql)
    print("Table sensor_readings is ready (with is_flagged, station_id VARCHAR(40)).")
