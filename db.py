"""Shared database access for AirBreda.

Credentials come from environment variables (loaded from a local .env file).
Never put the password in code or commit .env to Git.
"""
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

INSERT_SQL = """
    INSERT INTO sensor_readings (station_id, timestamp, component, value, is_flagged)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (station_id, timestamp, component) DO NOTHING
"""


def db_configured():
    return bool(os.environ.get("DB_HOST"))


def get_connection():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ.get("DB_NAME", "postgres"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        sslmode="require",
        connect_timeout=10,
    )


def save_rows(conn, rows):
    """rows: iterable of (station_id, timestamp, component, value, is_flagged).

    Idempotent (ON CONFLICT DO NOTHING). Returns how many rows were new.
    """
    inserted = 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(INSERT_SQL, row)
            inserted += cur.rowcount
    conn.commit()
    return inserted
