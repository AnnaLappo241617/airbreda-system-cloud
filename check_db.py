"""Show how many rows are in sensor_readings and the newest ones."""
from db import get_connection

if __name__ == "__main__":
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM sensor_readings")
        print("Total rows:", cur.fetchone()[0])
        cur.execute("SELECT * FROM sensor_readings ORDER BY timestamp DESC LIMIT 5")
        for row in cur.fetchall():
            print(row)
