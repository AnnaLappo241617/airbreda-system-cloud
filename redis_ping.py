"""Connectivity check: `docker exec air-ingest python redis_ping.py` -> PONG.
(The lab's `redis-cli` command doesn't exist in our Python images.)"""
from publisher import get_client

print("PONG" if get_client().ping() else "no reply")
