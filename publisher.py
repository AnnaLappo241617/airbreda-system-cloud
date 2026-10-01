"""Publish readings to the shared Redis list 'readings' (Day 2 message broker).

The broker is an ADDITION to the storage writes, not a replacement: if Redis is down,
the failure is logged and the script carries on. The database and S3 still receive the
data, so no reading is lost - only the queue misses it (see ADR-002).
"""
import json
import logging
import math
import os

import pandas as pd
import redis

from quality import log_event

QUEUE = "readings"


def get_client():
    host = os.environ.get("REDIS_HOST", "redis")  # "redis" = service name in docker-compose.yml
    port = int(os.environ.get("REDIS_PORT", "6379"))
    return redis.Redis(host=host, port=port, socket_connect_timeout=5, socket_timeout=5)


def to_utc_z(timestamp):
    return pd.to_datetime(timestamp, utc=True).strftime("%Y-%m-%dT%H:%M:%SZ")


def clean(value):
    """NaN -> None so it becomes JSON null (json.dumps would write invalid 'NaN')."""
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def publish(messages, source, client=None):
    if not messages:
        return 0
    try:
        client = client or get_client()
        payloads = [json.dumps({k: clean(v) for k, v in m.items()}) for m in messages]
        client.rpush(QUEUE, *payloads)
        log_event(logging.INFO, "queue_publish_success", source=source,
                  queue=QUEUE, messages=len(payloads))
        return len(payloads)
    except redis.RedisError as exc:
        log_event(logging.ERROR, "queue_publish_failed", source=source,
                  queue=QUEUE, error=str(exc))
        return 0
