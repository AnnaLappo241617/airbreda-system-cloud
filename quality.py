"""Shared observability helpers for AirBreda (Day 2 Lab 2).

- log_event():      one structured JSON log line to stdout per event
- BadDataTracker:   in-memory bad-data counter per source + "more than 10 in an hour" alert
- HealthState / start_health_server(): the /health endpoint of each ingestion service
"""
import json
import logging
import sys
import threading
from collections import deque
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

logger = logging.getLogger("airbreda")


def setup_logging(level=logging.INFO):
    """Every log line is exactly one JSON object -> easy to query in a dashboard later."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)


def utc_now():
    return datetime.now(timezone.utc)


def iso_z(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log_event(level, event, **fields):
    """Log {"level": ..., "logged_at": ..., "event": ..., **fields} as one JSON line."""
    record = {"level": logging.getLevelName(level), "logged_at": iso_z(utc_now()), "event": event}
    record.update(fields)
    logger.log(level, json.dumps(record, default=str))


class BadDataTracker:
    """Counts DATA_QUALITY_ERROR events for one source.

    bad_data_count  - total since the service started (shown on /health)
    threshold check - if MORE than `threshold` bad events happen within `window`,
                      log ONE BAD_DATA_THRESHOLD_EXCEEDED error (not one per event).
                      The alert re-arms once the rolling count drops back under the threshold.
    Note: in-memory only - the count resets when the container restarts.
    """

    def __init__(self, source, threshold=10, window=timedelta(hours=1), clock=utc_now):
        self.source = source
        self.threshold = threshold
        self.window = window
        self.clock = clock
        self.bad_data_count = 0
        self._recent = deque()
        self._alerted = False

    def record(self):
        now = self.clock()
        self.bad_data_count += 1
        self._recent.append(now)
        while self._recent and now - self._recent[0] > self.window:
            self._recent.popleft()
        if len(self._recent) > self.threshold:
            if not self._alerted:
                log_event(logging.ERROR, "BAD_DATA_THRESHOLD_EXCEEDED",
                          source=self.source, count=len(self._recent))
                self._alerted = True
        else:
            self._alerted = False


class HealthState:
    def __init__(self, source, tracker):
        self.source = source
        self.tracker = tracker
        self.last_successful_fetch = None  # UTC time of the last successful API/feed fetch

    def mark_success(self):
        self.last_successful_fetch = iso_z(utc_now())

    def as_dict(self):
        return {"last_successful_fetch": self.last_successful_fetch,
                "bad_data_count": self.tracker.bad_data_count,
                "source": self.source}


def start_health_server(state, port=8000):
    """Serve GET /health in a background thread (the main thread keeps polling)."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.rstrip("/") == "/health":
                body = json.dumps(state.as_dict()).encode()
                self.send_response(200)
            else:
                body = b'{"error": "not found"}'
                self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # silence default plain-text access logs
            pass

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log_event(logging.INFO, "health_server_started", source=state.source, port=port)
    return server
