"""Run a job once, or forever at a fixed polling interval.

POLLING INTERVAL: 1 hour (POLL_INTERVAL_SECONDS=3600) - justification
- Luchtmeetnet publishes ONE value per hour (hourly averages; confirmed on Day 1).
  Polling every minute would fetch the same reading ~60 times: 60 duplicate messages
  per hour on the queue (the DB absorbs duplicates via ON CONFLICT, the queue does not -
  every consumer would process each reading 60x). It also wastes the 100 req / 5 min
  fair-use budget.
- NDW: each run downloads ~1.5 MB (config) + ~0.7 MB (measured) compressed and parses
  them in memory. Every minute that is ~130 MB/hour of downloads plus heavy CPU, and
  S3 keys are named per hour, so 59 of 60 uploads would just overwrite the same files.
- Trade-off / known limitation: NDW timestamps are per minute (seen in the queue on
  Day 2, e.g. 00:52:00), so hourly polling stores one 1-minute traffic snapshot per hour,
  not an hourly average - while NO2 IS an hourly average. Fixing this needs aggregation
  of minute data, not faster polling of the snapshot.

RUN_ONCE=1 runs a single time and exits (useful for testing).
"""
import logging
import os
import time

from quality import log_event


def run(job, source):
    interval = int(os.environ.get("POLL_INTERVAL_SECONDS", "3600"))
    if os.environ.get("RUN_ONCE") == "1":
        return job()
    while True:
        try:
            status = job()
            log_event(logging.INFO, "run_finished", source=source,
                      status=status, next_run_in_seconds=interval)
        except Exception as exc:  # one bad run must not kill the long-running service
            log_event(logging.ERROR, "run_crashed", source=source, error=str(exc))
        time.sleep(interval)
