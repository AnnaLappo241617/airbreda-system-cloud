import json
import urllib.request

from quality import BadDataTracker, HealthState, start_health_server


def test_health_endpoint_returns_expected_fields():
    tracker = BadDataTracker("NDW")
    tracker.record(); tracker.record(); tracker.record()
    state = HealthState("NDW", tracker)
    state.mark_success()
    server = start_health_server(state, port=0)        # port 0 = any free port
    port = server.server_address[1]
    try:
        body = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/health").read())
    finally:
        server.shutdown()
    assert body["source"] == "NDW"
    assert body["bad_data_count"] == 3
    assert body["last_successful_fetch"].endswith("Z")
