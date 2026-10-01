import json

import pandas as pd
import redis

import ingest_air
import publisher


class FakeRedis:
    def __init__(self, fail=False):
        self.lists, self.fail = {}, fail

    def rpush(self, key, *values):
        if self.fail:
            raise redis.ConnectionError("simulated: broker down")
        self.lists.setdefault(key, []).extend(values)


def test_air_message_matches_lab_format():
    df = pd.DataFrame({"station_id": ["NL10240"], "timestamp": ["2024-01-15T08:00:00+00:00"],
                       "component": ["NO2"], "value": [18.4]})
    fake = FakeRedis()
    assert publisher.publish(ingest_air.build_messages(df), "Luchtmeetnet", client=fake) == 1
    assert json.loads(fake.lists["readings"][0]) == {
        "station_id": "NL10240", "timestamp": "2024-01-15T08:00:00Z",
        "component": "NO2", "value": 18.4}


def test_null_value_becomes_json_null():
    df = pd.DataFrame({"station_id": ["NL10240"], "timestamp": ["2024-01-15T09:00:00Z"],
                       "component": ["NO2"], "value": [None]})
    fake = FakeRedis()
    publisher.publish(ingest_air.build_messages(df), "Luchtmeetnet", client=fake)
    assert json.loads(fake.lists["readings"][0])["value"] is None


def test_broker_down_is_not_fatal():
    msgs = [{"station_id": "NL10240", "timestamp": "2024-01-15T08:00:00Z",
             "component": "NO2", "value": 18.4}]
    assert publisher.publish(msgs, "Luchtmeetnet", client=FakeRedis(fail=True)) == 0
