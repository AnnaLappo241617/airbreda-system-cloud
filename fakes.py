"""Fake database objects so tests never need a real PostgreSQL."""


class FakeCursor:
    def __init__(self, table):
        self.table, self.rowcount = table, 0

    def execute(self, sql, params):
        station_id, timestamp, component, value, is_flagged = params
        key = (station_id, timestamp, component)
        if key in self.table:            # mimic ON CONFLICT DO NOTHING
            self.rowcount = 0
        else:
            self.table[key] = {"value": value, "is_flagged": is_flagged}
            self.rowcount = 1

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakeConn:
    def __init__(self):
        self.table = {}

    def cursor(self):
        return FakeCursor(self.table)

    def commit(self):
        pass
