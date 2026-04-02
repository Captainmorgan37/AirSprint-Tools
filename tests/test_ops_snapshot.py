from datetime import UTC, datetime

import requests

from fl3xx_api import Fl3xxApiConfig
import ops_snapshot


class _NoopSession:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_pull_ops_snapshot_retries_roster_timeout(monkeypatch):
    calls = {"count": 0, "timeouts": []}

    def _fake_schedule(*args, **kwargs):
        return []

    def _fake_fetch_staff_roster(config, **kwargs):
        calls["count"] += 1
        calls["timeouts"].append(config.timeout)
        if calls["count"] < 3:
            raise requests.ReadTimeout("timed out")
        return [{"entries": [{"type": "P"}], "flights": []}]

    monkeypatch.setattr(ops_snapshot, "fetch_aircraft_schedule", _fake_schedule)
    monkeypatch.setattr(ops_snapshot, "fetch_staff_roster", _fake_fetch_staff_roster)
    monkeypatch.setattr(ops_snapshot, "assign_roster_to_schedule_rows", lambda rows, roster_rows: rows)
    monkeypatch.setattr(ops_snapshot.requests, "Session", _NoopSession)
    monkeypatch.setattr(ops_snapshot.time, "sleep", lambda _seconds: None)

    config = Fl3xxApiConfig(timeout=30)
    snapshot = ops_snapshot.pull_ops_snapshot(config, lane_targets=[])

    assert snapshot["warnings"] == []
    assert len(snapshot["roster_rows"]) == 1
    assert calls["count"] == 3
    assert calls["timeouts"] == [60, 60, 60]


def test_pull_ops_snapshot_surfaces_roster_failure_after_retries(monkeypatch):
    def _fake_schedule(*args, **kwargs):
        return []

    def _always_timeout(*args, **kwargs):
        raise requests.ReadTimeout("timed out")

    monkeypatch.setattr(ops_snapshot, "fetch_aircraft_schedule", _fake_schedule)
    monkeypatch.setattr(ops_snapshot, "fetch_staff_roster", _always_timeout)
    monkeypatch.setattr(ops_snapshot.requests, "Session", _NoopSession)
    monkeypatch.setattr(ops_snapshot.time, "sleep", lambda _seconds: None)

    config = Fl3xxApiConfig(timeout=30)
    snapshot = ops_snapshot.pull_ops_snapshot(config, lane_targets=[])

    assert len(snapshot["warnings"]) == 1
    warning = snapshot["warnings"][0]
    assert warning.startswith("Roster pull failed:")
    assert "after 3 attempts" in warning
    assert "at least 60s" in warning


def test_roster_pull_config_keeps_higher_timeout():
    config = Fl3xxApiConfig(timeout=90)
    adjusted = ops_snapshot._roster_pull_config(config)

    assert adjusted.timeout == 90


def test_roster_pull_config_raises_low_timeout_to_minimum():
    config = Fl3xxApiConfig(timeout=15)
    adjusted = ops_snapshot._roster_pull_config(config)

    assert adjusted.timeout == 60
    assert adjusted is not config
