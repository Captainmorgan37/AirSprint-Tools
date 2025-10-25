from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pytest

import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import taf_utils  # noqa: E402  pylint: disable=wrong-import-position


class DummyResponse:
    def __init__(self, payload: Dict[str, Any]):
        self._payload = payload
        self.status_code = 200

    def raise_for_status(self) -> None:  # pragma: no cover - interface compatibility
        return None

    def json(self) -> Dict[str, Any]:
        return self._payload


def _build_forecast_segment(start: str, end: str, **extra: Any) -> Dict[str, Any]:
    segment = {
        "fcstTimeFrom": start,
        "fcstTimeTo": end,
    }
    segment.update(extra)
    return segment


def test_iter_forecast_candidates_handles_json_string():
    segment = _build_forecast_segment("2024-10-24T11:00:00Z", "2024-10-24T17:00:00Z", windSpeed=12)
    json_blob = json.dumps([segment])

    candidates = list(taf_utils._iter_forecast_candidates(json_blob))

    assert len(candidates) == 1
    assert candidates[0]["fcstTimeFrom"] == segment["fcstTimeFrom"]


def test_get_taf_reports_normalises_nested_structures(monkeypatch: pytest.MonkeyPatch):
    payload = {
        "features": [
            {
                "properties": {
                    "station": "CYYZ",
                    "issueTime": "2024-10-24T09:00:00Z",
                    "validTimeFrom": "2024-10-24T09:00:00Z",
                    "validTimeTo": "2024-10-25T09:00:00Z",
                    "rawTAF": "TAF CYYZ 240900Z 2409/2515 20012KT P6SM BKN020",
                    "forecast": {
                        "periods": [
                            _build_forecast_segment(
                                "2024-10-24T09:00:00Z",
                                "2024-10-24T15:00:00Z",
                                windDir=200,
                                windSpeed=12,
                                visibilitySM=6,
                            ),
                            json.dumps(
                                _build_forecast_segment(
                                    "2024-10-24T15:00:00Z",
                                    "2024-10-24T21:00:00Z",
                                    windDir=210,
                                    windSpeed=18,
                                    wxString="-RA",
                                )
                            ),
                        ]
                    },
                }
            }
        ]
    }

    def fake_get(url: str, params: Dict[str, Any], timeout: int) -> DummyResponse:
        assert "CYYZ" in params["ids"].upper()
        return DummyResponse(payload)

    monkeypatch.setattr(taf_utils.requests, "get", fake_get)

    reports = taf_utils.get_taf_reports(["cyyz"])

    assert "CYYZ" in reports
    station_reports = reports["CYYZ"]
    assert len(station_reports) == 1

    forecast_periods = station_reports[0]["forecast"]
    assert len(forecast_periods) == 2

    first_period = forecast_periods[0]
    assert first_period["from_time"] == datetime(2024, 10, 24, 9, 0, tzinfo=timezone.utc)
    assert first_period["to_time"] == datetime(2024, 10, 24, 15, 0, tzinfo=timezone.utc)

    second_period = forecast_periods[1]
    assert second_period["details"]
    weather_entries = dict(second_period["details"])
    assert weather_entries.get("Weather") == "-RA"

