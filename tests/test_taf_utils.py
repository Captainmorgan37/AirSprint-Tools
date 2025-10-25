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


def _build_modern_segment(start: str, end: str, *, gust: int | None = None) -> Dict[str, Any]:
    segment: Dict[str, Any] = {
        "startTime": start,
        "endTime": end,
        "wind": {
            "direction": {"value": 170},
            "speed": {"value": 18},
        },
        "visibility": {"repr": "P6SM"},
        "clouds": [
            {
                "amount": "FEW",
                "base": {"value": 24000},
            }
        ],
    }
    if gust is not None:
        segment["wind"]["gust"] = {"value": gust}
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


def test_get_taf_reports_extracts_modern_forecast_format(monkeypatch: pytest.MonkeyPatch):
    payload = {
        "features": [
            {
                "properties": {
                    "station": "CYWG",
                    "issueTime": "2025-10-25T11:40:00Z",
                    "validTimeFrom": "2025-10-25T12:00:00Z",
                    "validTimeTo": "2025-10-26T12:00:00Z",
                    "rawTAF": "TAF CYWG 251140Z 2512/2612 16012KT P6SM FEW240 BECMG 2515/2517 17018G28KT",
                    "forecast": [
                        _build_modern_segment("2025-10-25T12:00:00Z", "2025-10-25T15:00:00Z"),
                        {
                            "change": {
                                "indicator": "BECMG",
                                "time": {
                                    "from": "2025-10-25T15:00:00Z",
                                    "to": "2025-10-25T17:00:00Z",
                                },
                            },
                            "wind": {
                                "direction": {"value": 170},
                                "speed": {"value": 18},
                                "gust": {"value": 28},
                            },
                            "visibility": {"repr": "P6SM"},
                            "clouds": [
                                {
                                    "amount": "FEW",
                                    "base": {"value": 24000},
                                }
                            ],
                        },
                        _build_modern_segment("2025-10-25T17:00:00Z", "2025-10-25T22:00:00Z", gust=28),
                    ],
                }
            }
        ]
    }

    def fake_get(url: str, params: Dict[str, Any], timeout: int) -> DummyResponse:
        assert params["format"] == "json"
        return DummyResponse(payload)

    monkeypatch.setattr(taf_utils.requests, "get", fake_get)

    reports = taf_utils.get_taf_reports(["CYWG"])

    station_reports = reports["CYWG"]
    assert station_reports
    forecast_periods = station_reports[0]["forecast"]
    assert len(forecast_periods) == 3

    third_period = forecast_periods[2]
    assert third_period["from_time"] == datetime(2025, 10, 25, 17, 0, tzinfo=timezone.utc)
    assert third_period["to_time"] == datetime(2025, 10, 25, 22, 0, tzinfo=timezone.utc)

    detail_map = dict(third_period["details"])
    assert detail_map["Wind Dir (°)"] == 170
    assert detail_map["Wind Speed (kt)"] == 18
    assert detail_map["Wind Gust (kt)"] == 28
    assert detail_map["Visibility"] == "P6SM"

    cloud_detail = next((value for label, value in third_period["details"] if label == "Clouds"), None)
    assert cloud_detail == "FEW 24000ft"

