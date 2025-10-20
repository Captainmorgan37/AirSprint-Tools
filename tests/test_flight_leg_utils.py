"""Tests for helpers in :mod:`flight_leg_utils`."""

from __future__ import annotations

import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from flight_leg_utils import normalize_fl3xx_payload


def test_normalize_infers_arrival_time_from_multiple_sources() -> None:
    payload = {
        "items": [
            {
                "tail": "C-FABC",
                "legs": [
                    {
                        "tail": "C-FABC",
                        "legId": "1",
                        "departureTimeUtc": "2024-07-01T12:00:00Z",
                        "arrivalTime": "2024-07-01T15:00:00Z",
                    },
                    {
                        "tail": "C-FABC",
                        "legId": "2",
                        "departureTimeUtc": "2024-07-02T12:00:00Z",
                        "arrival": {
                            "actualUtc": "2024-07-02T15:30:00Z",
                        },
                    },
                    {
                        "tail": "C-FABC",
                        "legId": "3",
                        "departureTimeUtc": "2024-07-03T12:00:00Z",
                        "times": {
                            "arrival": {
                                "scheduledUtc": "2024-07-03T16:45:00Z",
                            }
                        },
                    },
                ],
            }
        ]
    }

    rows, stats = normalize_fl3xx_payload(payload)

    assert stats["legs_normalized"] == 3
    assert rows[0]["arrival_time"] == "2024-07-01T15:00:00Z"
    assert rows[1]["arrival_time"] == "2024-07-02T15:30:00Z"
    assert rows[2]["arrival_time"] == "2024-07-03T16:45:00Z"
