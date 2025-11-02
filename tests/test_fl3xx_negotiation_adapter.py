"""Regression tests for FL3XX negotiation data helpers."""

from __future__ import annotations

from datetime import datetime, timezone

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from integrations.fl3xx_adapter import _initial_tail_snapshot


def test_tail_location_falls_back_to_first_departure_origin():
    window_start = datetime(2024, 5, 1, 8, tzinfo=timezone.utc)

    scheduled_rows = [
        {
            "tail_normalized": "C-GABC",
            "dep_time": "2024-05-01T10:00:00Z",
            "departure_airport": "cyyz",
        }
    ]

    tail_classes = {"C-GABC": "CJ"}

    tails = _initial_tail_snapshot(
        tail_classes,
        {},
        scheduled_rows,
        window_start=window_start,
    )

    assert len(tails) == 1
    tail = tails[0]
    assert tail.last_position_airport == "CYYZ"
    assert tail.last_position_ready_min == 120
    assert tail.available_from_min == 120


def test_tail_location_prefers_recent_arrival_over_fallback():
    window_start = datetime(2024, 5, 1, 8, tzinfo=timezone.utc)

    scheduled_rows = [
        {
            "tail_normalized": "C-GABC",
            "dep_time": "2024-05-01T09:30:00Z",
            "departure_airport": "CYYZ",
        }
    ]

    tail_classes = {"C-GABC": "CJ"}
    tail_positions = {"C-GABC": ("CYUL", 45)}

    tails = _initial_tail_snapshot(
        tail_classes,
        tail_positions,
        scheduled_rows,
        window_start=window_start,
    )

    assert len(tails) == 1
    tail = tails[0]
    assert tail.last_position_airport == "CYUL"
    assert tail.last_position_ready_min == 45
    assert tail.available_from_min == 45
