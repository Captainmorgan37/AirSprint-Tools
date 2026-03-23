from __future__ import annotations

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from feasibility.engine_phase1 import _collect_planning_note_feedback


def test_planning_note_uses_local_departure_date() -> None:
    # 07:00Z on 2 Jan is 23:00 local on 1 Jan in Los Angeles.
    leg = {
        "departure_icao": "KLAX",
        "arrival_icao": "KSLC",
        "departure_date_utc": "2024-01-02T07:00:00Z",
        "planning_notes": "1JAN24 KLAX-KSLC",
    }

    issues, confirmations = _collect_planning_note_feedback({"legs": [leg]})

    assert not issues
    assert any("2024-01-01" in confirmation for confirmation in confirmations)


def test_continuous_trip_uses_trip_start_date_for_later_legs() -> None:
    legs = [
        {
            "departure_icao": "CYWG",
            "arrival_icao": "CYFB",
            "departure_date_utc": "2026-06-09T20:00:00Z",
            "planning_notes": "09JUN26 CYWG-CYFB-EGGW",
        },
        {
            "departure_icao": "CYFB",
            "arrival_icao": "EGGW",
            "departure_date_utc": "2026-06-10T00:30:00Z",
            "planning_notes": "09JUN26 CYWG-CYFB-EGGW",
        },
    ]

    issues, confirmations = _collect_planning_note_feedback({"legs": legs})

    assert not any("does not match booked 2026-06-10" in issue for issue in issues)
    assert any("Leg 2 CYFB→EGGW" in confirmation for confirmation in confirmations)
