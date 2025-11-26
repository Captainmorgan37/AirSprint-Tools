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

