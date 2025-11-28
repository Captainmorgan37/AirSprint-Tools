from __future__ import annotations

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from feasibility.planning_notes import extract_requested_aircraft_from_note


def test_extract_requested_aircraft_skips_words_after_requesting() -> None:
    note = "24Club CJ3 owner requesting interchange to EMB"

    assert extract_requested_aircraft_from_note(note) == "EMB"


def test_extract_requested_aircraft_ignores_non_aircraft_tokens() -> None:
    note = "Owner requested assistance with catering details"

    assert extract_requested_aircraft_from_note(note) is None
