from __future__ import annotations

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from feasibility.planning_notes import (
    extract_requested_aircraft_from_note,
    find_route_mismatch,
    normalize_planning_note_text,
)


def test_extract_requested_aircraft_skips_words_after_requesting() -> None:
    note = "24Club CJ3 owner requesting interchange to EMB"

    assert extract_requested_aircraft_from_note(note) == "EMB"


def test_extract_requested_aircraft_ignores_non_aircraft_tokens() -> None:
    note = "Owner requested assistance with catering details"

    assert extract_requested_aircraft_from_note(note) is None


def test_extract_requested_aircraft_from_escaped_newlines() -> None:
    note = "03MAY KSDL - KLWT \\n-\\n8hr Infinity EMB owner requesting CJ Fleet \\n-\\nreference #5001"

    assert extract_requested_aircraft_from_note(note) == "CJ"


def test_normalize_planning_note_text_decodes_literal_newlines() -> None:
    note = "line1\\nline2\r\nline3"

    assert normalize_planning_note_text(note) == "line1\nline2\nline3"


def test_find_route_mismatch_accepts_dash_after_date_token() -> None:
    note = "21APR CYYZ-KMCO \n24APR- KMCO-MYNN\n27APR- MYNN-CYYZ"

    assert find_route_mismatch("KMCO", "MYNN", "2026-04-24T15:00:00Z", note) is None
