from __future__ import annotations

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from feasibility.planning_notes import (
    extract_requested_aircraft_from_note,
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
