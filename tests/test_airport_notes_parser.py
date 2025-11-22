"""Tests for airport operational notes parsing and categorization."""

from pathlib import Path
import sys

if str(Path(__file__).resolve().parents[1]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from feasibility.airport_notes_parser import parse_operational_restrictions


def test_operational_note_assigned_to_single_category() -> None:
    note = (
        "DEICE/ANTI-ICE: Type IV available at FBO during winter snow events"
    )

    parsed = parse_operational_restrictions([note])

    assert parsed["deice_notes"] == [note]
    assert parsed["winter_sensitivity"] is True
    assert parsed["winter_notes"] == []


def test_primary_category_prevents_duplicate_notes() -> None:
    note = "Slot and PPR required for winter operations"

    parsed = parse_operational_restrictions([note])

    assert parsed["slot_notes"] == [note]
    assert parsed["ppr_required"] is True
    assert parsed["ppr_notes"] == []
