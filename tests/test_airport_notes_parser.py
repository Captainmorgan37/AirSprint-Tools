"""Tests for airport operational notes parsing and categorization."""

from pathlib import Path
import sys

if str(Path(__file__).resolve().parents[1]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from feasibility.airport_notes_parser import (
    parse_customs_notes,
    parse_operational_restrictions,
)


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


def test_customs_note_assigned_to_single_section() -> None:
    note = (
        "CUSTOMS: Available with 24hr notice. Hrs: 0800-1700 Mon-Fri. Call CBP to request."
    )

    parsed = parse_customs_notes([note])

    assert parsed["customs_contact_notes"] == []
    assert parsed["general_customs_notes"] == [note]


def test_canpass_note_only_in_canpass_section() -> None:
    note = "Customs available via CANPASS only. Call 555-555-5555 with 24 hours notice."

    parsed = parse_customs_notes([note])

    assert parsed["canpass_notes"] == [note]
    assert parsed["general_customs_notes"] == []
