"""Tests for airport operational notes parsing and categorization."""

from pathlib import Path
import sys

if str(Path(__file__).resolve().parents[1]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from feasibility.airport_notes_parser import (
    parse_customs_notes,
    parse_operational_restrictions,
    summarize_operational_notes,
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

    assert parsed["customs_contact_required"] is True
    assert parsed["customs_contact_notes"] == []
    assert parsed["general_customs_notes"] == [note]


def test_canpass_note_only_in_canpass_section() -> None:
    note = "Customs available via CANPASS only. Call 555-555-5555 with 24 hours notice."

    parsed = parse_customs_notes([note])

    assert parsed["canpass_notes"] == [note]
    assert parsed["general_customs_notes"] == []


def test_operational_callouts_do_not_trigger_customs_contact_requirement() -> None:
    note = (
        "Primary Location: AirSprint Hangar. Secondary location: SkyService FBO."  # operational instruction
        " Call on the radio to SkyService with a heads up that you will be parking"
        " on their ramp and they will direct you to a gate."
    )

    parsed = parse_customs_notes([note])

    assert parsed["customs_contact_required"] is False
    assert parsed["customs_contact_notes"] == []
    assert note in parsed["location_notes"] or note in parsed["general_customs_notes"]


def test_customs_contact_number_not_treated_as_contact_requirement() -> None:
    note = (
        "CUSTOMS:\n"
        "AOE 24/7.\n"
        "Third Location: Small Aircraft Centre 'Customs Shack'. Contact # 403.477.5422. "
        "Commercial customs CBSA # 403.461.7564"
    )

    parsed = parse_customs_notes([note])

    assert parsed["customs_contact_required"] is False
    assert parsed["customs_contact_notes"] == []
    assert note in parsed["raw_notes"]


def test_deice_limited_not_triggered_by_holdover_language() -> None:
    note = (
        "DEICE/ANTI ICE: If temperature is below -14, better hold over times exist "
        "using the CDF fluid (EG106) due to very limited hold over times in the hangar."
    )

    parsed = parse_operational_restrictions([note])

    assert parsed["deice_limited"] is False
    assert parsed["deice_notes"] == [note]


def test_day_operations_only_blocks_night_ops() -> None:
    note = "Day Operations Only - NO RWY LIGHTS"

    parsed = parse_operational_restrictions([note])

    assert parsed["night_ops_allowed"] is False
    assert parsed["hour_notes"] == [note]


def test_weather_limitation_included_in_summary() -> None:
    note = "Good Weather Only (VFR weather - no night operations)"

    parsed = parse_operational_restrictions([note])

    assert parsed["weather_limitations"] == [note]

    summary = summarize_operational_notes("MYAM", [{"note": note}], parsed)

    assert summary.status == "CAUTION"
    assert any("Weather limitation" in issue for issue in summary.issues)


def test_non_restrictive_generic_notes_do_not_trigger_caution() -> None:
    notes = [
        {"note": "1 hr turn time required"},
        {"note": "NOTES: Closest airport with minimal or no restrictions KCHA 25nm NW (45 min drive)"},
    ]

    summary = summarize_operational_notes("KCHA", notes)

    assert summary.status == "INFO"
    assert summary.summary.startswith("Operational notes available")
    assert all(issue.startswith("Operational note:") for issue in summary.issues)
