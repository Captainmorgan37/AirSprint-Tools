from pathlib import Path
import sys

if str(Path(__file__).resolve().parents[1]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from feasibility.airport_module import AirportProfile, evaluate_suitability


def _default_profile() -> AirportProfile:
    return AirportProfile(
        icao="TEST",
        name="Test Airport",
        longest_runway_ft=10000,
        is_approved_for_ops=True,
        category=None,
        fl3xx_category="A",
        elevation_ft=None,
        country="CA",
    )


def test_operational_closure_keyword_sets_caution() -> None:
    profile = _default_profile()
    leg = {"aircraft_category": "SUPER_MIDSIZE_JET"}
    notes = [{"note": "Closure 2200-0600 for maintenance"}]

    result = evaluate_suitability(
        airport_profile=profile,
        leg=leg,
        operational_notes=notes,
        side="arrival",
    )

    assert result.status == "CAUTION"
    assert result.summary == "Operational closure noted"
    assert result.issues == [
        "Operational notes mention closures; review Fl3xx note for timing before dispatch."
    ]


def test_closed_time_range_sets_caution_instead_of_fail() -> None:
    profile = _default_profile()
    leg = {"aircraft_category": "SUPER_MIDSIZE_JET"}
    notes = [{"note": "AIRPORT CLOSED 2000-0600 LOCAL"}]

    result = evaluate_suitability(
        airport_profile=profile,
        leg=leg,
        operational_notes=notes,
        side="arrival",
    )

    assert result.status == "CAUTION"
    assert result.summary == "Operational closure noted"
    assert result.issues == [
        "Operational notes mention closures; review Fl3xx note for timing before dispatch."
    ]


def test_operational_closed_keyword_still_fails() -> None:
    profile = _default_profile()
    leg = {"aircraft_category": "SUPER_MIDSIZE_JET"}
    notes = [{"note": "Airport closed for GA traffic"}]

    result = evaluate_suitability(
        airport_profile=profile,
        leg=leg,
        operational_notes=notes,
        side="departure",
    )

    assert result.status == "FAIL"
    assert result.summary == "Operational closure in effect"
    assert result.issues == [
        "Operational notes indicate closures or curfews impacting this leg."
    ]


def test_closed_between_hours_does_not_fail() -> None:
    profile = _default_profile()
    leg = {"aircraft_category": "SUPER_MIDSIZE_JET"}
    notes = [
        {
            "note": (
                "DEICE/ANTI-ICE:\n\nExecutive Aviation\n• Type 1 (UCARXL 54) and 4 (UCAR INDULGENCE) available\n"
                "• PH: Site office is 604-302-4929, Supervisor Duncan Lundy cell 778-548-3842\n"
                "• Ice man freq: 129.20 \n• Hours of operation: Closed between 0100-0300. After hours service available w/ 24h notice"
            )
        }
    ]

    result = evaluate_suitability(
        airport_profile=profile,
        leg=leg,
        operational_notes=notes,
        side="arrival",
    )

    assert result.status == "PASS"
    assert result.summary == "Fl3xx category A approved"
    assert result.issues == []


def test_cylw_operational_closure_reminder_is_ignored() -> None:
    profile = AirportProfile(
        icao="CYLW",
        name="Kelowna International",
        longest_runway_ft=10000,
        is_approved_for_ops=True,
        category=None,
        fl3xx_category="A",
        elevation_ft=None,
        country="CA",
    )
    leg = {"aircraft_category": "SUPER_MIDSIZE_JET"}
    notes = [
        {
            "note": (
                "CAUTION - Crews to review NOTAMs prior to every operation. Taxiway and Airport Closures "
                "are common. Crews to contact phone number PRIOR to departure for permission to operate if "
                "required by NOTAM"
            )
        }
    ]

    result = evaluate_suitability(
        airport_profile=profile,
        leg=leg,
        operational_notes=notes,
        side="departure",
    )

    assert result.status == "PASS"
    assert result.summary == "Fl3xx category A approved"
    assert result.issues == []


def test_cylw_operational_closure_reminder_is_ignored_despite_punctuation() -> None:
    profile = AirportProfile(
        icao="CYLW",
        name="Kelowna International",
        longest_runway_ft=10000,
        is_approved_for_ops=True,
        category=None,
        fl3xx_category="A",
        elevation_ft=None,
        country="CA",
    )
    leg = {"aircraft_category": "SUPER_MIDSIZE_JET"}
    notes = [
        {
            "note": (
                "CAUTION-Crews to review NOTAMs prior to every operation; taxiway/airport closures are common—"
                "contact phone number prior to departure if required by NOTAM."
            )
        }
    ]

    result = evaluate_suitability(
        airport_profile=profile,
        leg=leg,
        operational_notes=notes,
        side="arrival",
    )

    assert result.status == "PASS"
    assert result.summary == "Fl3xx category A approved"
    assert result.issues == []


def test_fuel_service_closure_does_not_trigger_fail() -> None:
    profile = _default_profile()
    leg = {"aircraft_category": "SUPER_MIDSIZE_JET"}
    notes = [
        {
            "note": (
                "FUEL:\n\n• Fuel pump service closed at 1630L during winter months. Afterhours callout available with fee"
            )
        }
    ]

    result = evaluate_suitability(
        airport_profile=profile,
        leg=leg,
        operational_notes=notes,
        side="arrival",
    )

    assert result.status == "PASS"
    assert result.summary == "Fl3xx category A approved"
    assert result.issues == []


def test_date_specific_closure_note_ignored_when_not_applicable() -> None:
    profile = _default_profile()
    leg = {
        "aircraft_category": "SUPER_MIDSIZE_JET",
        "arrival_date_utc": "2025-12-12T18:00:00Z",
    }
    notes = [
        {
            "note": "2025 HOLIDAY HOURS \nDEC 24 - WILL NOT ACCEPT FLIGHTS AFTER 1200\nDEC 25 - CLOSED",
        }
    ]

    result = evaluate_suitability(
        airport_profile=profile,
        leg=leg,
        operational_notes=notes,
        side="arrival",
    )

    assert result.status == "PASS"
    assert result.summary == "Fl3xx category A approved"
    assert result.issues == []


def test_customs_closure_note_not_treated_as_operational_closure() -> None:
    profile = _default_profile()
    leg = {"aircraft_category": "SUPER_MIDSIZE_JET"}
    notes = [
        {
            "note": (
                "CUSTOMS: Available - AOE/15. Customs closed on federal holidays; after hours may be requested."
            )
        }
    ]

    result = evaluate_suitability(
        airport_profile=profile,
        leg=leg,
        operational_notes=notes,
        side="arrival",
    )

    assert result.status == "PASS"
    assert result.summary == "Fl3xx category A approved"
    assert result.issues == []
