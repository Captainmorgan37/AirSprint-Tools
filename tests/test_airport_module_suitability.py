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
