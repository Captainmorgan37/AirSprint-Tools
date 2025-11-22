import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from feasibility.airport_module import CustomsProfile, evaluate_customs
from feasibility.airport_notes_parser import parse_customs_notes


def _build_leg(arrival_utc: str) -> dict:
    return {
        "departure_icao": "CYEG",
        "arrival_icao": "CYYC",
        "arrival_date_utc": arrival_utc,
        "is_international": True,
    }


def test_customs_within_hours_passes_even_with_contact_requirement() -> None:
    note = (
        "Customs: Customs available per operational notes Daily 0000-2400. "
        "Customs contact required per notes. AOE 24/7"
    )
    parsed = parse_customs_notes([note])

    result = evaluate_customs(
        CustomsProfile(icao="CYYC", service_type="AOE", notes=None),
        _build_leg("2024-01-01T12:00:00Z"),
        "ARR",
        [],
        parsed_customs=parsed,
        tz_name="America/Edmonton",
    )

    assert result.status == "PASS"
    assert any("Customs contact required" in issue for issue in result.issues)


def test_customs_afterhours_available_is_caution_when_outside_hours() -> None:
    note = "Customs hours 0800-1700 Daily. After hours available with notice."
    parsed = parse_customs_notes([note])

    result = evaluate_customs(
        CustomsProfile(icao="CYYC", service_type="AOE", notes=None),
        _build_leg("2024-01-01T09:00:00Z"),
        "ARR",
        [],
        parsed_customs=parsed,
        tz_name="America/Edmonton",
    )

    assert result.status == "CAUTION"
    assert any("outside customs hours" in issue for issue in result.issues)


def test_customs_fails_when_outside_hours_and_no_afterhours_support() -> None:
    note = "Customs hours 0800-1700 Mon-Fri. After hours not available."
    parsed = parse_customs_notes([note])

    # 02:00 local on a Saturday (2024-01-06)
    arrival_dt = datetime(2024, 1, 6, 9, 0, tzinfo=timezone.utc)
    arrival_utc = arrival_dt.isoformat().replace("+00:00", "Z")

    result = evaluate_customs(
        CustomsProfile(icao="CYYC", service_type="AOE", notes=None),
        _build_leg(arrival_utc),
        "ARR",
        [],
        parsed_customs=parsed,
        tz_name="America/Edmonton",
    )

    assert result.status == "FAIL"
    assert any("outside customs hours" in issue for issue in result.issues)
