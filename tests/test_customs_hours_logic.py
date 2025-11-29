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


def test_aoe_arrival_outside_hours_requires_canpass() -> None:
    note = "Canada Customs: AOE 0800-1700 Daily. CANPASS required after hours."
    parsed = parse_customs_notes([note])

    result = evaluate_customs(
        CustomsProfile(icao="CYEG", service_type="AOE", notes=None),
        _build_leg("2024-01-06T05:00:00Z"),
        "ARR",
        [],
        parsed_customs=parsed,
        tz_name="America/Edmonton",
    )

    assert result.status == "FAIL"
    assert any("AOE clearance requires all passengers" in issue for issue in result.issues)


def test_aoe15_triggers_caution_notice() -> None:
    note = "Customs hours 0800-1700 Daily. Airport is AOE/15."
    parsed = parse_customs_notes([note])

    result = evaluate_customs(
        CustomsProfile(icao="CYEG", service_type="AOE", notes=None),
        _build_leg("2024-01-02T15:00:00Z"),
        "ARR",
        [],
        parsed_customs=parsed,
        tz_name="America/Edmonton",
    )

    assert result.status == "PASS"
    assert all("AOE/15" not in issue for issue in result.issues)


def test_aoe_canpass_requires_canpass_for_clearance() -> None:
    note = "Customs: AOE/CANPASS only."
    parsed = parse_customs_notes([note])

    result = evaluate_customs(
        CustomsProfile(icao="CYEG", service_type="AOE", notes=None),
        _build_leg("2024-01-02T15:00:00Z"),
        "ARR",
        [],
        parsed_customs=parsed,
        tz_name="America/Edmonton",
    )

    assert result.status == "FAIL"
    assert "AOE/CANPASS" in result.summary
    assert any("all passengers must hold CANPASS" in issue for issue in result.issues)


def test_aoe15_outside_hours_triggers_caution() -> None:
    note = "Customs hours 0800-1700 Daily. Airport is AOE/15."
    parsed = parse_customs_notes([note])

    result = evaluate_customs(
        CustomsProfile(icao="CYEG", service_type="AOE", notes=None),
        _build_leg("2024-01-02T02:00:00Z"),
        "ARR",
        [],
        parsed_customs=parsed,
        tz_name="America/Edmonton",
    )

    assert result.status == "CAUTION"
    assert any("AOE/15" in issue for issue in result.issues)


def test_aoe_with_canpass_notes_passes_within_hours() -> None:
    note = (
        "CUSTOMS - AOE\n\n"
        "Location: Execaire FBO\n"
        "Hours of Operation: 24/7\n"
        "Phone: 1-888-226-7277 / 514-633-7752\n"
        "Fax: 905-679-3300\n\n"
        "Proceed with standard CANPASS arrival set up process.\n"
    )
    parsed = parse_customs_notes([note])

    result = evaluate_customs(
        CustomsProfile(icao="CYUL", service_type="AOE", notes=None),
        _build_leg("2024-01-02T15:00:00Z"),
        "ARR",
        [],
        parsed_customs=parsed,
        tz_name="America/Toronto",
    )

    assert result.status == "PASS"
    assert "CANPASS arrival" not in (result.summary or "")


def test_customs_hours_ignore_phone_numbers() -> None:
    note = (
        "CUSTOMS:\n"
        "Available\n"
        "Location: Proceed to the CBP ramp unless otherwise directed\n"
        "PH: (813) 676-4590\n"
        "HRS: 0600 - 2200, 7 days a week\n"
        "Call Sector if unable to reach CBP directly"
    )

    parsed = parse_customs_notes([note])

    result = evaluate_customs(
        CustomsProfile(icao="KTPA", service_type="US", notes=None),
        _build_leg("2024-01-01T12:00:00Z"),
        "ARR",
        [],
        parsed_customs=parsed,
        tz_name="America/New_York",
    )

    assert any(hours.get("start") == "0600" for hours in parsed["customs_hours"])
    assert "676-4590" not in (result.summary or "")


def test_24_7_reference_to_other_airport_not_treated_as_hours() -> None:
    note = (
        "CUSTOMS: Available - AOE/15\n"
        "Hours of operation: 08:00L – 22:00L Mon-Fri."
        " If clearance is needed outside of regular hours in the event of a diversion,"
        " continuing to YUL or another airport with 24/7 clearance may be required."
    )

    parsed = parse_customs_notes([note])

    assert parsed["customs_hours"][0]["start"] == "08:00"
    assert parsed["customs_hours"][0]["end"] == "22:00"
    assert not any(
        entry.get("start") == "0000" and entry.get("end") == "2400"
        for entry in parsed["customs_hours"]
    )


def test_afterhours_notice_does_not_trigger_caution_within_hours() -> None:
    note = (
        "CUSTOMS: Available - AOE/15\n"
        "Hours of operation: 08:00L – 22:00L Mon-Fri, \n"
        "Customs outside these hours must be arranged 72hrs business hours in advance by contacting the FBO. \n"
        "After hours services are NOT guaranteed."
    )

    parsed = parse_customs_notes([note])

    result = evaluate_customs(
        CustomsProfile(icao="CYHU", service_type="AOE/15", notes=None),
        _build_leg("2025-11-25T15:08:00Z"),
        "ARR",
        [],
        parsed_customs=parsed,
        tz_name="America/Toronto",
    )

    assert result.status == "PASS"
    assert not any("prior notice" in issue for issue in result.issues)


def test_customs_hours_detects_seven_days_a_week() -> None:
    note = "Hours of operation: 1030-1900, 7 days/wk (No arrivals earlier than 1100LT - CBPO HM)"

    parsed = parse_customs_notes([note])

    assert parsed["customs_hours"]
    assert parsed["customs_hours"][0]["days"] == ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def test_to_separated_hours_are_parsed_before_inline_warning() -> None:
    note = (
        "CBSA free of charge service hours at Saguenay-Bagotville airport (CYBG) "
        "are Monday to Friday 8h00 to 16h30 local time, except holidays. "
        "Clearances between 1600-1630 can be more difficult to obtain as they "
        "want to be able to be fully cleared and shutdown by 1630."
    )

    parsed = parse_customs_notes([note])

    result = evaluate_customs(
        CustomsProfile(icao="CYBG", service_type="AOE", notes=None),
        _build_leg("2024-01-02T17:00:00Z"),
        "ARR",
        [],
        parsed_customs=parsed,
        tz_name="America/Toronto",
    )

    assert parsed["customs_hours"][0]["start"].startswith("8")
    assert parsed["customs_hours"][0]["end"].startswith("16")
    assert parsed["customs_hours"][0]["days"] == ["Mon", "Tue", "Wed", "Thu", "Fri"]
    assert result.status == "PASS"
