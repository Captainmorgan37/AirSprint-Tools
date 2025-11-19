from pathlib import Path
import sys

if str(Path(__file__).resolve().parents[1]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from feasibility import checker_trip

LOOKUP = {
    "CYYZ": {"country": "CA", "subd": "ONTARIO"},
    "KBOS": {"country": "US", "subd": "MASSACHUSETTS"},
    "EGLL": {"country": "GB"},
}


def _build_flight(dep: str, arr: str, notes: list[object]) -> dict[str, object]:
    return {
        "dep_airport": dep,
        "arr_airport": arr,
        "customsNotes": notes,
    }


def test_canada_us_international_skips_extra_jeppesen_note() -> None:
    flight = _build_flight(
        "CYYZ",
        "KBOS",
        [
            {
                "title": "Customs",
                "body": "Customs requires 2 hours notice for arrivals.",
            }
        ],
    )

    result = checker_trip.evaluate_trip(flight, airport_lookup=LOOKUP)

    assert result.status == "PASS"
    assert any("2 hours" in issue for issue in result.issues)
    assert all("Jeppesen customs support" not in issue for issue in result.issues)


def test_out_of_area_international_adds_jeppesen_note() -> None:
    flight = _build_flight(
        "CYYZ",
        "EGLL",
        [
            {
                "title": "Customs",
                "body": "Customs requires 24 hours notice.",
            }
        ],
    )

    result = checker_trip.evaluate_trip(flight, airport_lookup=LOOKUP)

    assert result.status == "CAUTION"
    assert any("International sector includes" in issue for issue in result.issues)


def test_customs_restriction_notes_trigger_caution() -> None:
    flight = _build_flight(
        "CYYZ",
        "KBOS",
        [
            {
                "title": "Customs",
                "body": "Customs closed after 2200 local.",
            }
        ],
    )

    result = checker_trip.evaluate_trip(flight, airport_lookup=LOOKUP)

    assert result.status == "CAUTION"
    assert any("Customs notes indicate restrictions" in issue for issue in result.issues)
