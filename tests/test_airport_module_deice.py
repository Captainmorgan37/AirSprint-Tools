from pathlib import Path
import sys

if str(Path(__file__).resolve().parents[1]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from feasibility.airport_module import DeiceProfile, _build_deice_profile, evaluate_deice
from feasibility.airport_notes_parser import _empty_parsed_restrictions
from flight_leg_utils import load_airport_metadata_lookup


def test_deice_notes_deduplicated_between_sources() -> None:
    note = (
        "DEICE/ANTI ICE: Types 1 and 4 available with limited hold over times."
    )
    parsed = _empty_parsed_restrictions()
    parsed["deice_notes"] = [note]
    profile = DeiceProfile(
        icao="CYYC", deice_available=True, notes=note, latitude=None, longitude=None
    )

    result = evaluate_deice(
        profile,
        operational_notes=[],
        leg={},
        side="arrival",
        parsed_restrictions=parsed,
    )

    assert result.status == "PASS"
    assert result.summary == "Deice available"
    assert result.issues == [f"Deice note: {note}"]


def test_warm_weather_detected_from_metadata_coordinates() -> None:
    metadata = load_airport_metadata_lookup()
    profile = _build_deice_profile("MYAM", metadata=metadata)

    result = evaluate_deice(profile, operational_notes=[], leg={}, side="arrival")

    assert result.status == "PASS"
    assert result.summary == "Deice not required (warm region)"
    assert result.issues == []
