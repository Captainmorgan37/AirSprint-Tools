from pathlib import Path
import sys

if str(Path(__file__).resolve().parents[1]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from feasibility.airport_module import DeiceProfile, evaluate_deice
from feasibility.airport_notes_parser import _empty_parsed_restrictions


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
