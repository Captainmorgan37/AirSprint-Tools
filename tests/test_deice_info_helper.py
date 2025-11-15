from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from deice_info_helper import get_deice_record, has_deice_available


def test_lookup_prefers_icao():
    record = get_deice_record(icao="CYAM")
    assert record is not None
    assert record.icao == "CYAM"
    assert record.has_deice is True
    assert "Types 1 & 4 available" in (record.deice_info or "")


def test_no_deice_flag():
    record = get_deice_record(icao="CYAZ")
    assert record is not None
    assert record.has_deice is False
    assert "Not Available" in (record.deice_info or "")


def test_blank_flag_returns_none():
    record = get_deice_record(icao="CJW5")
    assert record is not None
    assert record.has_deice is None


def test_iterable_identifiers_and_helper_flag():
    record = get_deice_record(identifiers=["", "cyaz"])
    assert record is not None
    assert record.icao == "CYAZ"
    assert has_deice_available(identifiers=["cyam"]) is True
    assert has_deice_available(identifiers=["cyaz"]) is False
