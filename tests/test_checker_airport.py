from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from feasibility import checker_airport as airport
from feasibility.data_access import CustomsRule


def test_international_requires_two_countries_case_insensitive():
    assert airport._international("Canada", "Canada") is False
    assert airport._international("Canada", "United States") is True
    assert airport._international("ca", "CA") is False
    assert airport._international("CA", None) is False


def test_evaluate_airport_calls_deice_helper(monkeypatch):
    called = {}

    def fake_has_deice_available(*, icao: str):
        called["icao"] = icao
        return False

    monkeypatch.setattr(airport, "has_deice_available", fake_has_deice_available)

    flight = {
        "dep_airport": "CYYZ",
        "arr_airport": "KTEB",
    }
    lookup = {
        "CYYZ": {"country": "CA", "subd": "ONTARIO"},
        "KTEB": {"country": "US", "subd": "NEW JERSEY"},
    }
    customs_rules = {
        "KTEB": CustomsRule(airport="KTEB", service_type=None, notes=None)
    }

    result = airport.evaluate_airport(flight, airport_lookup=lookup, customs_rules=customs_rules)

    assert called == {"icao": "KTEB"}
    assert result.summary == "No deice available at KTEB"
