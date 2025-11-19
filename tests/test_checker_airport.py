from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from feasibility import checker_airport as airport
from feasibility.data_access import CustomsRule
from feasibility.schemas import CategoryResult


def test_international_requires_two_countries_case_insensitive():
    assert airport._international("Canada", "Canada") is False
    assert airport._international("Canada", "United States") is True
    assert airport._international("ca", "CA") is False
    assert airport._international("CA", None) is False


def test_evaluate_airport_uses_feasibility_module_when_leg_context_available(monkeypatch):
    called: dict[str, object] = {}

    def fake_build_leg_context(flight, *, airport_metadata):  # noqa: ANN001
        called["context"] = (flight, airport_metadata)
        return {"leg_id": "123", "departure_icao": "CYYZ", "arrival_icao": "KTEB"}

    def fake_evaluate_leg(*args, **kwargs):  # noqa: ANN001, ANN002
        called["evaluate_args"] = (args, kwargs)
        return object()

    def fake_summarize(result):  # noqa: ANN001
        called["summarize"] = result
        return CategoryResult(status="CAUTION", summary="Structured result", issues=["X"])

    def explode_has_deice_available(*, icao: str):  # pragma: no cover - should never be called
        raise AssertionError(f"Unexpected deice lookup for {icao}")

    monkeypatch.setattr(airport, "build_leg_context_from_flight", fake_build_leg_context)
    monkeypatch.setattr(airport, "evaluate_airport_feasibility_for_leg", fake_evaluate_leg)
    monkeypatch.setattr(airport, "_summarize_airport_feasibility", fake_summarize)
    monkeypatch.setattr(airport, "has_deice_available", explode_has_deice_available)

    flight = {"dep_airport": "CYYZ", "arr_airport": "KTEB"}
    lookup = {
        "CYYZ": {"country": "CA", "tz": "America/Toronto"},
        "KTEB": {"country": "US", "tz": "America/New_York"},
    }

    result = airport.evaluate_airport(flight, airport_lookup=lookup, customs_rules={})

    assert isinstance(result, CategoryResult)
    assert result.summary == "Structured result"
    assert "context" in called and "evaluate_args" in called and "summarize" in called


def test_evaluate_airport_calls_deice_helper_when_leg_context_missing(monkeypatch):
    called = {}

    def fake_has_deice_available(*, icao: str):
        called["icao"] = icao
        return False

    monkeypatch.setattr(airport, "has_deice_available", fake_has_deice_available)
    monkeypatch.setattr(airport, "build_leg_context_from_flight", lambda *_, **__: None)
    monkeypatch.setattr(airport, "evaluate_airport_feasibility_for_leg", lambda *args, **kwargs: None)

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
