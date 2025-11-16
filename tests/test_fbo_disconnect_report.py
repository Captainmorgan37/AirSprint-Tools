import datetime as dt
from typing import Any, Dict

from fl3xx_api import Fl3xxApiConfig
from morning_reports import (
    MorningReportResult,
    _build_fbo_disconnect_report,
    _extract_handler_company,
)


def iso(ts: dt.datetime) -> str:
    return ts.replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def make_services_fetcher(data: Dict[str, Any]):
    def _fetch(config: Fl3xxApiConfig, flight_id: Any, *, session: Any = None) -> Any:
        key = str(flight_id)
        if key not in data:
            raise AssertionError(f"Unexpected flight id requested: {flight_id}")
        return data[key]

    return _fetch


def make_airport_services_fetcher(data: Dict[str, Any]):
    def _fetch(config: Fl3xxApiConfig, airport: Any, *, session: Any = None) -> Any:
        key = str(airport).upper()
        if key not in data:
            raise AssertionError(f"Unexpected airport requested: {airport}")
        return data[key]

    return _fetch


def test_matching_handlers_are_not_flagged():
    services = {
        "100": {
            "departureHandler": {"company": "Signature Calgary"},
            "arrivalHandler": {"company": "Signature Montreal"},
        },
        "101": {
            "departureHandler": {"company": "Signature Montreal"},
            "arrivalHandler": {"company": "Skyservice Toronto"},
        },
    }

    rows = [
        {
            "tail": "C-GABC",
            "leg_id": "LEG-100",
            "flightId": "100",
            "dep_time": iso(dt.datetime(2024, 6, 1, 14, 0)),
            "arrivalTimeUtc": iso(dt.datetime(2024, 6, 1, 16, 0)),
            "departureAirport": {"icao": "CYYC"},
            "arrivalAirport": {"icao": "CYUL"},
        },
        {
            "tail": "C-GABC",
            "leg_id": "LEG-101",
            "flightId": "101",
            "dep_time": iso(dt.datetime(2024, 6, 2, 1, 0)),
            "arrivalTimeUtc": iso(dt.datetime(2024, 6, 2, 3, 0)),
            "departureAirport": {"icao": "CYUL"},
            "arrivalAirport": {"icao": "CYYZ"},
        },
    ]

    result = _build_fbo_disconnect_report(
        rows,
        Fl3xxApiConfig(),
        fetch_services_fn=make_services_fetcher(services),
        fetch_airport_services_fn=make_airport_services_fetcher({}),
    )

    assert isinstance(result, MorningReportResult)
    assert result.rows == []
    assert result.metadata["match_count"] == 0
    assert result.metadata["comparisons_evaluated"] == 1


def test_mismatched_handlers_are_flagged():
    services = {
        "200": {
            "departureHandler": {"company": "Skyservice Toronto"},
            "arrivalHandler": {"company": "Signature Montreal"},
        },
        "201": {
            "departureHandler": {"company": "Skyservice Montreal"},
            "arrivalHandler": {"company": "Skyservice Ottawa"},
        },
    }

    rows = [
        {
            "tail": "C-GXYZ",
            "leg_id": "LEG-200",
            "flightId": "200",
            "dep_time": iso(dt.datetime(2024, 7, 4, 12, 0)),
            "arrivalTimeUtc": iso(dt.datetime(2024, 7, 4, 14, 0)),
            "departureAirport": {"icao": "CYYZ"},
            "arrivalAirport": {"icao": "CYUL"},
        },
        {
            "tail": "C-GXYZ",
            "leg_id": "LEG-201",
            "flightId": "201",
            "dep_time": iso(dt.datetime(2024, 7, 4, 18, 0)),
            "arrivalTimeUtc": iso(dt.datetime(2024, 7, 4, 20, 0)),
            "departureAirport": {"icao": "CYUL"},
            "arrivalAirport": {"icao": "CYOW"},
        },
    ]

    airport_services = {
        "CYUL": [
            {"company": "Signature Montreal", "type": {"id": 2, "name": "FBO"}},
            {"company": "Skyservice Montreal", "type": {"id": 2, "name": "FBO"}},
        ]
    }

    result = _build_fbo_disconnect_report(
        rows,
        Fl3xxApiConfig(),
        fetch_services_fn=make_services_fetcher(services),
        fetch_airport_services_fn=make_airport_services_fetcher(airport_services),
    )

    assert len(result.rows) == 1
    issue = result.rows[0]
    assert issue["issue_airport"] == "CYUL"
    assert issue["previous_leg_id"] == "LEG-200"
    assert issue["arrival_handler"].upper() == "SIGNATURE MONTREAL"
    assert issue["departure_handler"].upper() == "SKYSERVICE MONTREAL"
    assert "handler mismatch" in issue["line"].lower()
    assert "both handlers listed" in issue["line"].lower()
    assert issue["arrival_handler_listed"] is True
    assert issue["departure_handler_listed"] is True
    assert issue["handler_listing_status"] == "both_listed"
    assert issue["listing_scenario"] == "same_airport"
    assert result.metadata["match_count"] == 1
    assert result.metadata["comparisons_evaluated"] == 1


def test_missing_services_information_is_reported():
    services = {
        "301": {
            "departureHandler": {"company": "Skyservice Vancouver"},
            "arrivalHandler": {"company": "Signature Montreal"},
        }
    }

    rows = [
        {
            "tail": "C-GLMN",
            "leg_id": "LEG-300",
            "dep_time": iso(dt.datetime(2024, 8, 10, 9, 0)),
            "arrivalTimeUtc": iso(dt.datetime(2024, 8, 10, 11, 0)),
            "departureAirport": {"icao": "CYEG"},
            "arrivalAirport": {"icao": "CYVR"},
        },
        {
            "tail": "C-GLMN",
            "leg_id": "LEG-301",
            "flightId": "301",
            "dep_time": iso(dt.datetime(2024, 8, 10, 13, 0)),
            "arrivalTimeUtc": iso(dt.datetime(2024, 8, 10, 15, 0)),
            "departureAirport": {"icao": "CYVR"},
            "arrivalAirport": {"icao": "CYUL"},
        },
    ]

    airport_services = {
        "CYVR": [
            {"company": "Skyservice Vancouver", "type": {"id": 2, "name": "FBO"}},
        ]
    }

    result = _build_fbo_disconnect_report(
        rows,
        Fl3xxApiConfig(),
        fetch_services_fn=make_services_fetcher(services),
        fetch_airport_services_fn=make_airport_services_fetcher(airport_services),
    )

    assert len(result.rows) == 1
    issue = result.rows[0]
    assert issue["arrival_handler"] is None
    assert issue["departure_handler"].upper() == "SKYSERVICE VANCOUVER"
    assert result.warnings
    warning_text = "".join(result.warnings).lower()
    assert "missing flight identifier" in warning_text


def test_partial_airport_listings_flag_potential_scenario_one():
    services = {
        "401": {
            "departureHandler": {"company": "Skyservice Toronto"},
            "arrivalHandler": {"company": "Signature Montreal"},
        },
        "402": {
            "departureHandler": {"company": "Skyservice Montreal"},
            "arrivalHandler": {"company": "Skyservice Ottawa"},
        },
    }

    rows = [
        {
            "tail": "C-GXYZ",
            "leg_id": "LEG-401",
            "flightId": "401",
            "dep_time": iso(dt.datetime(2024, 7, 4, 12, 0)),
            "arrivalTimeUtc": iso(dt.datetime(2024, 7, 4, 14, 0)),
            "departureAirport": {"icao": "CYYZ"},
            "arrivalAirport": {"icao": "CYUL"},
        },
        {
            "tail": "C-GXYZ",
            "leg_id": "LEG-402",
            "flightId": "402",
            "dep_time": iso(dt.datetime(2024, 7, 4, 18, 0)),
            "arrivalTimeUtc": iso(dt.datetime(2024, 7, 4, 20, 0)),
            "departureAirport": {"icao": "CYUL"},
            "arrivalAirport": {"icao": "CYOW"},
        },
    ]

    airport_services = {
        "CYUL": [
            {"company": "Signature Montreal", "type": {"id": 2, "name": "FBO"}},
        ]
    }

    result = _build_fbo_disconnect_report(
        rows,
        Fl3xxApiConfig(),
        fetch_services_fn=make_services_fetcher(services),
        fetch_airport_services_fn=make_airport_services_fetcher(airport_services),
    )

    assert len(result.rows) == 1
    issue = result.rows[0]
    assert issue["arrival_handler_listed"] is True
    assert issue["departure_handler_listed"] is False
    assert issue["handler_listing_status"] == "missing_handler"
    assert issue["listing_scenario"] == "handler_missing"
    assert "departure missing" in issue["line"].lower()


def test_handler_company_is_extracted_from_nested_airport_service():
    payload = {
        "arrivalHandler": {
            "airportService": {
                "company": "Banyan Air Service",
                "phone": "+1-954-491-3170",
            }
        },
        "departureHandler": {
            "airportService": {
                "company": "Signature Montreal",
            }
        },
    }

    arrival = _extract_handler_company(payload, departure=False)
    departure = _extract_handler_company(payload, departure=True)

    assert arrival == "Banyan Air Service"
    assert departure == "Signature Montreal"

