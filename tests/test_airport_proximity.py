from __future__ import annotations

from unittest.mock import Mock

import pytest

import airport_proximity as ap


def test_haversine_nm_zero_distance() -> None:
    assert ap.haversine_nm(51.0, -114.0, 51.0, -114.0) == pytest.approx(0.0)


def test_geocode_address_mapbox_parses_response(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_response = Mock()
    mock_response.content = b"{}"
    mock_response.json.return_value = {"features": [{"center": [-114.0719, 51.0447]}]}
    mock_response.raise_for_status.return_value = None

    mock_get = Mock(return_value=mock_response)
    monkeypatch.setattr(ap.requests, "get", mock_get)
    ap.geocode_address_mapbox.cache_clear()

    lat, lon = ap.geocode_address_mapbox("Calgary, AB", token="test-token")

    assert (lat, lon) == pytest.approx((51.0447, -114.0719))
    mock_get.assert_called_once()


def test_nearest_airports_filters_and_sorts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ap, "_load_runway_lengths", lambda: {"CYYY": 5000, "CYYC": 12000, "CYBW": 5000})
    monkeypatch.setattr(
        ap,
        "load_fl3xx_airport_categories",
        lambda: {
            "CYYY": type("R", (), {"category": "A"})(),
            "CYYC": type("R", (), {"category": "B"})(),
            "CYBW": type("R", (), {"category": "NC"})(),
        },
    )

    records = [
        ("CYYY", "Airport 1", "City 1", 51.05, -114.05),
        ("CYYC", "Airport 2", "City 2", 51.13, -114.01),
        ("CYBW", "Airport 3", "City 3", 51.10, -114.37),
    ]

    result = ap.nearest_airports(
        51.05,
        -114.07,
        limit=5,
        min_runway_ft=4500,
        allowed_categories=["A", "B"],
        airport_records=records,
    )

    assert [item.icao for item in result] == ["CYYY", "CYYC"]
    assert all(item.airport_category in {"A", "B"} for item in result)


def test_suggest_addresses_mapbox_parses_results(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_response = Mock()
    mock_response.content = b"{}"
    mock_response.json.return_value = {
        "features": [
            {"place_name": "Calgary, Alberta, Canada", "center": [-114.0719, 51.0447]},
            {"place_name": "Calgary International Airport", "center": [-114.019, 51.1215]},
        ]
    }
    mock_response.raise_for_status.return_value = None

    mock_get = Mock(return_value=mock_response)
    monkeypatch.setattr(ap.requests, "get", mock_get)

    suggestions = ap.suggest_addresses_mapbox("calg", token="test-token", limit=2)

    assert [item.label for item in suggestions] == [
        "Calgary, Alberta, Canada",
        "Calgary International Airport",
    ]
    assert suggestions[0].latitude == pytest.approx(51.0447)
    assert suggestions[0].longitude == pytest.approx(-114.0719)
