from feasibility.overflight_route import find_route_overflight_countries


def test_find_route_overflight_countries_detects_cuba_crossing() -> None:
    countries = find_route_overflight_countries(
        (25.0, -90.0),
        (21.0, -80.0),
        eligible_countries=["CUBA", "GUATEMALA"],
    )
    assert "CUBA" in countries


def test_find_route_overflight_countries_detects_central_america_crossing() -> None:
    countries = find_route_overflight_countries(
        (14.6, -92.2),
        (15.3, -84.0),
        eligible_countries=["HONDURAS", "NICARAGUA", "EL SALVADOR", "GUATEMALA"],
    )
    assert "HONDURAS" in countries
    assert "GUATEMALA" in countries


def test_find_route_overflight_countries_returns_empty_when_route_misses_polygons() -> None:
    countries = find_route_overflight_countries(
        (40.0, -100.0),
        (42.0, -95.0),
        eligible_countries=["CUBA", "HONDURAS"],
    )
    assert countries == []
