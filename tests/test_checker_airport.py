from feasibility import checker_airport as airport


def test_international_requires_two_countries_case_insensitive():
    assert airport._international("Canada", "Canada") is False
    assert airport._international("Canada", "United States") is True
    assert airport._international("ca", "CA") is False
    assert airport._international("CA", None) is False
