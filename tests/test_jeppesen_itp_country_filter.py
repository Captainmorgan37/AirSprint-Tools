from jeppesen_itp_utils import (
    ALLOWED_COUNTRY_IDENTIFIERS,
    country_display_name,
    normalize_country_name,
)


def _should_flag(country):
    normalized = normalize_country_name(country)
    if not normalized:
        return False
    return normalized not in ALLOWED_COUNTRY_IDENTIFIERS


def test_allowed_country_codes_not_flagged():
    assert not _should_flag("US")
    assert not _should_flag("CA")
    assert not _should_flag("MX")


def test_caribbean_country_codes_not_flagged():
    assert not _should_flag("BS")  # Bahamas
    assert not _should_flag("TT")  # Trinidad and Tobago
    assert not _should_flag("VG")  # British Virgin Islands


def test_country_display_name_for_iso_codes():
    assert country_display_name("CO") == "Colombia"
    assert country_display_name("fr") == "France"


def test_country_display_name_for_special_cases():
    assert country_display_name("SX") == "Sint Maarten"
    assert country_display_name("BQ") == "Bonaire, Sint Eustatius, and Saba"
