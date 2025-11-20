from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from arrival_weather_utils import (
    _combine_highlight_levels,
    _get_ceiling_highlight,
    _get_visibility_highlight,
    _get_wind_highlight,
    _has_freezing_precip,
    _has_wintry_precip,
    _build_weather_value_html,
    _parse_ceiling_value,
    _parse_visibility_value,
)


def test_parse_ceiling_handles_raw_taf_code():
    assert _parse_ceiling_value("BKN004") == 400.0
    assert _get_ceiling_highlight("BKN004") == "red"


def test_parse_ceiling_handles_explicit_feet_suffix():
    assert _parse_ceiling_value("BKN 400ft") == 400.0
    assert _get_ceiling_highlight("BKN 400ft") == "red"


def test_combine_highlight_levels_prioritises_highest_severity():
    assert _combine_highlight_levels([None, "yellow", None]) == "yellow"
    assert _combine_highlight_levels(["yellow", "red"]) == "red"
    assert _combine_highlight_levels([None, None]) is None


def test_parse_visibility_handles_mixed_fractions():
    assert _parse_visibility_value("2 1/2SM") == 2.5
    assert _parse_visibility_value("1/2SM 2 1/2SM") == 2.5


def test_visibility_highlight_uses_combined_value():
    assert _get_visibility_highlight("2 1/2SM") == "yellow"


def test_wind_highlight_triggers_on_speed_or_gust():
    assert _get_wind_highlight("180 30kt") == "red"
    assert _get_wind_highlight("Wind 220 20kt G30") == "red"


def test_wind_highlight_ignores_lower_values():
    assert _get_wind_highlight("140 12kt") is None
    assert _get_wind_highlight("Wind Calm") is None


def test_detects_freezing_precip_tokens():
    assert _has_freezing_precip("-FZRA") is True
    assert _has_freezing_precip("FZFG") is True
    assert _has_freezing_precip("SN") is False


def test_detects_freezing_precip_from_list_like_strings():
    assert _has_freezing_precip("['FZRA', 'BR']") is True


def test_detects_wintry_precip_with_prefixes():
    assert _has_wintry_precip("VCSH SN") is True
    assert _has_wintry_precip("PLRA") is True
    assert _has_wintry_precip("BR") is False


def test_detects_wintry_precip_from_list_like_strings():
    assert _has_wintry_precip("['SN', 'BR']") is True


def test_weather_value_html_highlights_only_precip_tokens():
    rendered = _build_weather_value_html("CYOW, -FZRA, BR", "full")
    assert rendered is not None
    assert rendered.count("taf-highlight--blue") == 1
    assert "CYOW" in rendered and "BR" in rendered
    assert "CYOW</span>" not in rendered


def test_weather_value_html_requires_limited_deice_for_snow():
    assert _build_weather_value_html("SN BR", "full") is None
    rendered = _build_weather_value_html("SN BR", "unknown")
    assert rendered is not None
    assert rendered.count("taf-highlight--blue") == 1
