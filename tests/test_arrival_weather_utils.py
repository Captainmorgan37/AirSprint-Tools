from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from arrival_weather_utils import (
    _combine_highlight_levels,
    _get_ceiling_highlight,
    _get_visibility_highlight,
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
