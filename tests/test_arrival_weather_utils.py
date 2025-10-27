from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from arrival_weather_utils import (
    _combine_highlight_levels,
    _get_ceiling_highlight,
    _parse_ceiling_value,
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
