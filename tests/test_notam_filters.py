import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import notam_filters as nf


def test_taxiway_only_notam_hidden():
    text = "!MWH 10/062 MWH TWY J CLSD 2510241430-2510242300"
    assert nf.is_taxiway_only_notam(text) is True


def test_runway_notam_not_hidden_when_runway_present():
    text = "RWY 08/26 CLSD AVBL AS TWY"
    assert nf.is_taxiway_only_notam(text) is False


def test_non_taxi_notam_visible():
    text = "APRONS CLOSED FOR MAINTENANCE"
    assert nf.is_taxiway_only_notam(text) is False
