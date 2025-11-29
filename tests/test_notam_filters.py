from pathlib import Path
import sys
from datetime import datetime

if str(Path(__file__).resolve().parents[1]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from notam_filters import evaluate_closure_notam


def test_evaluate_closure_notam_fails_inside_window() -> None:
    notam = "AIRPORT CLOSED 2000-0600 LOCAL"
    planned_time = datetime(2024, 1, 1, 21, 0)

    assert evaluate_closure_notam(notam, planned_time) == "FAIL"


def test_evaluate_closure_notam_caution_near_start() -> None:
    notam = "AIRPORT CLOSED 2000-0600 LOCAL"
    planned_time = datetime(2024, 1, 1, 18, 45)

    assert evaluate_closure_notam(notam, planned_time) == "CAUTION"


def test_evaluate_closure_notam_caution_near_end() -> None:
    notam = "AIRPORT CLOSED 2000-0600 LOCAL"
    planned_time = datetime(2024, 1, 1, 6, 45)

    assert evaluate_closure_notam(notam, planned_time) == "CAUTION"


def test_evaluate_closure_notam_info_when_far_from_window() -> None:
    notam = "AIRPORT CLOSED 2000-0600 LOCAL"
    planned_time = datetime(2024, 1, 1, 15, 0)

    assert evaluate_closure_notam(notam, planned_time) == "INFO"
