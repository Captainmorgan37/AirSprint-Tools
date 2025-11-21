from __future__ import annotations

from pathlib import Path
import sys
from typing import Any, Dict

sys.path.append(str(Path(__file__).resolve().parents[1]))

from feasibility.quote_lookup import QuoteLookupError, build_quote_leg_options


def _build_quote_with_statuses(statuses: Dict[str, str]) -> Dict[str, Any]:
    return {
        "legs": [
            {
                "id": leg_id,
                "departureAirport": "AAA",
                "arrivalAirport": "BBB",
                "departureDateUTC": "2025-11-19T15:00:00Z",
                "arrivalDateUTC": "2025-11-19T16:15:00Z",
                "status": status,
            }
            for leg_id, status in statuses.items()
        ]
    }


def test_canceled_legs_are_ignored_when_building_options() -> None:
    quote = _build_quote_with_statuses({"L1": "OK", "L2": "CANCELED"})

    options = build_quote_leg_options(quote, quote_id="Q-123")

    assert [option["identifier"] for option in options] == ["L1"]
    assert [option["leg"]["status"] for option in options] == ["OK"]


def test_all_canceled_legs_raise_error() -> None:
    quote = _build_quote_with_statuses({"L1": "CANCELED", "L2": "CANCELLED"})

    try:
        build_quote_leg_options(quote, quote_id="Q-456")
    except QuoteLookupError as exc:
        assert "usable leg" in str(exc)
    else:
        raise AssertionError("Expected QuoteLookupError for all canceled legs")
