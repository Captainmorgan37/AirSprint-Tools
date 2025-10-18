import datetime as dt
import pathlib
import sys
from typing import Optional

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from morning_reports import (
    MorningReportResult,
    _build_owner_continuous_flight_validation_report,
)


def _leg(
    *,
    account: str,
    tail: str,
    dep: str,
    arr: str,
    leg_id: str,
    flight_type: str = "PAX",
    extra: Optional[dict] = None,
):
    row = {
        "accountName": account,
        "tail": tail,
        "dep_time": dep,
        "arrivalTimeUtc": arr,
        "leg_id": leg_id,
        "flightType": flight_type,
    }
    if extra:
        row.update(extra)
    return row


def iso(ts: dt.datetime) -> str:
    return ts.replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def test_same_tail_sequences_are_not_flagged():
    dep1 = iso(dt.datetime(2024, 5, 1, 12, 0))
    arr1 = iso(dt.datetime(2024, 5, 1, 14, 0))
    dep2 = iso(dt.datetime(2024, 5, 1, 18, 0))
    arr2 = iso(dt.datetime(2024, 5, 1, 20, 0))

    rows = [
        _leg(account="Owner A", tail="C-GAPP", dep=dep1, arr=arr1, leg_id="1"),
        _leg(account="Owner A", tail="C-GAPP", dep=dep2, arr=arr2, leg_id="2"),
    ]

    result = _build_owner_continuous_flight_validation_report(rows)

    assert isinstance(result, MorningReportResult)
    assert result.rows == []
    assert result.metadata["match_count"] == 0


def test_tail_change_with_large_gap_is_allowed():
    dep1 = iso(dt.datetime(2024, 5, 2, 8, 0))
    arr1 = iso(dt.datetime(2024, 5, 2, 9, 0))
    dep2 = iso(dt.datetime(2024, 5, 2, 13, 0))
    arr2 = iso(dt.datetime(2024, 5, 2, 15, 0))

    rows = [
        _leg(account="Owner B", tail="C-GAAA", dep=dep1, arr=arr1, leg_id="10"),
        _leg(account="Owner B", tail="C-GBBB", dep=dep2, arr=arr2, leg_id="11"),
    ]

    result = _build_owner_continuous_flight_validation_report(rows)

    assert result.rows == []
    assert result.metadata["match_count"] == 0


def test_tail_change_with_small_gap_is_flagged():
    dep1 = iso(dt.datetime(2024, 5, 3, 9, 0))
    arr1 = iso(dt.datetime(2024, 5, 3, 10, 30))
    dep2 = iso(dt.datetime(2024, 5, 3, 12, 0))
    arr2 = iso(dt.datetime(2024, 5, 3, 13, 0))

    dep3 = iso(dt.datetime(2024, 5, 3, 18, 0))
    arr3 = iso(dt.datetime(2024, 5, 3, 19, 0))

    rows = [
        _leg(account="Owner C", tail="C-G111", dep=dep1, arr=arr1, leg_id="21"),
        _leg(account="Owner C", tail="C-G222", dep=dep2, arr=arr2, leg_id="22"),
        _leg(account="Owner C", tail="C-G222", dep=dep3, arr=arr3, leg_id="23"),
    ]

    # Include an OCS PAX leg which should be ignored.
    rows.append(
        _leg(
            account="AIRSPRINT INC.",
            tail="C-G333",
            dep=dep1,
            arr=arr1,
            leg_id="99",
            extra={"flightType": "PAX"},
        )
    )

    result = _build_owner_continuous_flight_validation_report(rows)

    assert len(result.rows) == 1
    discrepancy = result.rows[0]
    assert discrepancy["account_name"] == "Owner C"
    assert discrepancy["previous_tail"] == "C-G111"
    assert discrepancy["next_tail"] == "C-G222"
    assert discrepancy["gap_minutes"] == 90
    assert result.metadata["match_count"] == 1
    assert result.metadata["flagged_accounts"] == ["Owner C"]
    assert "Owner C" in discrepancy["line"]


def test_non_pax_and_placeholder_legs_are_skipped():
    dep = iso(dt.datetime(2024, 5, 4, 6, 0))
    arr = iso(dt.datetime(2024, 5, 4, 7, 0))

    rows = [
        _leg(
            account="Owner D",
            tail="APP CJ3+",
            dep=dep,
            arr=arr,
            leg_id="31",
        ),
        _leg(
            account="Owner D",
            tail="C-G444",
            dep=dep,
            arr=arr,
            leg_id="32",
            flight_type="POS",
        ),
    ]

    result = _build_owner_continuous_flight_validation_report(rows)

    assert result.rows == []
    assert result.metadata["match_count"] == 0
