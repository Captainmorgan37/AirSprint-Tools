import datetime as dt
from typing import Dict, List

from flight_following_reports import (
    DutyStartCollection,
    compute_short_turn_summary_for_collection,
)
from short_turn_utils import summarize_short_turns


def _build_flight(
    tail: str,
    dep_airport: str,
    arr_airport: str,
    dep_time: str,
    arr_time: str,
    *,
    leg_id: str,
    flight_id: str,
    workflow: str | None = None,
    account: str | None = None,
) -> Dict[str, object]:
    payload: Dict[str, object] = {
        "tailNumber": tail,
        "departureAirportIcao": dep_airport,
        "arrivalAirportIcao": arr_airport,
        "scheduledOut": dep_time,
        "scheduledIn": arr_time,
        "bookingIdentifier": leg_id,
        "flightId": flight_id,
    }
    if workflow is not None:
        payload["workflowCustomName"] = workflow
    if account is not None:
        payload["accountName"] = account
    return payload


def test_summarize_short_turns_formats_expected_text() -> None:
    flights: List[Dict[str, object]] = [
        _build_flight(
            tail="C-GABC",
            dep_airport="CYYC",
            arr_airport="CYEG",
            dep_time="2024-10-23T15:00:00Z",
            arr_time="2024-10-23T16:00:00Z",
            leg_id="LEG1",
            flight_id="F1",
        ),
        _build_flight(
            tail="C-GABC",
            dep_airport="CYEG",
            arr_airport="CYYC",
            dep_time="2024-10-23T16:35:00Z",
            arr_time="2024-10-23T17:40:00Z",
            leg_id="LEG2",
            flight_id="F2",
            account="Sevensun Services",
        ),
    ]

    summary_text, short_df, metadata = summarize_short_turns(
        flights,
        threshold_min=45,
        priority_threshold_min=45,
    )

    assert "Short turns:" not in summary_text
    lines = summary_text.splitlines()
    assert lines
    assert lines[0].startswith("C-GABC")
    assert "C-GABC" in summary_text
    assert "Sevensun Services" in summary_text
    assert "35 mins" in summary_text
    assert metadata["turns_detected"] == 1
    assert not short_df.empty


def test_summarize_short_turns_respects_priority_threshold_override() -> None:
    flights: List[Dict[str, object]] = [
        _build_flight(
            tail="C-GXYZ",
            dep_airport="CYYC",
            arr_airport="CYEG",
            dep_time="2024-10-23T14:00:00Z",
            arr_time="2024-10-23T15:00:00Z",
            leg_id="A1",
            flight_id="FA1",
        ),
        _build_flight(
            tail="C-GXYZ",
            dep_airport="CYEG",
            arr_airport="CYYZ",
            dep_time="2024-10-23T15:50:00Z",
            arr_time="2024-10-23T18:10:00Z",
            leg_id="A2",
            flight_id="FA2",
            workflow="Priority Mission",
            account="Priority Account",
        ),
    ]

    summary_text_default, short_df_default, metadata_default = summarize_short_turns(
        flights,
        threshold_min=45,
    )
    assert metadata_default["turns_detected"] == 1
    assert "C-GXYZ" in summary_text_default
    assert "Priority Account" in summary_text_default
    assert not short_df_default.empty

    summary_text_override, short_df_override, metadata_override = summarize_short_turns(
        flights,
        threshold_min=45,
        priority_threshold_min=45,
    )
    assert metadata_override["turns_detected"] == 0
    assert short_df_override.empty
    assert "None" in summary_text_override


def test_compute_short_turn_summary_for_collection_extracts_flights() -> None:
    target_date = dt.date(2024, 10, 23)
    collection = DutyStartCollection(
        target_date=target_date,
        start_utc=dt.datetime(2024, 10, 23, tzinfo=dt.timezone.utc),
        end_utc=dt.datetime(2024, 10, 24, tzinfo=dt.timezone.utc),
        snapshots=[],
        grouped_flights={
            "C-GABC": [
                {"flight_payload": _build_flight(
                    tail="C-GABC",
                    dep_airport="CYYC",
                    arr_airport="CYEG",
                    dep_time="2024-10-23T15:00:00Z",
                    arr_time="2024-10-23T16:00:00Z",
                    leg_id="LEG1",
                    flight_id="F1",
                )},
                {"flight_payload": _build_flight(
                    tail="C-GABC",
                    dep_airport="CYEG",
                    arr_airport="CYYC",
                    dep_time="2024-10-23T16:35:00Z",
                    arr_time="2024-10-23T17:40:00Z",
                    leg_id="LEG2",
                    flight_id="F2",
                )},
            ]
        },
    )

    summary_text, count, metadata = compute_short_turn_summary_for_collection(
        collection,
        threshold_min=45,
        priority_threshold_min=45,
        local_tz_name="UTC",
    )

    assert "Short turns:" not in summary_text
    assert summary_text.startswith("C-GABC")
    assert count == 1
    assert metadata["turns_detected"] == 1
