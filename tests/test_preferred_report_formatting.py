from datetime import datetime, timezone

import morning_reports
from morning_reports import MorningReportResult


def test_upgraded_flights_preferred_block_formatting():
    rows = [
        {
            "date": "2025-10-17",
            "tail": "CFASF",
            "booking_reference": "EPIJK",
            "account_name": "Tim Hockey",
        },
        {
            "date": "2025-10-17",
            "tail": "CGBAS",
            "booking_reference": "JEOXZ1",
            "account_name": "Tammy Ann Vipond",
        },
        {
            "date": "2025-10-18",
            "tail": "CFSNY",
            "booking_reference": "ABIHZ1",
            "account_name": "Mecii Management Inc",
        },
        {
            "date": "2025-10-18",
            "tail": "CFASQ",
            "booking_reference": "ETMIH",
            "account_name": "Kolikana Holdings Ltd",
        },
    ]

    result = MorningReportResult(
        code="16.1.10",
        title="Upgraded Flights Report",
        header_label="Upgrade Workflow Flights",
        rows=rows,
    )

    expected = (
        "UPGRADES: (based on the Upgraded Flights Report)\n\n"
        "17OCT25\n\n"
        "CFASF - EPIJK - Tim Hockey\n"
        "CGBAS - JEOXZ1 - Tammy Ann Vipond\n\n"
        "18OCT25\n\n"
        "CFSNY - ABIHZ1 - Mecii Management Inc\n"
        "CFASQ - ETMIH - Kolikana Holdings Ltd"
    )

    assert result.formatted_output() == expected


def test_preferred_block_converts_datetimes_to_mountain_dates():
    rows = [
        {
            "date": datetime(2025, 10, 20, 1, 0, tzinfo=timezone.utc),
            "tail": "CFASF",
            "booking_reference": "EPIJK",
            "account_name": "Tim Hockey",
        }
    ]

    result = MorningReportResult(
        code="16.1.10",
        title="Upgraded Flights Report",
        header_label="Upgrade Workflow Flights",
        rows=rows,
    )

    expected = (
        "UPGRADES: (based on the Upgraded Flights Report)\n\n"
        "19OCT25\n\n"
        "CFASF - EPIJK - Tim Hockey"
    )

    assert result.formatted_output() == expected


def test_cj3_on_cj2_preferred_block_formatting():
    rows = [
        {
            "date": "2025-10-17",
            "tail": "CFASP",
            "booking_identifier": "HEGEU",
            "account_name": "Michael Culbert and Heather Culbert",
            "pax_count": 1,
            "block_time_display": "2:23",
        },
        {
            "date": "2025-10-18",
            "tail": "CFASP",
            "booking_identifier": "EHIBG",
            "account_name": "IG Av Group Ltd.",
            "pax_count": 1,
            "block_time_display": "2:33",
        },
    ]

    result = MorningReportResult(
        code="16.1.6",
        title="CJ3 Owners on CJ2 Report",
        header_label="CJ3 Owners on CJ2",
        rows=rows,
    )

    expected = (
        "CJ3 CLIENTS ON CJ2: (based on the CJ3 Owners on CJ2 Report)\n\n"
        "17OCT25\n\n"
        "CFASP - HEGEU - Michael Culbert and Heather Culbert - 1 PAX - 2:23 FLIGHT TIME\n\n"
        "18OCT25\n\n"
        "CFASP - EHIBG - IG Av Group Ltd. - 1 PAX - 2:33 FLIGHT TIME"
    )

    assert result.formatted_output() == expected


def test_cj3_on_cj2_preferred_block_includes_runway_alerts():
    rows = [
        {
            "date": "2025-10-17",
            "tail": "CFASP",
            "booking_identifier": "HEGEU",
            "account_name": "Michael Culbert and Heather Culbert",
            "pax_count": 1,
            "block_time_display": "2:23",
            "runway_alerts": [
                {
                    "role": "Departure",
                    "airport": "CYBW",
                    "airport_raw": "CYBW",
                    "max_runway_length_ft": 4800,
                },
                {
                    "role": "Arrival",
                    "airport": "CYQL",
                    "airport_raw": "CYQL",
                    "max_runway_length_ft": 4700,
                },
            ],
            "runway_alert_threshold_ft": 4900,
        }
    ]

    result = MorningReportResult(
        code="16.1.6",
        title="CJ3 Owners on CJ2 Report",
        header_label="CJ3 Owners on CJ2",
        rows=rows,
    )

    expected = (
        "CJ3 CLIENTS ON CJ2: (based on the CJ3 Owners on CJ2 Report)\n\n"
        "17OCT25\n\n"
        "CFASP - HEGEU - Michael Culbert and Heather Culbert - 1 PAX - 2:23 FLIGHT TIME\n"
        "    ALERT: Departure CYBW max runway 4,800 FT (< 4,900 FT)\n"
        "    ALERT: Arrival CYQL max runway 4,700 FT (< 4,900 FT)"
    )

    assert result.formatted_output() == expected


def test_cj3_on_cj2_preferred_block_omits_runway_confirmation_notice():
    rows = [
        {
            "date": "2025-10-17",
            "tail": "CFASP",
            "booking_identifier": "HEGEU",
            "account_name": "Michael Culbert and Heather Culbert",
            "pax_count": 1,
            "block_time_display": "2:23",
            "runway_alerts": [],
            "runway_alert_threshold_ft": 4900,
        }
    ]

    result = MorningReportResult(
        code="16.1.6",
        title="CJ3 Owners on CJ2 Report",
        header_label="CJ3 Owners on CJ2",
        rows=rows,
    )

    expected = (
        "CJ3 CLIENTS ON CJ2: (based on the CJ3 Owners on CJ2 Report)\n\n"
        "17OCT25\n\n"
        "CFASP - HEGEU - Michael Culbert and Heather Culbert - 1 PAX - 2:23 FLIGHT TIME"
    )

    assert result.formatted_output() == expected


def test_cj3_on_cj2_preferred_block_strips_confirmation_note_from_copy_block(
    monkeypatch,
):
    rows = [
        {
            "date": "2025-10-17",
            "tail": "CFASP",
            "booking_identifier": "HEGEU",
            "account_name": "Michael Culbert and Heather Culbert",
            "pax_count": 1,
            "block_time_display": "2:23",
            "runway_alerts": [],
            "runway_alert_threshold_ft": 4900,
        }
    ]

    expected = morning_reports._render_preferred_block(
        rows,
        header="CJ3 CLIENTS ON CJ2: (based on the CJ3 Owners on CJ2 Report)",
        line_builder=morning_reports._build_cj3_line,
    )

    result = MorningReportResult(
        code="16.1.6",
        title="CJ3 Owners on CJ2 Report",
        header_label="CJ3 Owners on CJ2",
        rows=rows,
        metadata={"runway_confirmation_note": "All runways confirmed as 4,900' or longer"},
    )

    original_render = morning_reports._render_preferred_block

    def fake_render(rows, *, header, line_builder):
        block = original_render(rows, header=header, line_builder=line_builder)
        return "\n".join([block, "    All runways confirmed as 4,900' or longer"])

    monkeypatch.setattr(morning_reports, "_render_preferred_block", fake_render)

    assert result.formatted_output() == expected


def test_priority_status_preferred_block_formatting():
    rows = [
        {
            "date": "2025-10-17",
            "tail": "CFSFS",
            "booking_reference": "EXWEI",
            "account_name": "CJ3CO LP, by its general partner, CJ3CO INC.",
            "has_issue": True,
            "status": "Turn time only 45.0 min before departure (requires 90 min)",
        },
        {
            "date": "2025-10-17",
            "tail": "CGASR",
            "booking_reference": "LIAZF",
            "account_name": "Eastside Dodge Chrysler Jeep Ltd.",
            "has_issue": True,
            "status": "Missing crew check-in timestamps",
        },
        {
            "date": "2025-10-18",
            "tail": "CFSEF",
            "booking_reference": "ONMOZ",
            "account_name": "2350803 Alberta Ltd.",
            "has_issue": False,
        },
        {
            "date": "2025-10-18",
            "tail": "CFSFS",
            "booking_reference": "EMHIC",
            "account_name": "25 Woodstream Holdings Ltd.",
            "has_issue": False,
        },
    ]

    result = MorningReportResult(
        code="16.1.7",
        title="Priority Status Report",
        header_label="Priority Duty-Start Validation",
        rows=rows,
    )

    expected = (
        "PRIORITY CLIENTS: (based on the Priority Status Report)\n\n"
        "17OCT25\n\n"
        "CFSFS - EXWEI - CJ3CO LP, by its general partner, CJ3CO INC. - NOT ACCOMMODATED - Turn time only 45.0 min before departure (requires 90 min)\n"
        "CGASR - LIAZF - Eastside Dodge Chrysler Jeep Ltd. - NOT ACCOMMODATED - Missing crew check-in timestamps\n\n"
        "18OCT25\n\n"
        "CFSEF - ONMOZ - 2350803 Alberta Ltd. - ACCOMMODATED\n"
        "CFSFS - EMHIC - 25 Woodstream Holdings Ltd. - ACCOMMODATED"
    )

    assert result.formatted_output() == expected
