import datetime as dt

from fl3xx_api import Fl3xxApiConfig
from reserve_calendar_pax_pull import run_reserve_pax_pull, select_reserve_dates_for_year


def test_select_reserve_dates_for_year_2025():
    dates = select_reserve_dates_for_year(2025)
    assert dates
    assert all(item.year == 2025 for item in dates)


def test_run_reserve_pax_pull_filters_window_and_pax(monkeypatch):
    target = dt.date(2025, 1, 4)
    flights_payload = [
        {
            "flightId": "F-1",
            "dep_time": "2025-01-04T09:00:00Z",  # 02:00 MT
            "paxNumber": 3,
            "bookingReference": "BK-1",
            "accountName": "Owner A",
            "workflowCustomName": "CANADA Customs",
        },
        {
            "flightId": "F-2",
            "dep_time": "2025-01-04T08:59:00Z",  # 01:59 MT out of window
            "paxNumber": 2,
            "bookingReference": "BK-2",
            "accountName": "Owner B",
            "workflowCustomName": "US Customs",
        },
        {
            "flightId": "F-3",
            "dep_time": "2025-01-04T10:00:00Z",
            "paxNumber": 0,
            "bookingReference": "BK-3",
            "accountName": "Owner C",
            "workflowCustomName": "US Customs",
        },
    ]

    def stub_fetch_flights(config, from_date, to_date, session=None):
        return flights_payload if from_date == target else [], {}

    monkeypatch.setattr("reserve_calendar_pax_pull.fetch_flights", stub_fetch_flights)
    monkeypatch.setattr("reserve_calendar_pax_pull.normalize_fl3xx_payload", lambda payload: (payload["items"], {}))
    monkeypatch.setattr("reserve_calendar_pax_pull.filter_out_subcharter_rows", lambda rows: (rows, 0))

    result = run_reserve_pax_pull(Fl3xxApiConfig(), year=2025)
    jan4 = next(day for day in result.days if day.date == target)

    assert jan4.diagnostics["pax_flights"] == 1
    assert jan4.rows[0]["Flight Ref"] == "BK-1"
    assert jan4.rows[0]["Owner"] == "Owner A"
    assert jan4.rows[0]["PAX"] == 3
