from datetime import date, datetime, timezone

import pytest
import requests

from fl3xx_api import Fl3xxApiConfig, fetch_flights, MOUNTAIN_TIME_ZONE_NAME


class FakeResponse:
    def __init__(self, status_code: int = 200, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else []

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None, verify=None):
        self.calls.append({"url": url, "params": params})
        if not self.responses:
            raise RuntimeError("No response queued")
        return self.responses.pop(0)

    def close(self):
        pass


def test_fetch_flights_retries_on_400_with_split():
    session = FakeSession(
        [
            FakeResponse(status_code=400),
            FakeResponse(payload=[{"id": "left"}]),
            FakeResponse(payload=[{"id": "right"}]),
        ]
    )
    config = Fl3xxApiConfig(api_token="token")
    reference_time = datetime(2024, 1, 1, tzinfo=timezone.utc)

    flights, metadata = fetch_flights(
        config,
        from_date=date(2024, 1, 1),
        to_date=date(2024, 1, 5),
        session=session,
        now=reference_time,
    )

    assert flights == [{"id": "left"}, {"id": "right"}]
    assert metadata["partial_requests"][0]["from_date"] == "2024-01-01"
    assert metadata["partial_requests"][0]["to_date"] == "2024-01-03"
    assert metadata["partial_requests"][1]["from_date"] == "2024-01-03"
    assert metadata["partial_requests"][1]["to_date"] == "2024-01-05"
    assert session.calls[0]["params"][1] == ("timeZone", MOUNTAIN_TIME_ZONE_NAME)
    assert len(session.calls) == 3


def test_fetch_flights_propagates_second_400_after_split_attempt():
    session = FakeSession(
        [
            FakeResponse(status_code=400),
            FakeResponse(status_code=400),
        ]
    )
    config = Fl3xxApiConfig(api_token="token")
    reference_time = datetime(2024, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(requests.HTTPError):
        fetch_flights(
            config,
            from_date=date(2024, 1, 1),
            to_date=date(2024, 1, 5),
            session=session,
            now=reference_time,
        )

    assert len(session.calls) == 2
