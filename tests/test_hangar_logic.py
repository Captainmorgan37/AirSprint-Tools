import pytest

from hangar_logic import (
    evaluate_hangar_need,
    identify_aircraft_category,
    is_client_departure,
)


def _build_taf(temp: float | None = None, weather: str | None = None, raw: str = ""):
    details = []
    if temp is not None:
        details.append(("Temperature", str(temp)))
    if weather:
        details.append(("Weather", weather))
    return [
        {
            "forecast": [
                {
                    "details": details,
                }
            ],
            "raw": raw,
        }
    ]


def test_below_freezing_alone_does_not_trigger():
    taf = _build_taf(temp=-5)
    metar = [{"temperature": -2, "dewpoint": -10, "wind_speed": 12}]

    assessment = evaluate_hangar_need(taf, metar)

    assert assessment["triggers"] == []
    assert assessment["needs_hangar"] is False


def test_frost_requires_multiple_indicators():
    taf = _build_taf(temp=-4)
    metar = [{"temperature": -1, "dewpoint": -3, "wind_speed": 4}]

    assessment = evaluate_hangar_need(taf, metar)

    assert any("overnight frost" in trigger for trigger in assessment["triggers"])
    assert assessment["needs_hangar"] is True


def test_temperature_below_twenty_triggers_unconditionally():
    taf = _build_taf(temp=-22)
    assessment = evaluate_hangar_need(taf, [])

    assert any("-20" in trigger for trigger in assessment["triggers"])


def test_cj_client_departure_threshold():
    taf = _build_taf(temp=-12)
    metar = [{"temperature": -8, "dewpoint": -9, "wind_speed": 8}]

    assessment = evaluate_hangar_need(
        taf,
        metar,
        aircraft_category="CJ",
        client_departure=True,
    )

    assert any("CJ" in trigger for trigger in assessment["triggers"])


def test_cj_non_client_departure_does_not_trigger():
    taf = _build_taf(temp=-12)
    assessment = evaluate_hangar_need(
        taf,
        [],
        aircraft_category="CJ",
        client_departure=False,
    )

    assert all("CJ" not in trigger for trigger in assessment["triggers"])


def test_legacy_threshold():
    taf = _build_taf(temp=-16)
    assessment = evaluate_hangar_need(
        taf,
        [],
        aircraft_category="Legacy",
        client_departure=False,
    )

    assert any("Legacy/Praetor" in trigger for trigger in assessment["triggers"])


@pytest.mark.parametrize(
    "row,expected",
    [
        ({"assignedAircraftType": "Praetor 500"}, "PRAETOR"),
        ({"ownerClass": "Legacy Owner"}, "LEGACY"),
        ({"tail": "C-FSNY"}, "CJ"),
        (
            {
                "tail": "C-GASW",
                "workflowCustomName": "Legacy Ops",
            },
            "LEGACY",
        ),
    ],
)
def test_identify_aircraft_category(row, expected):
    assert identify_aircraft_category(row) == expected


@pytest.mark.parametrize(
    "row,expected",
    [
        ({"ownerClass": "Client Flight"}, True),
        ({"ownerClass": "CJ Owner"}, False),
        ({"flightType": "Charter Pax"}, True),
        ({"accountName": "Charter Client"}, True),
    ],
)
def test_is_client_departure(row, expected):
    assert is_client_departure(row) is expected

