import pytest

from hangar_logic import (
    evaluate_hangar_need,
    identify_aircraft_category,
    is_client_departure,
)


def _build_taf(
    temp: float | None = None,
    weather: str | None = None,
    raw: str = "",
    wind_speed: float | None = None,
    wind_gust: float | None = None,
):
    details = []
    if temp is not None:
        details.append(("Temperature", str(temp)))
    if weather:
        details.append(("Weather", weather))
    if wind_speed is not None:
        details.append(("Wind Speed (kt)", str(wind_speed)))
    if wind_gust is not None:
        details.append(("Wind Gust (kt)", str(wind_gust)))
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
        ({"tail": "C-GASL"}, "LEGACY"),
        ({"tail": "CFSDO"}, "LEGACY"),
        ({"tail": "C-FSNY"}, "LEGACY"),
        ({"tail": "C-FASP"}, "CJ"),
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


def test_snow_weather_code_triggers_hangar():
    taf = _build_taf(temp=-2, weather="-SN")
    assessment = evaluate_hangar_need(taf, [])

    assert any("Snow" in trigger for trigger in assessment["triggers"])


def test_freezing_fog_detected_from_metar():
    taf = _build_taf(temp=-1)
    metar = [
        {
            "temperature": -1,
            "dewpoint": -1,
            "wind_speed": 2,
            "metar_data": {"wxString": "FG"},
        }
    ]

    assessment = evaluate_hangar_need(taf, metar)

    assert any("Freezing fog" in trigger for trigger in assessment["triggers"])


def test_warm_fog_does_not_trigger_hangar():
    taf = _build_taf(temp=6, weather="FG")
    metar = [
        {
            "temperature": 14,
            "dewpoint": 12,
            "wind_speed": 10,
            "metar_data": {"wxString": "FG"},
        }
    ]

    assessment = evaluate_hangar_need(taf, metar)

    assert all("fog" not in trigger.lower() for trigger in assessment["triggers"])
    assert any("Fog expected but temperatures remain above freezing" in note for note in assessment["notes"])


def test_strong_winds_from_metar_trigger():
    metar = [
        {
            "temperature": -5,
            "dewpoint": -7,
            "wind_speed": 32,
        }
    ]

    assessment = evaluate_hangar_need([], metar)

    assert any("sustained winds" in trigger for trigger in assessment["triggers"])


def test_wet_then_freeze_scenario():
    taf = _build_taf(temp=-5)
    metar = [
        {
            "temperature": 2,
            "dewpoint": 1,
            "wind_speed": 6,
            "metar_data": {"wxString": "-RA"},
        }
    ]

    assessment = evaluate_hangar_need(taf, metar)

    assert any("refreeze" in trigger for trigger in assessment["triggers"])

