"""Core evaluation helpers for the hangar recommendation tool.

This module exposes pure functions so that the Streamlit page can remain
focused on presentation concerns while the decision making can be unit tested.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, MutableSequence, Sequence


EMBRAER_TAILS = {
    "CGASL",
    "CFASV",
    "CFLAS",
    "CFJAS",
    "CFASF",
    "CGASE",
    "CGASK",
    "CGXAS",
    "CGBAS",
    "CFSNY",
    "CFSYX",
    "CFSBR",
    "CFSRX",
    "CFSJR",
    "CFASQ",
    "CFSDO",
}
"""Tail numbers (without hyphen) that correspond to Embraer aircraft."""

CJ_TAILS = {
    "CFASP",
    "CFASR",
    "CFASW",
    "CFIAS",
    "CGASR",
    "CGZAS",
    "CFASY",
    "CGASW",
    "CGAAS",
    "CFNAS",
    "CGNAS",
    "CGFFS",
    "CFSFS",
    "CGFSX",
    "CFSFO",
    "CFSNP",
    "CFSQX",
    "CFSFP",
    "CFSEF",
    "CFSDN",
    "CGFSD",
    "CFSUP",
    "CFSRY",
    "CGFSJ",
}
"""Tail numbers (without hyphen) that correspond to CJ aircraft."""


def _normalize_text(value: Any) -> str | None:
    """Return an upper-cased string or ``None`` when the value is empty."""

    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
    else:
        text = str(value).strip()
    if not text:
        return None
    return text.upper()


def _iter_text_candidates(source: Mapping[str, Any], *keys: str) -> Iterable[str]:
    for key in keys:
        if key in source:
            normalized = _normalize_text(source.get(key))
            if normalized:
                yield normalized


def identify_aircraft_category(row: Mapping[str, Any] | None) -> str | None:
    """Derive the broad aircraft category (CJ, Legacy, Praetor) from a row."""

    if row is None:
        return None

    for text in _iter_text_candidates(
        row,
        "assignedAircraftType",
        "assigned_aircraft_type",
        "aircraftCategory",
        "aircraft_category",
        "aircraftType",
        "aircraft_type",
        "aircraftTypeAssigned",
    ):
        if "PRAETOR" in text:
            return "PRAETOR"
        if "LEGACY" in text:
            return "LEGACY"
        if "CJ" in text:
            return "CJ"

    for text in _iter_text_candidates(row, "ownerClass", "owner_class", "ownerClassification"):
        if "PRAETOR" in text:
            return "PRAETOR"
        if "LEGACY" in text:
            return "LEGACY"
        if "CJ" in text:
            return "CJ"

    tail = row.get("tail")
    tail_normalized = _normalize_text(tail)
    if tail_normalized:
        tail_compact = tail_normalized.replace("-", "")
        if tail_compact in EMBRAER_TAILS:
            return "LEGACY"
        if tail_normalized.startswith("C-GA") or tail_normalized.startswith("C-FNA"):
            # Legacy and Praetor tails share the C-GA/C-FNA prefix family.
            # Fall back to checking for distinguishing keywords when available.
            for text in _iter_text_candidates(row, "workflowCustomName", "workflow_custom_name"):
                if "PRAETOR" in text:
                    return "PRAETOR"
                if "LEGACY" in text:
                    return "LEGACY"
        if tail_compact in CJ_TAILS:
            return "CJ"
        if tail_normalized.startswith("C-FS") or tail_normalized.startswith("C-GFS"):
            return "CJ"

    return None


def is_client_departure(row: Mapping[str, Any] | None) -> bool:
    """Best-effort heuristic to determine if a leg is client-occupied."""

    if row is None:
        return False

    owner_keywords = ("CLIENT", "CHARTER")
    owner_exclusions = ("OWNER",)

    for text in _iter_text_candidates(
        row,
        "ownerClass",
        "owner_class",
        "ownerClassification",
        "ownerType",
        "ownerTypeName",
        "ownerClassName",
        "aircraftOwnerClass",
    ):
        if any(keyword in text for keyword in owner_keywords):
            return True
        if any(exclusion in text for exclusion in owner_exclusions):
            return False

    for text in _iter_text_candidates(row, "flightType", "flight_type"):
        if any(keyword in text for keyword in owner_keywords + ("PAX", "PASSENGER")):
            return True
        if any(exclusion in text for exclusion in owner_exclusions):
            return False

    for text in _iter_text_candidates(row, "accountName", "account", "client", "clientName"):
        if any(keyword in text for keyword in owner_keywords):
            return True

    return False


def parse_temp_from_taf(taf_segments: Iterable[Mapping[str, Any]]) -> float | None:
    temps: list[float] = []
    for seg in taf_segments:
        details = seg.get("details", []) if isinstance(seg, Mapping) else []
        for entry in details:
            if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                continue
            label, value = entry
            label_text = _normalize_text(label)
            if not label_text:
                continue
            if "TEMP" in label_text:
                try:
                    temps.append(float(value))
                except (TypeError, ValueError):
                    continue
    return min(temps) if temps else None


def parse_weather_codes(taf_segments: Iterable[Mapping[str, Any]]) -> list[str]:
    wx_codes: list[str] = []
    for seg in taf_segments:
        details = seg.get("details", []) if isinstance(seg, Mapping) else []
        for entry in details:
            if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                continue
            label, value = entry
            if _normalize_text(label) == "WEATHER" and isinstance(value, str):
                wx_codes.extend(part.strip().upper() for part in value.split(",") if part.strip())
    return wx_codes


def _split_weather_string(text: str | None) -> list[str]:
    if not text:
        return []
    parts: list[str] = []
    for token in text.replace(",", " ").split():
        normalized = token.strip().upper()
        if normalized:
            parts.append(normalized)
    return parts


def _collect_metar_weather_codes(metar_data: Iterable[Mapping[str, Any]]) -> set[str]:
    codes: set[str] = set()
    for report in metar_data or []:
        metar_props = report.get("metar_data")
        if isinstance(metar_props, Mapping):
            for key in ("wxString", "wx_string", "weather", "weather_string"):
                value = metar_props.get(key)
                if isinstance(value, str):
                    codes.update(_split_weather_string(value))
            present_weather = metar_props.get("presentWeather")
            if isinstance(present_weather, Sequence):
                for entry in present_weather:
                    if isinstance(entry, Mapping):
                        descriptor = (
                            (entry.get("intensity") or "")
                            + (entry.get("descriptor") or "")
                            + (entry.get("phenomenon") or "")
                        )
                        normalized = _normalize_text(descriptor)
                        if normalized:
                            codes.add(normalized)
    return codes


def _extract_metar_sky_layers(metar_data: Iterable[Mapping[str, Any]]) -> list[str]:
    for report in metar_data or []:
        metar_props = report.get("metar_data")
        if not isinstance(metar_props, Mapping):
            continue
        sky = metar_props.get("skyCondition") or metar_props.get("sky_condition")
        layers: list[str] = []
        if isinstance(sky, Sequence):
            for layer in sky:
                if isinstance(layer, Mapping):
                    cover = layer.get("skyCover") or layer.get("cover")
                    normalized = _normalize_text(cover)
                    if normalized:
                        layers.append(normalized)
                elif isinstance(layer, str):
                    normalized = _normalize_text(layer)
                    if normalized:
                        layers.append(normalized)
        elif isinstance(sky, str):
            normalized = _normalize_text(sky)
            if normalized:
                layers.append(normalized)
        if layers:
            return layers
    return []


def _is_mostly_clear_sky(layers: Sequence[str]) -> bool:
    if not layers:
        return False
    acceptable = {"SKC", "CLR", "FEW", "SCT", "NSC"}
    return all(layer in acceptable for layer in layers)


def _parse_weather_code(code: str) -> dict[str, Any]:
    normalized = _normalize_text(code)
    if not normalized:
        return {"code": "", "descriptors": set(), "core": "", "heavy": False}
    heavy = normalized.startswith("+")
    stripped = normalized.lstrip("+-")
    descriptors: list[str] = []
    prefixes = ("VC", "MI", "PR", "BC", "DR", "BL", "SH", "TS", "FZ")
    changed = True
    while changed and stripped:
        changed = False
        for prefix in prefixes:
            if stripped.startswith(prefix):
                descriptors.append(prefix)
                stripped = stripped[len(prefix) :]
                changed = True
                break
    return {
        "code": normalized,
        "descriptors": set(descriptors),
        "core": stripped,
        "heavy": heavy,
    }


def _parse_wind_from_taf(taf_segments: Iterable[Mapping[str, Any]]) -> tuple[float | None, float | None]:
    max_wind: float | None = None
    max_gust: float | None = None
    for seg in taf_segments:
        details = seg.get("details", []) if isinstance(seg, Mapping) else []
        for entry in details:
            if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                continue
            label, value = entry
            normalized = _normalize_text(label)
            if not normalized:
                continue
            if normalized == "WIND SPEED (KT)":
                try:
                    speed = float(value)
                except (TypeError, ValueError):
                    continue
                max_wind = speed if max_wind is None else max(max_wind, speed)
            elif normalized == "WIND GUST (KT)":
                try:
                    gust = float(value)
                except (TypeError, ValueError):
                    continue
                max_gust = gust if max_gust is None else max(max_gust, gust)
    return max_wind, max_gust


def extract_metar_value(metar_data: Iterable[Mapping[str, Any]], key: str) -> float | None:
    for report in metar_data or []:
        value = report.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _format_conditions(descriptions: MutableSequence[str]) -> str:
    if not descriptions:
        return ""
    if len(descriptions) == 1:
        return descriptions[0]
    if len(descriptions) == 2:
        return f"{descriptions[0]} and {descriptions[1]}"
    return ", ".join(descriptions[:-1]) + f", and {descriptions[-1]}"


def evaluate_hangar_need(
    taf_data: list[dict],
    metar_data: list[dict],
    *,
    aircraft_category: str | None = None,
    client_departure: bool = False,
) -> dict[str, Any]:
    assessment: dict[str, Any] = {
        "needs_hangar": False,
        "triggers": [],
        "notes": [],
        "min_temp": None,
        "metar_temp": None,
        "metar_dewpoint": None,
        "aircraft_category": aircraft_category,
        "client_departure": client_departure,
    }

    normalized_category = _normalize_text(aircraft_category)
    if normalized_category:
        assessment["notes"].append(f"Aircraft category: {normalized_category.title()}")
    if client_departure:
        assessment["notes"].append("Next leg is client-occupied.")

    metar_temp = extract_metar_value(metar_data, "temperature")
    metar_dewpoint = extract_metar_value(metar_data, "dewpoint")
    metar_wind = extract_metar_value(metar_data, "wind_speed")
    metar_gust = extract_metar_value(metar_data, "wind_gust")
    assessment["metar_temp"] = metar_temp
    assessment["metar_dewpoint"] = metar_dewpoint

    if not metar_data:
        assessment["notes"].append("No recent METAR observation retrieved.")

    if metar_temp is not None:
        assessment["notes"].append(f"Current METAR temperature: {metar_temp:.0f}°C")
    if metar_dewpoint is not None:
        if metar_temp is not None:
            spread = metar_temp - metar_dewpoint
            assessment["notes"].append(
                f"Current dewpoint: {metar_dewpoint:.0f}°C (spread {spread:.0f}°C)"
            )
        else:
            assessment["notes"].append(
                f"Current dewpoint from METAR: {metar_dewpoint:.0f}°C"
            )

    temp_min: float | None = None
    temp_for_thresholds: float | None = None
    wx_codes: list[str] = []
    taf_wind: float | None = None
    taf_gust: float | None = None

    if not taf_data:
        assessment["notes"].append(
            "No TAF data available — unable to evaluate local weather risks."
        )
    else:
        segments = taf_data[0].get("forecast", [])
        temp_min = parse_temp_from_taf(segments)
        wx_codes = parse_weather_codes(segments)
        taf_wind, taf_gust = _parse_wind_from_taf(segments)
        assessment["min_temp"] = temp_min

        if temp_min is not None:
            assessment["notes"].append(
                f"Forecast minimum temperature: {temp_min:.0f}°C"
            )
            temp_for_thresholds = temp_min

    if temp_min is None and metar_temp is not None:
        estimated_min = metar_temp - 3
        temp_for_thresholds = estimated_min
        assessment["notes"].append(
            f"Estimating overnight low near {estimated_min:.0f}°C based on current METAR trend."
        )

    forecast_codes = set(wx_codes)
    if not forecast_codes and taf_data:
        raw_text = _normalize_text(taf_data[0].get("raw"))
        if raw_text and "FROST" in raw_text:
            forecast_codes.add("FROST")

    metar_weather_codes = _collect_metar_weather_codes(metar_data)

    if wx_codes or forecast_codes:
        assessment["notes"].append(
            "Weather codes in primary forecast window: "
            + ", ".join(sorted(forecast_codes or set(wx_codes)))
        )
    elif taf_data:
        assessment["notes"].append(
            "No significant weather codes in the primary TAF segment."
        )

    if metar_weather_codes:
        assessment["notes"].append(
            "Current METAR weather codes: " + ", ".join(sorted(metar_weather_codes))
        )

    combined_weather_codes = forecast_codes | metar_weather_codes
    parsed_combined_codes = [_parse_weather_code(code) for code in combined_weather_codes]
    parsed_metar_codes = [_parse_weather_code(code) for code in metar_weather_codes]

    triggers: list[str] = assessment["triggers"]

    cold_triggered = False
    if temp_for_thresholds is not None:
        if temp_for_thresholds <= -25:
            triggers.append("Forecast temperature at or below -25°C — extreme cold risk")
            cold_triggered = True
        elif temp_for_thresholds <= -20:
            triggers.append("Temperature at or below -20°C — hangar required")
            cold_triggered = True
        else:
            if normalized_category in {"LEGACY", "PRAETOR"} and temp_for_thresholds <= -15:
                triggers.append(
                    "Legacy/Praetor overnight with forecast ≤ -15°C"
                )
            if (
                normalized_category == "CJ"
                and client_departure
                and temp_for_thresholds <= -10
            ):
                triggers.append(
                    "Client departure on CJ with forecast ≤ -10°C"
                )

    if not cold_triggered and metar_temp is not None and metar_temp <= -20:
        triggers.append("Current METAR temperature ≤ -20°C — hangar required")
        cold_triggered = True

    frost_conditions: list[str] = []
    frost_temp_indicator = False
    if temp_for_thresholds is not None and temp_for_thresholds <= 1:
        frost_temp_indicator = True
        if temp_for_thresholds < 0:
            frost_conditions.append("sub-zero temperatures expected")
        else:
            frost_conditions.append("overnight low near freezing")
    elif metar_temp is not None and metar_temp <= 3:
        frost_temp_indicator = True
        frost_conditions.append(f"current temp {metar_temp:.0f}°C")

    if metar_wind is not None and metar_wind <= 8:
        frost_conditions.append(f"light surface winds ({metar_wind:.0f} kt)")
    if metar_temp is not None and metar_dewpoint is not None:
        spread = abs(metar_temp - metar_dewpoint)
        if spread <= 3:
            frost_conditions.append(f"temp/dewpoint spread {spread:.0f}°C")
    if _is_mostly_clear_sky(_extract_metar_sky_layers(metar_data)):
        frost_conditions.append("clear or mostly clear skies")
    frost_forecasted = any(
        entry["code"] in {"FROST"}
        or entry["core"] in {"FZFG"}
        or "FZ" in entry["descriptors"]
        for entry in parsed_combined_codes
    )
    if frost_forecasted:
        frost_conditions.append("frost/freezing mention in forecast")

    if frost_temp_indicator and len(frost_conditions) >= 3:
        triggers.append(
            "Risk of overnight frost — " + _format_conditions(frost_conditions)
        )
    elif frost_temp_indicator and frost_conditions:
        assessment["notes"].append(
            "Partial frost indicators: " + _format_conditions(frost_conditions)
        )

    freezing_precip_codes = [
        entry["code"]
        for entry in parsed_combined_codes
        if (
            ("FZ" in entry["descriptors"] and any(token in entry["core"] for token in ("RA", "DZ", "SN")))
            or entry["core"].endswith("PL")
        )
    ]
    if freezing_precip_codes:
        triggers.append(
            "Freezing precipitation expected (" + ", ".join(sorted(freezing_precip_codes)) + ")"
        )

    snow_codes = [
        entry
        for entry in parsed_combined_codes
        if "SN" in entry["core"] and "BL" not in entry["descriptors"]
    ]
    if snow_codes:
        if any(entry["heavy"] for entry in snow_codes):
            triggers.append("Heavy snow or significant accumulation expected")
        else:
            triggers.append("Snow or wintry precipitation in the hangar window")

    fog_entries = [
        entry
        for entry in parsed_combined_codes
        if entry["core"].endswith("FG") or entry["core"] == "BR"
    ]
    if fog_entries:
        freezing_fog = any("FZ" in entry["descriptors"] for entry in fog_entries)
        freezing_fog = freezing_fog or (
            (metar_temp is not None and metar_temp <= 0)
            or (temp_for_thresholds is not None and temp_for_thresholds <= 0)
        )
        if freezing_fog:
            triggers.append("Freezing fog or fog with sub-zero temperatures expected")
        else:
            triggers.append("Dense fog expected at the aerodrome")

    blowing_snow_entries = [
        entry for entry in parsed_combined_codes if "BL" in entry["descriptors"] and "SN" in entry["core"]
    ]
    cold_for_blowing_snow = any(
        temp is not None and temp <= -20 for temp in (metar_temp, temp_for_thresholds)
    )
    if blowing_snow_entries and cold_for_blowing_snow:
        triggers.append("Blowing snow with extreme cold expected")

    ice_crystal_entries = [entry for entry in parsed_combined_codes if entry["core"] == "IC"]
    if ice_crystal_entries and cold_for_blowing_snow:
        triggers.append("Ice crystals present with extreme cold")

    thunder_entries = [entry for entry in parsed_combined_codes if "TS" in entry["descriptors"]]
    hail_entries = [
        entry
        for entry in parsed_combined_codes
        if any(token in entry["core"] for token in ("GR", "GS"))
    ]
    if thunder_entries:
        triggers.append("Thunderstorms expected over the aerodrome")
    if hail_entries:
        triggers.append("Hail indicated in forecast/observations")

    wind_alerted = False
    if metar_wind is not None and metar_wind >= 30:
        triggers.append(f"Observed sustained winds ≥30 kt ({metar_wind:.0f} kt)")
        wind_alerted = True
    if metar_gust is not None and metar_gust >= 40:
        triggers.append(f"Observed gusts ≥40 kt (G{metar_gust:.0f})")
        wind_alerted = True
    if taf_wind is not None and taf_wind >= 30:
        triggers.append(f"Forecast sustained winds ≥30 kt ({taf_wind:.0f} kt)")
        wind_alerted = True
    if taf_gust is not None and taf_gust >= 40:
        triggers.append(f"Forecast gusts ≥40 kt (G{taf_gust:.0f})")
        wind_alerted = True

    if not wind_alerted and metar_wind is not None and metar_wind <= 5:
        assessment["notes"].append(
            f"Current surface winds {metar_wind:.0f} kt — conducive to radiational cooling."
        )

    recent_wet_precip = False
    if metar_temp is not None and metar_temp >= 0 and parsed_metar_codes:
        for entry in parsed_metar_codes:
            if any(token in entry["core"] for token in ("RA", "DZ", "SN")):
                recent_wet_precip = True
                break
    if (
        recent_wet_precip
        and temp_for_thresholds is not None
        and temp_for_thresholds <= -2
    ):
        triggers.append(
            "Recent wet precip with sub-zero overnight forecast — risk of refreeze"
        )

    if not triggers:
        assessment["notes"].append(
            "No hangar-triggering conditions detected in current forecast."
        )

    assessment["needs_hangar"] = bool(triggers)
    return assessment

