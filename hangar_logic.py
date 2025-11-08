"""Core evaluation helpers for the hangar recommendation tool.

This module exposes pure functions so that the Streamlit page can remain
focused on presentation concerns while the decision making can be unit tested.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, MutableSequence


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
        if tail_normalized.startswith("C-FS") or tail_normalized.startswith("C-GFS"):
            return "CJ"
        if tail_normalized.startswith("C-GA") or tail_normalized.startswith("C-FNA"):
            # Legacy and Praetor tails share the C-GA/C-FNA prefix family.
            # Fall back to checking for distinguishing keywords when available.
            for text in _iter_text_candidates(row, "workflowCustomName", "workflow_custom_name"):
                if "PRAETOR" in text:
                    return "PRAETOR"
                if "LEGACY" in text:
                    return "LEGACY"

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

    if not taf_data:
        assessment["notes"].append(
            "No TAF data available — unable to evaluate local weather risks."
        )
    else:
        segments = taf_data[0].get("forecast", [])
        temp_min = parse_temp_from_taf(segments)
        wx_codes = parse_weather_codes(segments)
        assessment["min_temp"] = temp_min

        if temp_min is None:
            assessment["notes"].append(
                "Forecast minimum temperature unavailable in TAF."
            )
        else:
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

    frost_codes = set(wx_codes)
    if not frost_codes and taf_data:
        raw_text = _normalize_text(taf_data[0].get("raw"))
        if raw_text and "FROST" in raw_text:
            frost_codes.add("FROST")

    if wx_codes or frost_codes:
        assessment["notes"].append(
            "Weather codes in primary forecast window: "
            + ", ".join(sorted(frost_codes or set(wx_codes)))
        )
    elif taf_data:
        assessment["notes"].append(
            "No significant weather codes in the primary TAF segment."
        )

    triggers: list[str] = assessment["triggers"]

    if temp_for_thresholds is not None:
        if temp_for_thresholds <= -20:
            triggers.append("Temperature at or below -20°C — hangar required")
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

    frost_conditions: list[str] = []
    temp_below_freezing = False
    if temp_for_thresholds is not None and temp_for_thresholds < 0:
        temp_below_freezing = True
        frost_conditions.append("sub-zero temperatures")
    if metar_wind is not None and metar_wind < 10:
        frost_conditions.append(f"light surface winds ({metar_wind:.0f} kt)")
    if metar_temp is not None and metar_dewpoint is not None:
        spread = abs(metar_temp - metar_dewpoint)
        if spread <= 5:
            frost_conditions.append(f"temp/dewpoint spread {spread:.0f}°C")
    frost_forecasted = any(code in {"FROST", "FZFG", "FZRA", "FZDZ"} for code in frost_codes)
    if frost_forecasted:
        frost_conditions.append("frost/freezing code in forecast")

    if len(frost_conditions) >= 3 and temp_below_freezing:
        triggers.append(
            "Risk of overnight frost — " + _format_conditions(frost_conditions)
        )
    elif temp_below_freezing and frost_conditions:
        assessment["notes"].append(
            "Partial frost indicators: " + _format_conditions(frost_conditions)
        )

    if any(code.startswith("FZ") for code in frost_codes):
        triggers.append(
            "Freezing precipitation expected (FZ prefix codes present)"
        )

    if any(code in frost_codes for code in ["TS", "GR", "GS"]):
        triggers.append("Thunderstorm or hail risk indicated in TAF")

    if metar_wind is not None and metar_wind <= 5:
        assessment["notes"].append(
            f"Current surface winds {metar_wind:.0f} kt — conducive to radiational cooling."
        )

    if not triggers:
        assessment["notes"].append(
            "No hangar-triggering conditions detected in current forecast."
        )

    assessment["needs_hangar"] = bool(triggers)
    return assessment

