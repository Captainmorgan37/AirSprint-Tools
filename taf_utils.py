"""Utilities for retrieving and normalising TAF forecasts."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, MutableMapping, Sequence, Tuple

import json
import requests


TAF_FORECAST_FIELDS = [
    (("windDir", "wind_direction", "wind_dir"), "Wind Dir (°)"),
    (("windSpeed", "wind_speed", "windSpd"), "Wind Speed (kt)"),
    (("windGust", "wind_gust", "windGustKt"), "Wind Gust (kt)"),
    (("visibility", "visibilitySM", "visibility_sm", "visibility_mi"), "Visibility"),
    (("probability", "probabilityPercent", "probability_percent"), "Probability (%)"),
    (("icing",), "Icing"),
    (("turbulence",), "Turbulence"),
]


def format_iso_timestamp(value: Any) -> Tuple[str, datetime | None]:
    """Return a human-readable timestamp and a timezone-aware datetime."""

    if value in (None, "", []):
        return "N/A", None

    def _format(dt_obj: datetime) -> Tuple[str, datetime]:
        dt_utc = dt_obj.astimezone(timezone.utc)
        return dt_utc.strftime("%b %d %Y, %H:%MZ"), dt_utc

    if isinstance(value, (int, float)):
        try:
            seconds = float(value)
        except (TypeError, ValueError):
            return str(value), None
        if seconds > 1e12:
            seconds /= 1000.0
        dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
        return _format(dt)

    value_str = str(value).strip()
    if not value_str:
        return "N/A", None

    if value_str.isdigit():
        try:
            seconds = int(value_str)
        except ValueError:
            seconds = None
        if seconds is not None:
            if len(value_str) > 10:
                seconds /= 1000.0
            dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
            return _format(dt)

    try:
        dt = datetime.fromisoformat(value_str.replace("Z", "+00:00"))
    except ValueError:
        return value_str, None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return _format(dt)


def _simplify_detail_value(value: Any) -> Any:
    if isinstance(value, MutableMapping):
        preferred_keys = ("repr", "text", "raw", "string")
        numeric_keys = ("value", "visibility", "minValue", "maxValue")
        for key in preferred_keys:
            if key in value:
                simplified = _simplify_detail_value(value[key])
                if simplified not in (None, "", []):
                    return simplified
        for key in numeric_keys:
            if key in value:
                simplified = _simplify_detail_value(value[key])
                if simplified not in (None, "", []):
                    return simplified
        return json.dumps(value)
    if isinstance(value, (list, tuple)):
        parts = [
            str(_simplify_detail_value(item))
            for item in value
            if _simplify_detail_value(item) not in (None, "", [])
        ]
        return ", ".join(parts) if parts else None
    return value


def build_detail_list(data_dict: Any, field_map: Iterable[Tuple[Iterable[str], str]]) -> List[Tuple[str, Any]]:
    if not isinstance(data_dict, MutableMapping):
        return []

    details: List[Tuple[str, Any]] = []
    for key_spec, label in field_map:
        value = None
        for key in key_spec:
            if key in data_dict and data_dict[key] not in (None, "", []):
                value = data_dict[key]
                break
        if value in (None, "", []):
            continue
        simplified = _simplify_detail_value(value)
        if simplified in (None, "", []):
            continue
        details.append((label, simplified))
    return details


def _normalize_aviationweather_features(data: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(data, MutableMapping):
        for key in ("features", "data", "items", "reports"):
            if key in data and isinstance(data[key], Iterable):
                for item in data[key]:
                    if isinstance(item, MutableMapping):
                        props = item.get("properties")
                        if isinstance(props, MutableMapping):
                            yield props  # type: ignore[misc]
                        elif isinstance(item, MutableMapping):
                            yield item  # type: ignore[misc]
                return
        yield data  # type: ignore[misc]
    elif isinstance(data, Iterable):
        for item in data:
            if isinstance(item, MutableMapping):
                props = item.get("properties") if isinstance(item, MutableMapping) else None
                if isinstance(props, MutableMapping):
                    yield props  # type: ignore[misc]
                else:
                    yield item  # type: ignore[misc]


def get_taf_reports(icao_codes: Sequence[str]) -> Dict[str, List[Dict[str, Any]]]:
    if not icao_codes:
        return {}

    url = "https://aviationweather.gov/api/data/taf"
    params = {
        "ids": ",".join(sorted({code.upper() for code in icao_codes if code})),
        "format": "json",
        "mostRecent": "true",
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
    except requests.HTTPError as exc:
        status_code = getattr(exc.response, "status_code", None)
        if status_code == 400:
            fallback_params = {"ids": params["ids"], "format": "json"}
            response = requests.get(url, params=fallback_params, timeout=10)
            response.raise_for_status()
        else:
            raise

    data = response.json()
    taf_reports: Dict[str, List[Dict[str, Any]]] = {}

    for props in _normalize_aviationweather_features(data):
        station = (
            props.get("station")
            or props.get("stationId")
            or props.get("icaoId")
            or props.get("icao_id")
            or ""
        )
        station = str(station).upper()
        if not station:
            continue

        issue_display, issue_dt = format_iso_timestamp(
            props.get("issueTime")
            or props.get("issue_time")
            or props.get("obsTime")
            or props.get("obs_time")
            or props.get("bulletinTime")
        )
        valid_from_display, valid_from_dt = format_iso_timestamp(
            props.get("validTimeFrom") or props.get("valid_time_from")
        )
        valid_to_display, valid_to_dt = format_iso_timestamp(
            props.get("validTimeTo") or props.get("valid_time_to")
        )
        raw_text = (
            props.get("rawTAF")
            or props.get("rawText")
            or props.get("raw")
            or props.get("raw_text")
            or ""
        )

        forecast_source = (
            props.get("forecast")
            or props.get("forecastList")
            or props.get("periods")
            or []
        )
        if isinstance(forecast_source, MutableMapping):
            nested: List[Any] = []
            for key in ("data", "period", "periods", "forecast"):
                value = forecast_source.get(key)
                if isinstance(value, list):
                    nested.extend(value)
            if not nested:
                nested.extend(
                    value for value in forecast_source.values() if isinstance(value, MutableMapping)
                )
            forecast_source = nested
        if not isinstance(forecast_source, list):
            forecast_source = []

        forecast_periods: List[Dict[str, Any]] = []
        for fc in forecast_source:
            if not isinstance(fc, MutableMapping):
                continue
            fc_from_display, fc_from_dt = format_iso_timestamp(
                fc.get("fcstTimeFrom") or fc.get("timeFrom") or fc.get("time_from")
            )
            fc_to_display, fc_to_dt = format_iso_timestamp(
                fc.get("fcstTimeTo") or fc.get("timeTo") or fc.get("time_to")
            )
            fc_details = build_detail_list(fc, TAF_FORECAST_FIELDS)

            wx = fc.get("wxString") or fc.get("weather") or fc.get("wx_string")
            if wx:
                if isinstance(wx, list):
                    wx = ", ".join(str(v) for v in wx if v not in (None, ""))
                fc_details.append(("Weather", wx))

            clouds = (
                fc.get("clouds")
                or fc.get("cloudList")
                or fc.get("skyCondition")
                or fc.get("sky_condition")
            )
            if isinstance(clouds, list):
                cloud_parts: List[str] = []
                for cloud in clouds:
                    if not isinstance(cloud, MutableMapping):
                        continue
                    cover = (
                        cloud.get("cover")
                        or cloud.get("cloudCover")
                        or cloud.get("cloud_cover")
                        or cloud.get("skyCover")
                    )
                    base = (
                        cloud.get("base")
                        or cloud.get("base_feet")
                        or cloud.get("cloudBaseFT")
                        or cloud.get("cloudBaseFt")
                        or cloud.get("baseFeetAGL")
                        or cloud.get("base_feet_agl")
                    )
                    if cover and base:
                        cloud_parts.append(f"{cover} {base}ft")
                    elif cover:
                        cloud_parts.append(str(cover))
                if cloud_parts:
                    fc_details.append(("Clouds", ", ".join(cloud_parts)))

            forecast_periods.append(
                {
                    "from_display": fc_from_display,
                    "from_time": fc_from_dt,
                    "to_display": fc_to_display,
                    "to_time": fc_to_dt,
                    "details": fc_details,
                }
            )

        taf_reports.setdefault(station, []).append(
            {
                "station": station,
                "raw": raw_text,
                "issue_time_display": issue_display,
                "issue_time": issue_dt,
                "valid_from_display": valid_from_display,
                "valid_from": valid_from_dt,
                "valid_to_display": valid_to_display,
                "valid_to": valid_to_dt,
                "forecast": forecast_periods,
            }
        )

    return taf_reports


__all__ = ["TAF_FORECAST_FIELDS", "build_detail_list", "format_iso_timestamp", "get_taf_reports"]
