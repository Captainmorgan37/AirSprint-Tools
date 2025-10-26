"""Utilities for retrieving and normalising TAF forecasts."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, MutableMapping, Sequence, Tuple

import json
import re
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

FORECAST_CONTAINER_KEYS: Tuple[str, ...] = (
    "data",
    "period",
    "periods",
    "forecast",
    "forecastList",
    "items",
    "segments",
)

FORECAST_RELEVANT_KEYS: Tuple[str, ...] = (
    "fcstTimeFrom",
    "fcstTimeTo",
    "timeFrom",
    "timeTo",
    "time_from",
    "time_to",
    "startTime",
    "endTime",
    "start_time",
    "end_time",
    "start",
    "end",
    "wxString",
    "wx_string",
    "weather",
    "windDir",
    "wind_direction",
    "wind_dir",
    "wind",
    "windSpeed",
    "wind_speed",
    "windSpd",
    "windGust",
    "wind_gust",
    "visibility",
    "visibilitySM",
    "visibility_sm",
    "visibility_mi",
    "clouds",
    "cloudList",
    "skyCondition",
    "sky_condition",
)

TIME_FROM_FIELDS: Tuple[str, ...] = (
    "fcstTimeFrom",
    "timeFrom",
    "time_from",
    "startTime",
    "start_time",
    "from",
    "start",
)

TIME_TO_FIELDS: Tuple[str, ...] = (
    "fcstTimeTo",
    "timeTo",
    "time_to",
    "endTime",
    "end_time",
    "to",
    "end",
)


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


def _iter_forecast_candidates(value: Any) -> Iterable[MutableMapping[str, Any]]:
    """Yield potential forecast period dictionaries from varied structures."""

    def _walk(item: Any) -> Iterable[Any]:
        if item in (None, "", []):
            return
        if isinstance(item, str):
            text = item.strip()
            if not text:
                return
            try:
                parsed = json.loads(text)
            except (TypeError, ValueError):
                return
            yield from _walk(parsed)
            return
        if isinstance(item, MutableMapping):
            yield item
            handled_ids: set[int] = set()
            for key in FORECAST_CONTAINER_KEYS:
                if key in item:
                    child = item[key]
                    handled_ids.add(id(child))
                    yield from _walk(child)
            for value in item.values():
                if id(value) in handled_ids:
                    continue
                if isinstance(value, (MutableMapping, list, tuple, set)):
                    yield from _walk(value)
                elif isinstance(value, str):
                    yield from _walk(value)
            return
        if isinstance(item, Iterable) and not isinstance(item, (bytes, bytearray)):
            for sub in item:
                yield from _walk(sub)

    seen: set[int] = set()
    for candidate in _walk(value):
        if not isinstance(candidate, MutableMapping):
            continue
        candidate_id = id(candidate)
        if candidate_id in seen:
            continue
        seen.add(candidate_id)
        if any(key in candidate for key in FORECAST_RELEVANT_KEYS):
            yield candidate


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


def _unwrap_time_value(value: Any) -> Any:
    """Return the most useful representation of a time-like structure."""

    if isinstance(value, MutableMapping):
        for key in ("value", "dateTime", "date_time", "iso", "iso8601", "timestamp"):
            if key in value and value[key] not in (None, "", []):
                return _unwrap_time_value(value[key])
        for key in ("repr", "text", "raw", "string"):
            if key in value and value[key] not in (None, "", []):
                return value[key]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            candidate = _unwrap_time_value(item)
            if candidate not in (None, "", []):
                return candidate
    return value


def _extract_time_field(segment: MutableMapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if key in segment and segment[key] not in (None, "", []):
            return _unwrap_time_value(segment[key])

    for container_key in ("change", "time", "period", "window", "transition"):
        nested = segment.get(container_key)
        if isinstance(nested, MutableMapping):
            value = _extract_time_field(nested, keys)
            if value not in (None, "", []):
                return _unwrap_time_value(value)
    return None


def _normalise_forecast_segment(segment: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    """Augment raw forecast data with legacy-style fields for easier rendering."""

    normalized: Dict[str, Any] = dict(segment)

    wind = normalized.get("wind")
    if isinstance(wind, MutableMapping):
        direction = _simplify_detail_value(wind.get("direction"))
        speed = _simplify_detail_value(
            wind.get("speed")
            or wind.get("speedKt")
            or wind.get("speedKts")
            or wind.get("speed_kts")
        )
        gust = _simplify_detail_value(
            wind.get("gust")
            or wind.get("gustKt")
            or wind.get("gustKts")
            or wind.get("gust_kts")
        )
        if direction not in (None, "", []):
            normalized.setdefault("windDir", direction)
        if speed not in (None, "", []):
            normalized.setdefault("windSpeed", speed)
        if gust not in (None, "", []):
            normalized.setdefault("windGust", gust)

    visibility = normalized.get("visibility") or normalized.get("vis")
    if isinstance(visibility, MutableMapping):
        vis_value = _simplify_detail_value(visibility)
        if vis_value not in (None, "", []):
            normalized.setdefault("visibility", vis_value)

    probability = normalized.get("probability")
    if isinstance(probability, MutableMapping):
        prob_value = _simplify_detail_value(probability)
        if prob_value not in (None, "", []):
            normalized.setdefault("probability", prob_value)

    for icing_key in ("icing", "icingConditions"):
        icing_value = normalized.get(icing_key)
        if isinstance(icing_value, MutableMapping):
            simplified = _simplify_detail_value(icing_value)
            if simplified not in (None, "", []):
                normalized.setdefault("icing", simplified)

    for turb_key in ("turbulence", "turbulenceConditions"):
        turb_value = normalized.get(turb_key)
        if isinstance(turb_value, MutableMapping):
            simplified = _simplify_detail_value(turb_value)
            if simplified not in (None, "", []):
                normalized.setdefault("turbulence", simplified)

    weather = normalized.get("weather")
    if isinstance(weather, Iterable) and not isinstance(weather, (str, bytes, bytearray)):
        parts: List[str] = []
        for item in weather:
            simplified = _simplify_detail_value(item)
            if simplified in (None, "", []):
                continue
            parts.append(str(simplified))
        normalized["weather"] = parts
    elif isinstance(weather, MutableMapping):
        simplified = _simplify_detail_value(weather)
        if simplified not in (None, "", []):
            normalized["weather"] = simplified

    return normalized


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


def _fallback_parse_raw_taf(
    raw_taf: str,
    issue_dt: datetime | None,
    valid_from_dt: datetime | None,
    valid_to_dt: datetime | None,
) -> List[Dict[str, Any]]:
    """Parse raw TAF text into simple forecast segments when none are provided."""

    if not (raw_taf and issue_dt and valid_from_dt and valid_to_dt):
        return []

    taf_main = raw_taf.split(" RMK")[0]
    tokens = taf_main.split()

    idx = 0
    if idx < len(tokens) and tokens[idx].startswith("TAF"):
        idx += 1
    if idx < len(tokens) and re.match(r"^[A-Z]{3,4}$", tokens[idx]):
        idx += 1
    if idx < len(tokens) and re.match(r"^\d{6}Z$", tokens[idx]):
        idx += 1
    if idx < len(tokens) and re.match(r"^\d{4}/\d{4}$", tokens[idx]):
        idx += 1

    def _parse_fm(token: str) -> datetime | None:
        match = re.match(r"FM(\d{2})(\d{2})(\d{2})", token)
        if not match or not issue_dt:
            return None
        day = int(match.group(1))
        hour = int(match.group(2))
        minute = int(match.group(3))

        year = issue_dt.year
        month = issue_dt.month
        if day < issue_dt.day - 15:
            month += 1
            if month > 12:
                month = 1
                year += 1

        return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)

    def _parse_range_ddhh_ddhh(token: str) -> tuple[datetime | None, datetime | None]:
        match = re.match(r"^(\d{2})(\d{2})/(\d{2})(\d{2})$", token)
        if not match or not issue_dt:
            return (None, None)

        start_day = int(match.group(1))
        start_hour = int(match.group(2))
        end_day = int(match.group(3))
        end_hour = int(match.group(4))

        def _mk(day: int, hour: int) -> datetime:
            year = issue_dt.year
            month = issue_dt.month
            if day < issue_dt.day - 15:
                month += 1
                if month > 12:
                    month = 1
                    year += 1
            return datetime(year, month, day, hour, 0, tzinfo=timezone.utc)

        return (_mk(start_day, start_hour), _mk(end_day, end_hour))

    def _extract_conditions_from_tokens(tokens_list: List[str]) -> List[Tuple[str, str]]:
        wind_re = re.compile(r"^(?P<dir>\d{3}|VRB)(?P<spd>\d{2,3})(G(?P<gst>\d{2,3}))?KT$")
        cloud_re = re.compile(r"^(FEW|SCT|BKN|OVC)(\d{3})([A-Z]{2,3})?$")

        wind_dir = wind_spd = wind_gust = None
        visibility = None
        weather_codes: List[str] = []
        cloud_layers: List[str] = []

        for token in tokens_list:
            wind_match = wind_re.match(token)
            if wind_match and wind_spd is None:
                wind_dir = wind_match.group("dir")
                wind_spd = wind_match.group("spd")
                wind_gust = wind_match.group("gst")
                continue

            if visibility is None and (token.endswith("SM") or token == "P6SM"):
                visibility = token
                continue

            cloud_match = cloud_re.match(token)
            if cloud_match:
                coverage = cloud_match.group(1)
                base_hundreds = int(cloud_match.group(2))
                suffix = cloud_match.group(3) or ""
                base_ft = base_hundreds * 100
                cloud_layers.append(f"{coverage} {base_ft}ft{suffix}")
                continue

            if re.match(r"^[-+A-Z]{2,}$", token) and not token.endswith("KT"):
                weather_codes.append(token)

        details: List[Tuple[str, str]] = []
        if wind_dir or wind_spd or wind_gust:
            if wind_dir:
                details.append(("Wind Dir (°)", wind_dir))
            if wind_spd:
                details.append(("Wind Speed (kt)", wind_spd))
            if wind_gust:
                details.append(("Wind Gust (kt)", wind_gust))
        if visibility:
            details.append(("Visibility", visibility))
        if weather_codes:
            details.append(("Weather", ", ".join(weather_codes)))
        if cloud_layers:
            details.append(("Clouds", ", ".join(cloud_layers)))

        return details

    segs_raw: List[Tuple[datetime, List[str], List[Dict[str, Any]]]] = []
    cur_start = valid_from_dt
    cur_tokens: List[str] = []
    cur_tempo: List[Dict[str, Any]] = []

    i = idx
    while i < len(tokens):
        token = tokens[i]

        fm_dt = _parse_fm(token)
        if fm_dt:
            if cur_start:
                segs_raw.append((cur_start, list(cur_tokens), list(cur_tempo)))
            cur_start = fm_dt
            cur_tokens = []
            cur_tempo = []
            i += 1
            continue

        if token == "TEMPO":
            i += 1
            tempo_start = tempo_end = None
            if i < len(tokens) and re.match(r"^\d{4}/\d{4}$", tokens[i]):
                tempo_start, tempo_end = _parse_range_ddhh_ddhh(tokens[i])
                i += 1

            tempo_tokens: List[str] = []
            while i < len(tokens):
                peek = tokens[i]
                if peek in ("TEMPO", "BECMG") or peek.startswith("PROB") or _parse_fm(peek):
                    break
                tempo_tokens.append(peek)
                i += 1

            tempo_details = _extract_conditions_from_tokens(tempo_tokens)
            cur_tempo.append(
                {
                    "start": tempo_start or cur_start,
                    "end": tempo_end or valid_to_dt,
                    "prob": None,
                    "details": tempo_details,
                }
            )
            continue

        if token.startswith("PROB") and re.match(r"^PROB\d{2}$", token):
            prob_token = token
            i += 1

            if i < len(tokens) and tokens[i] == "TEMPO":
                i += 1

            tempo_start = tempo_end = None
            if i < len(tokens) and re.match(r"^\d{4}/\d{4}$", tokens[i]):
                tempo_start, tempo_end = _parse_range_ddhh_ddhh(tokens[i])
                i += 1

            tempo_tokens: List[str] = []
            while i < len(tokens):
                peek = tokens[i]
                if peek in ("TEMPO", "BECMG") or peek.startswith("PROB") or _parse_fm(peek):
                    break
                tempo_tokens.append(peek)
                i += 1

            tempo_details = _extract_conditions_from_tokens(tempo_tokens)
            cur_tempo.append(
                {
                    "start": tempo_start or cur_start,
                    "end": tempo_end or valid_to_dt,
                    "prob": prob_token,
                    "details": tempo_details,
                }
            )
            continue

        if token.startswith("BECMG"):
            i += 1
            if i < len(tokens) and re.match(r"^\d{4}/\d{4}$", tokens[i]):
                i += 1
            continue

        if cur_start:
            cur_tokens.append(token)
        i += 1

    if cur_start:
        segs_raw.append((cur_start, list(cur_tokens), list(cur_tempo)))

    segments: List[Dict[str, Any]] = []

    for index, (segment_start, segment_tokens, segment_tempo) in enumerate(segs_raw):
        if index + 1 < len(segs_raw):
            segment_end = segs_raw[index + 1][0]
        else:
            segment_end = valid_to_dt

        prevailing_details = _extract_conditions_from_tokens(segment_tokens)

        tempo_blocks: List[Dict[str, Any]] = []
        for tempo_block in segment_tempo:
            block_start = tempo_block.get("start") or segment_start
            block_end = tempo_block.get("end") or segment_end
            tempo_blocks.append(
                {
                    "start": block_start,
                    "end": block_end,
                    "prob": tempo_block.get("prob"),
                    "details": tempo_block.get("details", []),
                }
            )

        segments.append(
            {
                "from_display": segment_start.strftime("%b %d %Y, %H:%MZ"),
                "from_time": segment_start,
                "to_display": segment_end.strftime("%b %d %Y, %H:%MZ") if segment_end else "N/A",
                "to_time": segment_end,
                "details": prevailing_details,
                "tempo": tempo_blocks,
            }
        )

    return segments


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

        forecast_periods: List[Dict[str, Any]] = []
        forecast_source = (
            props.get("forecast")
            or props.get("forecastList")
            or props.get("periods")
            or []
        )
        for fc in _iter_forecast_candidates(forecast_source):
            if not isinstance(fc, MutableMapping):
                continue
            fc_processed = _normalise_forecast_segment(fc)
            from_value = _extract_time_field(fc_processed, TIME_FROM_FIELDS)
            to_value = _extract_time_field(fc_processed, TIME_TO_FIELDS)
            fc_from_display, fc_from_dt = format_iso_timestamp(from_value)
            fc_to_display, fc_to_dt = format_iso_timestamp(to_value)
            fc_details = build_detail_list(fc_processed, TAF_FORECAST_FIELDS)

            wx = (
                fc_processed.get("wxString")
                or fc_processed.get("weather")
                or fc_processed.get("wx_string")
            )
            if wx:
                if isinstance(wx, Iterable) and not isinstance(wx, (str, bytes, bytearray)):
                    parts = []
                    for item in wx:
                        simplified = _simplify_detail_value(item)
                        if simplified in (None, "", []):
                            continue
                        parts.append(str(simplified))
                    wx = ", ".join(parts)
                fc_details.append(("Weather", wx))

            clouds = (
                fc_processed.get("clouds")
                or fc_processed.get("cloudList")
                or fc_processed.get("skyCondition")
                or fc_processed.get("sky_condition")
            )
            if isinstance(clouds, str):
                try:
                    parsed_clouds = json.loads(clouds)
                except (TypeError, ValueError):
                    parsed_clouds = []
                clouds = parsed_clouds
            if isinstance(clouds, Iterable) and not isinstance(clouds, (str, bytes, bytearray)):
                cloud_iterable = clouds
            else:
                cloud_iterable = []
            if isinstance(cloud_iterable, Iterable):
                cloud_parts: List[str] = []
                for cloud in cloud_iterable:
                    if not isinstance(cloud, MutableMapping):
                        continue
                    cover = (
                        cloud.get("cover")
                        or cloud.get("cloudCover")
                        or cloud.get("cloud_cover")
                        or cloud.get("skyCover")
                        or cloud.get("amount")
                        or cloud.get("repr")
                    )
                    base = (
                        cloud.get("base")
                        or cloud.get("base_feet")
                        or cloud.get("cloudBaseFT")
                        or cloud.get("cloudBaseFt")
                        or cloud.get("baseFeetAGL")
                        or cloud.get("base_feet_agl")
                        or cloud.get("baseFeet")
                    )
                    if isinstance(base, MutableMapping):
                        base = (
                            base.get("value")
                            or base.get("feet")
                            or base.get("repr")
                            or base.get("minValue")
                            or base.get("maxValue")
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

        if not forecast_periods:
            forecast_periods = _fallback_parse_raw_taf(
                raw_text,
                issue_dt,
                valid_from_dt,
                valid_to_dt,
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
