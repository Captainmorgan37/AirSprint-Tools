"""Utilities for retrieving and normalising TAF forecasts."""

from __future__ import annotations

import calendar
import csv
import json
import math
import os
import re

from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Sequence, Tuple

import requests


EARTH_RADIUS_NM = 3440.065  # nautical miles
FALLBACK_TAF_SEARCH_RADII_NM = [60, 90, 120, 180]

_AIRPORT_COORDS: Dict[str, Tuple[float, float]] = {}


def _coerce_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _guess_datetime_from_tokens(
    day: int, hour: int, minute: int = 0, *, reference: Optional[datetime] = None
) -> Optional[datetime]:
    if reference is None:
        reference = datetime.utcnow().replace(tzinfo=timezone.utc)

    year = reference.year
    month = reference.month

    last_day = calendar.monthrange(year, month)[1]
    if day > last_day:
        day = last_day

    try:
        candidate = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
    except ValueError:
        return None

    if reference - candidate > timedelta(days=15):
        month += 1
        if month > 12:
            month = 1
            year += 1
    elif candidate - reference > timedelta(days=15):
        month -= 1
        if month < 1:
            month = 12
            year -= 1

    last_day = calendar.monthrange(year, month)[1]
    if day > last_day:
        day = last_day

    try:
        return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_raw_taf_bulletins(raw_text: str) -> List[Dict[str, Any]]:
    """
    Parse a raw (text) response from aviationweather.gov/api/data/taf?bbox=...&format=raw
    into bulletin dicts that mirror the structured API output AND include a 'forecast'
    list synthesized from the raw text (via _fallback_parse_raw_taf).

    This version also correctly handles validity ranges like `2912/2924`, where
    `24` means "00Z on the next day".
    """
    pattern = re.compile(r"\bTAF\b.*?(?=(?:\sTAF\b|$))", re.DOTALL)
    bulletins: List[Dict[str, Any]] = []
    reference = datetime.utcnow().replace(tzinfo=timezone.utc)

    for block in pattern.findall(raw_text):
        # Collapse whitespace/newlines
        clean = " ".join(block.replace("\n", " ").split())
        if not clean:
            continue

        tokens = clean.split()
        if not tokens or tokens[0] != "TAF":
            continue

        # After "TAF", skip AMD/COR/RTD if present
        idx = 1
        while idx < len(tokens) and tokens[idx] in {"AMD", "COR", "RTD"}:
            idx += 1
        if idx >= len(tokens):
            continue

        station = tokens[idx].upper().strip()
        if not station:
            continue

        # We'll keep ISO strings for downstream display (format_iso_timestamp)
        # and also keep tz-aware datetime objects for _fallback_parse_raw_taf
        issue_iso: Optional[str] = None
        valid_from_iso: Optional[str] = None
        valid_to_iso: Optional[str] = None

        issue_dt: Optional[datetime] = None
        start_dt: Optional[datetime] = None
        end_dt: Optional[datetime] = None

        # tokens[idx+1] => issue time like "291146Z"
        if idx + 1 < len(tokens):
            m_issue = re.match(r"^(\d{2})(\d{2})(\d{2})Z$", tokens[idx + 1])
            if m_issue:
                guess = _guess_datetime_from_tokens(
                    int(m_issue.group(1)),  # day
                    int(m_issue.group(2)),  # hour
                    int(m_issue.group(3)),  # minute
                    reference=reference,
                )
                if guess:
                    issue_dt = guess
                    issue_iso = guess.isoformat()

        # tokens[idx+2] => validity range like "2912/2924"
        if idx + 2 < len(tokens):
            m_val = re.match(r"^(\d{2})(\d{2})/(\d{2})(\d{2})$", tokens[idx + 2])
            if m_val:
                start_day = int(m_val.group(1))
                start_hour = int(m_val.group(2))
                end_day = int(m_val.group(3))
                end_hour = int(m_val.group(4))

                # Handle the TAF convention where HH=24 means "next day at 00Z"
                if start_hour == 24:
                    start_hour = 0
                    start_day += 1
                if end_hour == 24:
                    end_hour = 0
                    end_day += 1

                start_guess = _guess_datetime_from_tokens(
                    start_day,
                    start_hour,
                    reference=reference,
                )
                end_guess = _guess_datetime_from_tokens(
                    end_day,
                    end_hour,
                    reference=reference,
                )

                if start_guess:
                    start_dt = start_guess
                    valid_from_iso = start_guess.isoformat()
                if end_guess:
                    end_dt = end_guess
                    valid_to_iso = end_guess.isoformat()

        # Build the "props-like" dict this bulletin should look like
        entry: Dict[str, Any] = {
            "station": station,
            "rawTAF": clean,
            "rawText": clean,
            "raw": clean,
        }
        if issue_iso:
            entry["issueTime"] = issue_iso
        if valid_from_iso:
            entry["validTimeFrom"] = valid_from_iso
        if valid_to_iso:
            entry["validTimeTo"] = valid_to_iso

        # Synthesize structured forecast segments from this raw TAF.
        # IMPORTANT: _fallback_parse_raw_taf needs non-None issue_dt/start_dt/end_dt
        # to build usable segments. Before, `end_dt` could be None whenever the TAF
        # ended in "/2924". Now we normalize "24" -> next day 00Z so end_dt is valid.
        entry["forecast"] = _fallback_parse_raw_taf(
            clean,
            issue_dt,
            start_dt,
            end_dt,
        )

        bulletins.append(entry)

    return bulletins



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

    # Normalise a handful of non-standard timestamp representations that crop up
    # in the various aviationweather.gov responses.  We have seen values such as
    # ``2024-10-24T09:00:00-0400`` (offset without a colon),
    # ``20241024T0900Z`` (compact Zulu format) and strings that include a
    # trailing ``[UTC]`` suffix.  Coerce these into ISO-8601 so
    # ``datetime.fromisoformat`` can handle them reliably.
    normalized = value_str
    if normalized.endswith("[UTC]"):
        normalized = normalized[:-5]
    if normalized.upper().endswith(" UTC"):
        normalized = normalized[:-4] + "Z"
    if normalized.upper().endswith(" Z"):
        normalized = normalized[:-2] + "Z"

    match = re.match(r"^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})Z$", normalized)
    if match:
        normalized = (
            f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
            f"T{match.group(4)}:{match.group(5)}Z"
        )

    if re.search(r"[+-]\d{4}$", normalized) and not re.search(r"[+-]\d{2}:\d{2}$", normalized):
        normalized = normalized[:-4] + normalized[-4:-2] + ":" + normalized[-2:]

    if normalized.isdigit():
        try:
            seconds = int(normalized)
        except ValueError:
            seconds = None
        if seconds is not None:
            if len(normalized) > 10:
                seconds /= 1000.0
            dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
            return _format(dt)

    try:
        dt = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
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
        features = data.get("features")
        if isinstance(features, Iterable):
            for item in features:
                if not isinstance(item, MutableMapping):
                    continue
                props = item.get("properties")
                if isinstance(props, MutableMapping):
                    yield props  # type: ignore[misc]
                else:
                    yield item  # type: ignore[misc]
            return

        data_container = data.get("data")
        if isinstance(data_container, MutableMapping):
            for value in data_container.values():
                if isinstance(value, Iterable):
                    for item in value:
                        if isinstance(item, MutableMapping):
                            yield item  # type: ignore[misc]
            return

        reports = data.get("reports")
        if isinstance(reports, Iterable):
            for rep in reports:
                if isinstance(rep, MutableMapping):
                    yield rep  # type: ignore[misc]
            return

        yield data  # type: ignore[misc]
        return

    if isinstance(data, Iterable):
        for item in data:
            if not isinstance(item, MutableMapping):
                continue
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

            if hour == 24:
                hour = 0
                day += 1

            while True:
                try:
                    return datetime(year, month, day, hour, 0, tzinfo=timezone.utc)
                except ValueError:
                    # handle month rollover when the day exceeds the length of the month
                    days_in_month = calendar.monthrange(year, month)[1]
                    day -= days_in_month
                    month += 1
                    if month > 12:
                        month = 1
                        year += 1

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

        segments.append(
            {
                # readable strings
                "from_display": segment_start.strftime("%b %d %Y, %H:%MZ"),
                "to_display": (
                    segment_end.strftime("%b %d %Y, %H:%MZ") if segment_end else "N/A"
                ),
        
                # datetime objects (original keys)
                "from_time": segment_start,
                "to_time": segment_end,
        
                # NEW: alias keys that _extract_time_field() will actually pick up
                "time_from": segment_start,
                "time_to": segment_end,
        
                # details
                "details": prevailing_details,
                "tempo": tempo_blocks,
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


def _load_airport_coords_db() -> None:
    global _AIRPORT_COORDS
    if _AIRPORT_COORDS:
        return

    db_path = os.path.join(os.path.dirname(__file__), "Airport TZ.txt")

    try:
        with open(db_path, newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            for row in reader:
                if not row or len(row) < 9:
                    continue

                icao = row[0].strip().strip('"').upper()
                if not icao or icao == "ICAO":
                    continue

                try:
                    lat_val = float(row[7])
                    lon_val = float(row[8])
                except (ValueError, IndexError):
                    continue

                _AIRPORT_COORDS[icao] = (lat_val, lon_val)
    except OSError:
        pass


def _fetch_station_coords_from_api(station: str) -> Optional[Tuple[float, float]]:
    url = "https://aviationweather.gov/adds/dataserver_current/httpparam"
    params = {
        "dataSource": "stations",
        "requestType": "retrieve",
        "format": "JSON",
        "stationString": station,
    }

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
    except requests.RequestException:
        return None

    try:
        data = resp.json()
    except ValueError:
        return None

    stations = []
    if isinstance(data, MutableMapping):
        stations = data.get("data", {}).get("STATION", [])  # type: ignore[assignment]

    for props in stations:
        if not isinstance(props, MutableMapping):
            continue

        lat = _coerce_float(props.get("latitude") or props.get("lat"))
        lon = _coerce_float(props.get("longitude") or props.get("lon"))

        if lat is not None and lon is not None:
            return (lat, lon)

    return None


@lru_cache(maxsize=512)
def _lookup_station_coordinates(station: str) -> Optional[Tuple[float, float]]:
    station = (station or "").upper().strip()
    if not station:
        return None

    _load_airport_coords_db()
    if station in _AIRPORT_COORDS:
        return _AIRPORT_COORDS[station]

    return _fetch_station_coords_from_api(station)


def _haversine_distance_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    rlat1 = math.radians(lat1)
    rlon1 = math.radians(lon1)
    rlat2 = math.radians(lat2)
    rlon2 = math.radians(lon2)

    dlat = rlat2 - rlat1
    dlon = rlon2 - rlon1

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return EARTH_RADIUS_NM * c

def _make_bbox(lat: float, lon: float, radius_nm: float) -> str:
    """
    Build a bounding box string for aviationweather.gov/api/data/taf.

    aviationweather.gov expects bbox as "lat0,lon0,lat1,lon1"
    where (lat0,lon0) is SW corner and (lat1,lon1) is NE corner.

    We'll approximate a circle with a square:
      ~60 NM per degree latitude
      ~60 NM * cos(lat) per degree longitude
    """
    # how many degrees of lat/lon ≈ this many NM?
    dlat = radius_nm / 60.0

    cos_lat = math.cos(math.radians(lat))
    if abs(cos_lat) < 1e-6:
        dlon = radius_nm / 60.0
    else:
        dlon = radius_nm / (60.0 * cos_lat)

    min_lat = lat - dlat
    max_lat = lat + dlat
    min_lon = lon - dlon
    max_lon = lon + dlon

    return f"{min_lat:.4f},{min_lon:.4f},{max_lat:.4f},{max_lon:.4f}"


def _fetch_nearby_taf_report(station_id: str) -> Optional[Dict[str, Any]]:
    """
    Find the nearest available TAF if `station_id` itself doesn't have one.

    Steps:
      1. Get coordinates for the requested station (prefers Airport TZ.txt).
      2. For increasing radii (60, 90, 120, 180 NM):
         - Build a lat/lon bbox around that point.
         - Call aviationweather.gov/api/data/taf with that bbox, asking for raw TAF text.
         - Parse the returned bulletins with _parse_raw_taf_bulletins.
         - For each bulletin:
             * identify its station
             * skip if it's literally the same as station_id
             * compute distance to our origin station (using our coord DB)
         - Keep the closest.
      3. Return the best match, with fallback metadata.

    Returns:
        A dict shaped like a TAF "props" block plus:
          "is_fallback": True
          "fallback_distance_nm": float
          "fallback_radius_nm": float
        or None if nothing usable was found.
    """
    station_id = (station_id or "").upper().strip()
    if not station_id:
        return None

    # Step 1: where are we?
    base_coords = _lookup_station_coordinates(station_id)
    if not base_coords:
        return None
    base_lat, base_lon = base_coords

    best_entry: Optional[Dict[str, Any]] = None
    best_distance: Optional[float] = None

    taf_url = "https://aviationweather.gov/api/data/taf"

    for radius_nm in FALLBACK_TAF_SEARCH_RADII_NM:
        bbox_str = _make_bbox(base_lat, base_lon, radius_nm)

        params = {
            "bbox": bbox_str,
            "time": "issue",   # "issue" = most recent issuance
            "format": "raw",   # we want plain text TAFs
        }

        try:
            resp = requests.get(taf_url, params=params, timeout=10)
            # If the API gives 204 No Content or empty text, skip
            if resp.status_code == 204 or not resp.text.strip():
                continue
            resp.raise_for_status()
        except requests.RequestException:
            continue

        raw_text = resp.text or ""
        bulletins = _parse_raw_taf_bulletins(raw_text)
        if not bulletins:
            continue

        found_any_here = False

        for taf in bulletins:
            # taf from _parse_raw_taf_bulletins() has keys like:
            #   "station", "rawTAF", "issueTime", "validTimeFrom", "validTimeTo", ...
            taf_station = str(
                taf.get("station")
                or taf.get("stationId")
                or taf.get("station_id")
                or taf.get("icaoId")
                or taf.get("icao_id")
                or ""
            ).upper().strip()
            if not taf_station:
                continue

            # Don't return ourselves as a "fallback"
            if taf_station == station_id:
                continue

            # Measure distance
            their_coords = _lookup_station_coordinates(taf_station)
            if their_coords:
                dist_nm = _haversine_distance_nm(
                    base_lat, base_lon,
                    their_coords[0], their_coords[1],
                )
            else:
                dist_nm = None

            if dist_nm is None:
                # If we can't measure distance, it's not very helpful;
                # skip unless we literally have no better candidate.
                if best_entry is not None:
                    continue

            # Decide if this is better than what we had
            if best_distance is not None and dist_nm is not None:
                if dist_nm >= best_distance:
                    continue

            # new best candidate
            candidate = dict(taf)
            candidate["is_fallback"] = True
            candidate["fallback_distance_nm"] = dist_nm
            candidate["fallback_radius_nm"] = radius_nm

            best_entry = candidate
            best_distance = dist_nm if dist_nm is not None else best_distance
            found_any_here = True

        # stop expanding once we found at least one viable TAF in this radius
        if found_any_here and best_entry is not None:
            break

    return best_entry


def _build_report_from_props(
    props: MutableMapping[str, Any],
    *,
    is_fallback: bool = False,
    fallback_distance_nm: Optional[float] = None,
    fallback_radius_nm: Optional[float] = None,
) -> Optional[Dict[str, Any]]:

    # Support both AviationWeather (/api/data/taf) and ADDS (dataserver_current)
    station = (
        props.get("station")
        or props.get("stationId")
        or props.get("station_id")      # <-- added
        or props.get("icaoId")
        or props.get("icao_id")
        or ""
    )
    station = str(station).upper().strip()
    if not station:
        return None

    issue_display, issue_dt = format_iso_timestamp(
        props.get("issueTime")
        or props.get("issue_time")
        or props.get("obsTime")
        or props.get("obs_time")
        or props.get("bulletinTime")
    )

    valid_from_display, valid_from_dt = format_iso_timestamp(
        props.get("validTimeFrom")
        or props.get("valid_time_from")
        or props.get("valid_time_from_iso")  # being defensive
    )

    valid_to_display, valid_to_dt = format_iso_timestamp(
        props.get("validTimeTo")
        or props.get("valid_time_to")
        or props.get("valid_time_to_iso")
    )

    raw_text = (
        props.get("rawTAF")
        or props.get("rawText")
        or props.get("raw_text")
        or props.get("raw")
        or ""
    )

    # Forecast segments (either structured or fallback-parse the raw TAF)
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
        to_value   = _extract_time_field(fc_processed, TIME_TO_FIELDS)

        fc_from_display, fc_from_dt = format_iso_timestamp(from_value)
        fc_to_display,   fc_to_dt   = format_iso_timestamp(to_value)

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
                    if simplified not in (None, "", []):
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
            cloud_parts: List[str] = []
            for cloud in clouds:
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
        # fall back to manual TAF parser if we didn't get structured segments
        forecast_periods = _fallback_parse_raw_taf(
            raw_text,
            issue_dt,
            valid_from_dt,
            valid_to_dt,
        )

    report: Dict[str, Any] = {
        "station": station,
        "raw": raw_text,
        "issue_time_display": issue_display,
        "issue_time": issue_dt,
        "valid_from_display": valid_from_display,
        "valid_from": valid_from_dt,
        "valid_to_display": valid_to_display,
        "valid_to": valid_to_dt,
        "forecast": forecast_periods,
        "is_fallback": is_fallback,
    }

    if is_fallback:
        if fallback_distance_nm is not None:
            report["fallback_distance_nm"] = fallback_distance_nm
        if fallback_radius_nm is not None:
            report["fallback_radius_nm"] = fallback_radius_nm

    return report



def get_taf_reports(icao_codes: Sequence[str]) -> Dict[str, List[Dict[str, Any]]]:
    clean_codes: List[str] = []
    for code in icao_codes:
        if not code:
            continue
        up = code.strip().upper()
        if up and up not in clean_codes:
            clean_codes.append(up)

    if not clean_codes:
        return {}

    params = {
        "ids": ",".join(sorted(clean_codes)),
        "format": "json",
        "mostRecent": "true",
    }

    url = "https://aviationweather.gov/api/data/taf"

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

    try:
        data = response.json()
    except ValueError:
        text = response.text or ""
        stripped = text.strip()
        if stripped:
            if "TAF" in stripped:
                bulletins = _parse_raw_taf_bulletins(stripped)
                data = {"reports": bulletins}
            else:
                data = []
        else:
            data = []

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for props in _normalize_aviationweather_features(data):
        if not isinstance(props, MutableMapping):
            continue
        report = _build_report_from_props(props)
        if not report:
            continue
        grouped.setdefault(report["station"], []).append(report)

    for station, reports in list(grouped.items()):
        reports_sorted = sorted(
            reports,
            key=lambda r: r.get("issue_time")
            or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        grouped[station] = [reports_sorted[0]]

    results: Dict[str, List[Dict[str, Any]]] = {code: [] for code in clean_codes}

    for code in clean_codes:
        if code in grouped:
            entry = dict(grouped[code][0])
            entry["is_fallback"] = False
            entry.pop("fallback_distance_nm", None)
            entry.pop("fallback_radius_nm", None)
            entry["requested_station"] = code
            results[code] = [entry]
            continue

        fallback = _fetch_nearby_taf_report(code)
        if not fallback:
            continue

        report = _build_report_from_props(
            dict(fallback),
            is_fallback=True,
            fallback_distance_nm=fallback.get("fallback_distance_nm"),
            fallback_radius_nm=fallback.get("fallback_radius_nm"),
        )

        if report:
            report.setdefault("issue_time_display", fallback.get("issue_time_display", "N/A"))
            report.setdefault("valid_from_display", fallback.get("valid_from_display", "N/A"))
            report.setdefault("valid_to_display", fallback.get("valid_to_display", "N/A"))
            report["requested_station"] = code
            results[code] = [report]

    return results


__all__ = ["TAF_FORECAST_FIELDS", "build_detail_list", "format_iso_timestamp", "get_taf_reports"]
