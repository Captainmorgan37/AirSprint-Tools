"""Utilities for retrieving and normalising TAF forecasts."""

from __future__ import annotations

import calendar
import csv
import json
import math
import os
import re

from datetime import datetime, timezone
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


def _fetch_nearby_taf_report(station_id: str) -> Optional[Dict[str, Any]]:
    station_id = (station_id or "").upper().strip()
    if not station_id:
        return None

    base_coords = _lookup_station_coordinates(station_id)
    if not base_coords:
        return None

    base_lat, base_lon = base_coords

    best_entry: Optional[Dict[str, Any]] = None
    best_distance: Optional[float] = None

    api_coords = _fetch_station_coords_from_api(station_id)
    if api_coords:
        base_lat, base_lon = api_coords

    base_url = "https://aviationweather.gov/adds/dataserver_current/httpparam"

    for radius_nm in FALLBACK_TAF_SEARCH_RADII_NM:
        def _request(radial_target: str) -> Tuple[str, List[Dict[str, Any]]]:
            params = {
                "dataSource": "tafs",
                "requestType": "retrieve",
                "format": "JSON",
                "radialDistance": f"{radius_nm};{radial_target}",
                "hoursBeforeNow": "5",
            }

            try:
                resp = requests.get(base_url, params=params, timeout=10)
                resp.raise_for_status()
            except requests.HTTPError as exc:
                status_code = getattr(exc.response, "status_code", None)
                if status_code == 400:
                    return "bad_request", []
                return "error", []
            except requests.RequestException:
                return "error", []

            try:
                data = resp.json()
            except ValueError:
                return "error", []

            tafs = [taf for taf in _normalize_aviationweather_features(data)]
            return "ok", tafs

        status, taf_candidates = _request(station_id)

        if status == "bad_request":
            if api_coords is None:
                api_coords = _fetch_station_coords_from_api(station_id)
                if api_coords:
                    base_lat, base_lon = api_coords
            target_coords: Optional[str] = None
            if api_coords:
                target_coords = f"{api_coords[0]},{api_coords[1]}"
            else:
                target_coords = f"{base_lat},{base_lon}"

            if target_coords:
                status, taf_candidates = _request(target_coords)
            else:
                taf_candidates = []
        elif status == "ok" and not taf_candidates:
            target_coords = f"{base_lat},{base_lon}"
            status_coords, taf_candidates_coords = _request(target_coords)
            if status_coords == "ok" and taf_candidates_coords:
                taf_candidates = taf_candidates_coords
        elif status != "ok":
            continue

        found_candidate = False

        for taf in taf_candidates:
            taf_station = (taf.get("station") or taf.get("stationId") or "").upper().strip()
            if not taf_station or taf_station == station_id:
                continue

            distance_nm = _coerce_float(
                taf.get("distanceNm")
                or taf.get("distance_nm")
                or taf.get("distance")
            )

            if distance_nm is None:
                taf_coords = _lookup_station_coordinates(taf_station)
                if taf_coords:
                    distance_nm = _haversine_distance_nm(
                        base_lat,
                        base_lon,
                        taf_coords[0],
                        taf_coords[1],
                    )

            if best_distance is not None:
                if distance_nm is None:
                    continue
                if distance_nm >= best_distance:
                    continue

            candidate = dict(taf)
            candidate["is_fallback"] = True
            candidate["fallback_distance_nm"] = distance_nm
            candidate["fallback_radius_nm"] = radius_nm
            best_entry = candidate
            if distance_nm is not None:
                best_distance = distance_nm
            found_candidate = True

        if found_candidate and best_entry is not None:
            break

    return best_entry


def _build_report_from_props(
    props: MutableMapping[str, Any],
    *,
    is_fallback: bool = False,
    fallback_distance_nm: Optional[float] = None,
    fallback_radius_nm: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    station = (
        props.get("station")
        or props.get("stationId")
        or props.get("icaoId")
        or props.get("icao_id")
        or ""
    )
    station = str(station).upper()
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

    data = response.json()

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
