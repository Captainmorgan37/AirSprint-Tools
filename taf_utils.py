import csv
import math
import os
import re
from datetime import datetime, timedelta
from calendar import monthrange
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

import requests
from collections.abc import MutableMapping

###############################################################################
# Constants / config
###############################################################################

EARTH_RADIUS_NM = 3440.065  # nautical miles
FALLBACK_TAF_SEARCH_RADII_NM = [60, 90, 120, 180]  # how far we'll look for a "nearby" TAF

# We'll lazy-load this from Airport TZ.txt
_AIRPORT_COORDS: Dict[str, Tuple[float, float]] = {}


###############################################################################
# General small helpers
###############################################################################

def _coerce_float(x) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _normalize_aviationweather_features(data):
    """
    AviationWeather endpoints sometimes return:
    - GeoJSON-style: {"features":[{"properties":{...}}, ...]}
    - Plain JSON list: [{"station":"CYXX", ...}, ...]
    - Dict with "reports": {"reports":[{...},{...}]}
    - Single dict with keys directly
    This yields dict-like "property bundles" so we can treat them uniformly.
    """
    if isinstance(data, dict):
        # GeoJSON-ish
        feats = data.get("features")
        if isinstance(feats, list):
            for feat in feats:
                if not isinstance(feat, dict):
                    continue
                props = feat.get("properties")
                if isinstance(props, dict):
                    yield props
                else:
                    # Sometimes there's useful stuff directly in feat
                    yield feat
            return

        # "reports" style
        reps = data.get("reports")
        if isinstance(reps, list):
            for rep in reps:
                if isinstance(rep, dict):
                    yield rep
            return

        # Just treat the dict itself as one "feature"
        yield data
        return

    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            if "properties" in item and isinstance(item["properties"], dict):
                yield item["properties"]
            else:
                yield item


###############################################################################
# Time parsing helpers
###############################################################################

def _safe_build_dt(day: int, hour: int, minute: int = 0,
                   ref_dt: Optional[datetime] = None) -> Optional[datetime]:
    """
    TAFs don't include month/year on every time token, just DDHHMMZ or DDHH/DDHH.
    We guess using "now", then adjust ±1 month if we're off by >20 days.
    Good enough to sort TAFs and show nice timestamps.
    """
    if ref_dt is None:
        ref_dt = datetime.utcnow()

    year = ref_dt.year
    month = ref_dt.month

    # clamp "day" so 31 doesn't explode in February, etc.
    last_day_this_month = monthrange(year, month)[1]
    if day > last_day_this_month:
        day = last_day_this_month

    try:
        dt = datetime(year, month, day, hour, minute)
    except ValueError:
        return None

    # If guess is too far in the past/future, wrap month.
    if (ref_dt - dt) > timedelta(days=20):
        # maybe actually next month
        m2 = month + 1
        y2 = year
        if m2 > 12:
            m2 = 1
            y2 += 1
        last_day_next = monthrange(y2, m2)[1]
        adj_day = min(day, last_day_next)
        try:
            dt = datetime(y2, m2, adj_day, hour, minute)
        except ValueError:
            pass
    elif (dt - ref_dt) > timedelta(days=20):
        # maybe actually previous month
        m2 = month - 1
        y2 = year
        if m2 < 1:
            m2 = 12
            y2 -= 1
        last_day_prev = monthrange(y2, m2)[1]
        adj_day = min(day, last_day_prev)
        try:
            dt = datetime(y2, m2, adj_day, hour, minute)
        except ValueError:
            pass

    return dt


def _parse_issue_time(issue_token: str) -> Tuple[Optional[datetime], str]:
    """
    issue_token like '281340Z' -> DDHHMMZ
    returns (datetime_obj_or_None, "28 Oct 13:40Z")
    """
    m = re.match(r"(?P<day>\d{2})(?P<hour>\d{2})(?P<min>\d{2})Z", issue_token)
    if not m:
        return None, "N/A"

    day = int(m.group("day"))
    hour = int(m.group("hour"))
    minute = int(m.group("min"))

    dt = _safe_build_dt(day, hour, minute)
    if dt is None:
        return None, "N/A"

    return dt, dt.strftime("%d %b %H:%MZ")


def _parse_valid_period(validity_token: str) -> Dict[str, object]:
    """
    validity_token like '2812/2912' -> DDHH/DDHH
    returns display strings and dt objects:
      "valid_from_display": "28 Oct 12Z"
      "valid_to_display":   "29 Oct 12Z"
    """
    m = re.match(
        r"(?P<d1>\d{2})(?P<h1>\d{2})/(?P<d2>\d{2})(?P<h2>\d{2})",
        validity_token
    )
    if not m:
        return {
            "valid_from_display": "N/A",
            "valid_to_display": "N/A",
            "valid_from_dt": None,
            "valid_to_dt": None,
        }

    d1 = int(m.group("d1"))
    h1 = int(m.group("h1"))
    d2 = int(m.group("d2"))
    h2 = int(m.group("h2"))

    start_dt = _safe_build_dt(d1, h1, 0)
    end_dt = _safe_build_dt(d2, h2, 0)

    if start_dt is None:
        start_disp = "N/A"
    else:
        start_disp = start_dt.strftime("%d %b %HZ")

    if end_dt is None:
        end_disp = "N/A"
    else:
        end_disp = end_dt.strftime("%d %b %HZ")

    return {
        "valid_from_display": start_disp,
        "valid_to_display": end_disp,
        "valid_from_dt": start_dt,
        "valid_to_dt": end_dt,
    }


###############################################################################
# TAF parsing helpers
###############################################################################

def _parse_single_taf_block(raw_block: str) -> Optional[Dict[str, object]]:
    """
    Parse one TAF bulletin in "raw" AviationWeather format:
      TAF CYKF 281340Z 2814/2914 24012KT P6SM SCT020 ...
    or with AMD/COR:
      TAF AMD CYKF 281400Z 2814/2914 ...

    We extract:
      station
      issue_time / issue_time_display
      valid_from_display / valid_to_display
      raw
      forecast (empty list for now)
    """
    # normalize whitespace
    raw_block = " ".join(raw_block.replace("\n", " ").split())
    tokens = raw_block.split()
    if not tokens or tokens[0] != "TAF":
        return None

    idx = 1
    if idx < len(tokens) and tokens[idx] in ("AMD", "COR", "RTD"):
        idx += 1

    if idx >= len(tokens):
        return None
    station = tokens[idx].upper().strip()

    issue_token = tokens[idx + 1] if (idx + 1) < len(tokens) else ""
    validity_token = tokens[idx + 2] if (idx + 2) < len(tokens) else ""

    issue_dt, issue_disp = _parse_issue_time(issue_token)
    validity_info = _parse_valid_period(validity_token)

    taf_dict = {
        "station": station,
        "issue_time": issue_dt,
        "issue_time_display": issue_disp,
        "valid_from_display": validity_info["valid_from_display"],
        "valid_to_display": validity_info["valid_to_display"],
        "raw": raw_block,
        "forecast": [],        # you can expand this later if you want segment tables
        "is_fallback": False,  # may flip to True later
        # "fallback_distance_nm": float(...)
        # "fallback_radius_nm": float(...)
    }
    return taf_dict


def _parse_all_tafs(raw_text: str) -> List[Dict[str, object]]:
    """
    AviationWeather returns multiple bulletins concatenated. We'll split them.
    Pattern: "TAF " ... up to next " TAF " or end.
    """
    if not raw_text:
        return []

    pattern = re.compile(r"\bTAF\b.*?(?=(?:\sTAF\b|$))", re.DOTALL)
    blocks = pattern.findall(raw_text)

    tafs: List[Dict[str, object]] = []
    for block in blocks:
        taf_obj = _parse_single_taf_block(block.strip())
        if taf_obj:
            tafs.append(taf_obj)

    return tafs


###############################################################################
# Airport coordinate database loader
###############################################################################

def _load_airport_coords_db() -> None:
    """
    Populate _AIRPORT_COORDS from Airport TZ.txt if not loaded yet.

    Expected CSV columns from your file dump look like:
    0: ICAO
    1: IATA
    2: Name
    3: City
    4: Subd / Province / State
    5: Country
    6: Elevation (ft)
    7: Lat (decimal degrees)
    8: Lon (decimal degrees)
    9: TZ
    10: LID (local id)

    Example row (Stratford, Ontario):
    "CYSA","","Stratford Municipal Airport","Stratford","Ontario","CA",1215,43.4156,-80.9344,"America/Toronto",""
    """
    global _AIRPORT_COORDS
    if _AIRPORT_COORDS:
        return  # already loaded

    db_path = os.path.join(os.path.dirname(__file__), "Airport TZ.txt")

    try:
        with open(db_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                # need at least up to lon index
                if not row or len(row) < 9:
                    continue

                icao_raw = row[0].strip().strip('"').upper()
                if not icao_raw or icao_raw == "ICAO":
                    # skip header or empty rows
                    continue

                # lat/lon columns are index 7,8 in your file
                try:
                    lat_val = float(row[7])
                    lon_val = float(row[8])
                except (ValueError, IndexError):
                    continue

                _AIRPORT_COORDS[icao_raw] = (lat_val, lon_val)

    except OSError:
        # couldn't open the file -> leave dict empty, we'll fall back to API
        pass


###############################################################################
# Coordinate lookup for a station
###############################################################################

@lru_cache(maxsize=512)
def _lookup_station_coordinates(station: str) -> Optional[Tuple[float, float]]:
    """
    Return (lat, lon) for a station.

    Priority order:
      1. Your local Airport TZ.txt file (this fixes CYSA/Stratford vs CYSA/Sable Island,
         and also fills in small aerodromes like CYLS).
      2. AviationWeather /api/data/stationinfo (as a fallback for anything not in your file).
    """
    station = (station or "").upper().strip()
    if not station:
        return None

    # 1. local database
    _load_airport_coords_db()
    if station in _AIRPORT_COORDS:
        return _AIRPORT_COORDS[station]

    # 2. fallback to AviationWeather stationinfo
    url = "https://aviationweather.gov/api/data/stationinfo"
    params = {"ids": station, "format": "json"}

    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 204 or not resp.content.strip():
            return None
        resp.raise_for_status()
    except requests.RequestException:
        return None

    try:
        data = resp.json()
    except ValueError:
        return None

    # Walk through whatever data shape they return
    coords_from_api: Optional[Tuple[float, float]] = None

    for props in _normalize_aviationweather_features(data):
        if not isinstance(props, MutableMapping):
            continue

        lat = _coerce_float(
            props.get("lat")
            or props.get("latitude")
            or props.get("stationLatitude")
            or props.get("latitudeDeg")
            or props.get("latitude_deg")
        )
        lon = _coerce_float(
            props.get("lon")
            or props.get("longitude")
            or props.get("stationLongitude")
            or props.get("longitudeDeg")
            or props.get("longitude_deg")
        )

        if lat is not None and lon is not None:
            coords_from_api = (lat, lon)
            break

        # Sometimes stationinfo comes back more GeoJSON-y:
        geom = props.get("geometry") if isinstance(props, dict) else None
        if isinstance(geom, dict):
            coords_list = geom.get("coordinates")
            if (
                isinstance(coords_list, (list, tuple))
                and len(coords_list) >= 2
            ):
                # [lon, lat] typical GeoJSON
                lon_try = _coerce_float(coords_list[0])
                lat_try = _coerce_float(coords_list[1])
                if lat_try is not None and lon_try is not None:
                    coords_from_api = (lat_try, lon_try)
                    break

    return coords_from_api


###############################################################################
# Distance / bbox helpers
###############################################################################

def _haversine_distance_nm(lat1: float, lon1: float,
                           lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles."""
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
    Build a bounding box string for AviationWeather /api/data/taf.

    AviationWeather wants bbox as "lat0,lon0,lat1,lon1"
    corresponding (roughly) to SW corner then NE corner.

    We'll approximate the circle radius_nm with a square:
      1 deg latitude ~ 60 NM
      1 deg longitude ~ 60 NM * cos(latitude)
    """
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

    # bbox=lat0,lon0,lat1,lon1 (SW lat/lon, NE lat/lon)
    return f"{min_lat:.4f},{min_lon:.4f},{max_lat:.4f},{max_lon:.4f}"


###############################################################################
# Raw TAF fetcher
###############################################################################

def _fetch_taf_text(params: Dict[str, str]) -> str:
    """
    Call AviationWeather /api/data/taf.

    We'll request "raw" format because it's consistent and easy to parse,
    and it's valid for both ids=... and bbox=...

    If the API returns 400 because it doesn't like extra params,
    we retry with only the essentials.
    """
    base_url = "https://aviationweather.gov/api/data/taf"

    try:
        r = requests.get(base_url, params=params, timeout=10)
        # 204 or blank -> no data
        if r.status_code == 204 or not r.text.strip():
            return ""
        r.raise_for_status()
        return r.text
    except requests.HTTPError as exc:
        status_code = getattr(exc.response, "status_code", None)
        if status_code == 400:
            # strip to bare essentials: just ids or bbox
            fallback_params = {}
            if "ids" in params:
                fallback_params["ids"] = params["ids"]
            if "bbox" in params:
                fallback_params["bbox"] = params["bbox"]

            r2 = requests.get(base_url, params=fallback_params, timeout=10)
            if r2.status_code == 204 or not r2.text.strip():
                return ""
            r2.raise_for_status()
            return r2.text
        else:
            raise
    except requests.RequestException:
        return ""


###############################################################################
# Fallback / nearest-TAF search
###############################################################################

def _fetch_nearby_taf_report(station_id: str) -> Optional[Dict[str, object]]:
    """
    Try to "borrow" a TAF from the nearest reporting aerodrome if this station
    doesn't have its own TAF.

    Steps:
      1. Get coords for the requested station (from Airport TZ.txt or API).
      2. Expand radius 60 -> 90 -> 120 -> 180 NM.
      3. For each radius:
         - Build bbox around that point.
         - Query /api/data/taf?bbox=...&time=issue&format=raw
         - Parse all TAFs returned.
         - For each TAF, get that TAF station's coords.
         - Skip if it's literally the same station_id we're looking up.
         - Compute distance. Track the closest.
      4. Return the closest as fallback with "is_fallback": True
         and distance metadata.
    """
    station_id = (station_id or "").upper().strip()
    if not station_id:
        return None

    base_coords = _lookup_station_coordinates(station_id)
    if not base_coords:
        return None

    base_lat, base_lon = base_coords

    best_entry = None
    best_dist = None
    best_radius = None

    for radius_nm in FALLBACK_TAF_SEARCH_RADII_NM:
        bbox_str = _make_bbox(base_lat, base_lon, radius_nm)

        taf_text = _fetch_taf_text(
            {
                "bbox": bbox_str,
                "time": "issue",
                "format": "raw",
            }
        )

        taf_list = _parse_all_tafs(taf_text)
        if not taf_list:
            continue

        for taf in taf_list:
            taf_station = taf.get("station")
            if not taf_station:
                continue

            # don't "fallback" to the same station (useless,
            # and also protects you from cases where NOAA thinks
            # CYSA is Sable Island and just returns itself)
            if taf_station.upper().strip() == station_id:
                continue

            their_coords = _lookup_station_coordinates(taf_station)
            if not their_coords:
                continue

            dist_nm = _haversine_distance_nm(
                base_lat,
                base_lon,
                their_coords[0],
                their_coords[1],
            )

            if best_entry is None or dist_nm < (best_dist or 1e9):
                candidate = dict(taf)
                candidate["is_fallback"] = True
                candidate["fallback_distance_nm"] = dist_nm
                candidate["fallback_radius_nm"] = radius_nm
                best_entry = candidate
                best_dist = dist_nm
                best_radius = radius_nm

        # Once we found any candidate in this radius, stop expanding.
        if best_entry is not None:
            break

    return best_entry


###############################################################################
# Public main: get_taf_reports
###############################################################################

def get_taf_reports(icao_codes: List[str]) -> Dict[str, List[Dict[str, object]]]:
    """
    Main entry point your Streamlit app calls.

    Input: list like ["CYLS", "CYSA"]
    Output: dict like:
      {
        "CYLS": [ { taf_dict }, ... ],
        "CYSA": [ { taf_dict }, ... ],
      }

    Each taf_dict contains:
      station
      issue_time
      issue_time_display
      valid_from_display
      valid_to_display
      raw
      forecast
      is_fallback (bool)
      fallback_distance_nm (float, only if is_fallback)
      fallback_radius_nm (float, only if is_fallback)
    """
    # normalize + dedupe while preserving order
    clean_codes: List[str] = []
    for code in icao_codes:
        if not code:
            continue
        up = code.strip().upper()
        if up and up not in clean_codes:
            clean_codes.append(up)

    if not clean_codes:
        return {}

    results: Dict[str, List[Dict[str, object]]] = {code: [] for code in clean_codes}

    # 1. Ask AviationWeather for direct TAFs for all requested stations
    taf_text = _fetch_taf_text(
        {
            "ids": ",".join(clean_codes),
            "time": "issue",
            "format": "raw",
        }
    )

    taf_list = _parse_all_tafs(taf_text)

    # Group by issuing station
    grouped: Dict[str, List[Dict[str, object]]] = {}
    for taf in taf_list:
        stn = taf.get("station")
        if not stn:
            continue
        grouped.setdefault(stn, []).append(taf)

    # Keep newest TAF per station
    for stn, tafs in grouped.items():
        tafs_sorted = sorted(
            tafs,
            key=lambda t: t.get("issue_time") or datetime.min,
            reverse=True,
        )
        grouped[stn] = [tafs_sorted[0]]

    # Fill direct hits
    for code in clean_codes:
        if code in grouped:
            direct_taf = dict(grouped[code][0])
            direct_taf["is_fallback"] = False
            direct_taf.pop("fallback_distance_nm", None)
            direct_taf.pop("fallback_radius_nm", None)
            results[code] = [direct_taf]

    # 2. For any station still empty, try nearest fallback
    for code in clean_codes:
        if results[code]:
            continue
        nearby = _fetch_nearby_taf_report(code)
        if nearby:
            results[code] = [nearby]

    return results
