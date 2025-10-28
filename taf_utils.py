import math
import re
from datetime import datetime, timedelta
from calendar import monthrange
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

import requests

# Constants
EARTH_RADIUS_NM = 3440.065  # nautical miles
FALLBACK_TAF_SEARCH_RADII_NM = [60, 90, 120, 180]  # how far we’ll look for a “nearby” TAF

###############################################################################
# Time parsing helpers
###############################################################################

def _safe_build_dt(day: int, hour: int, minute: int = 0, ref_dt: Optional[datetime] = None) -> Optional[datetime]:
    """
    Take a DDHHMM-style time stamp from a TAF and try to map it to a real datetime
    (UTC-ish) in the current / near-current month.

    We don't get month/year in the TAF header, just day/hour/minute.
    We'll guess using "now", and if that guess is > ~20 days off, we shift ±1 month.
    This is good enough to sort "latest TAF".
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

    # If our guess is way in the past or way in the future, assume month wrap.
    if (ref_dt - dt) > timedelta(days=20):
        # probably actually next month
        month2 = month + 1
        year2 = year
        if month2 > 12:
            month2 = 1
            year2 += 1
        last_day_next = monthrange(year2, month2)[1]
        adj_day = min(day, last_day_next)
        try:
            dt = datetime(year2, month2, adj_day, hour, minute)
        except ValueError:
            pass
    elif (dt - ref_dt) > timedelta(days=20):
        # probably actually previous month
        month2 = month - 1
        year2 = year
        if month2 < 1:
            month2 = 12
            year2 -= 1
        last_day_prev = monthrange(year2, month2)[1]
        adj_day = min(day, last_day_prev)
        try:
            dt = datetime(year2, month2, adj_day, hour, minute)
        except ValueError:
            pass

    return dt


def _parse_issue_time(issue_token: str) -> Tuple[Optional[datetime], str]:
    """
    Example issue_token: '311730Z' -> day=31, 17:30Z
    Returns (datetime_obj_or_None, nice_display_string)
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

    # Format like "31 Oct 17:30Z"
    return dt, dt.strftime("%d %b %H:%MZ")


def _parse_valid_period(validity_token: str) -> Dict[str, object]:
    """
    Example validity_token: '3118/3121' -> valid 31st 18Z thru 31st 21Z.
    Returns display strings and dt objects.
    """
    m = re.match(r"(?P<d1>\d{2})(?P<h1>\d{2})/(?P<d2>\d{2})(?P<h2>\d{2})", validity_token)
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
        # "31 Oct 18Z"
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
    Parse one TAF bulletin. We assume AviationWeather's "raw" output style:
    e.g.
        TAF CYHZ 281132Z 2812/2912 24012KT P6SM SCT020 ...
      or
        TAF AMD CYHZ 281332Z 2813/2912 ...

    We pull out:
    - station
    - issue time
    - validity window
    - entire raw TAF text (for display)
    - forecast=[]  (empty for now; UI will just skip the table)
    """
    # normalize whitespace
    raw_block = " ".join(raw_block.replace("\n", " ").split())
    tokens = raw_block.split()
    if not tokens or tokens[0] != "TAF":
        return None

    idx = 1
    # Handle TAF AMD / TAF COR / etc.
    if idx < len(tokens) and tokens[idx] in ("AMD", "COR", "RTD"):
        idx += 1

    if idx >= len(tokens):
        return None

    station = tokens[idx].upper()

    issue_token = tokens[idx + 1] if (idx + 1) < len(tokens) else ""
    validity_token = tokens[idx + 2] if (idx + 2) < len(tokens) else ""

    issue_dt, issue_disp = _parse_issue_time(issue_token)
    validity_info = _parse_valid_period(validity_token)

    taf_dict = {
        "station": station,
        "issue_time": issue_dt,  # datetime (or None)
        "issue_time_display": issue_disp,  # "31 Oct 17:30Z"
        "valid_from_display": validity_info["valid_from_display"],  # "31 Oct 18Z"
        "valid_to_display": validity_info["valid_to_display"],      # "31 Oct 21Z"
        "raw": raw_block,        # full text; UI will pretty-print via format_taf_for_display_html
        "forecast": [],          # we’re not building segment table right now
        "is_fallback": False,    # may flip to True later
        # optional: "fallback_distance_nm" added only for fallback
    }

    return taf_dict


def _parse_all_tafs(raw_text: str) -> List[Dict[str, object]]:
    """
    AviationWeather TAF endpoint returns multiple bulletins in one text blob, like:
        TAF ESSB 311730Z 3118/3121 ...
        TAF ESOW 310530Z 3106/3110 ...
        TAF ESOE 170030Z 1701/1709 ...
    (Sometimes they're all jammed together on one line.)
    We'll split on repeated "TAF <...>" patterns.
    """
    if not raw_text:
        return []

    # "TAF ...." up until the next " TAF " or end of string
    pattern = re.compile(r"\bTAF\b.*?(?=(?:\sTAF\b|$))", re.DOTALL)
    blocks = pattern.findall(raw_text)

    tafs: List[Dict[str, object]] = []
    for block in blocks:
        taf_obj = _parse_single_taf_block(block.strip())
        if taf_obj:
            tafs.append(taf_obj)

    return tafs


###############################################################################
# Low-level fetch helpers
###############################################################################

def _fetch_taf_text(params: Dict[str, str]) -> str:
    """
    Hit the AviationWeather TAF endpoint.

    /api/data/taf supports:
      - ids=AAAA,BBBB
      - bbox=lat0,lon0,lat1,lon1
      - format=raw|json|...
      - time=valid|issue
      - etc.

    We'll ask for raw text because it's stable and doesn't depend on schema.
    If the server 400s because of optional params, we retry with a simpler set.
    """
    base_url = "https://aviationweather.gov/api/data/taf"

    try:
        r = requests.get(base_url, params=params, timeout=10)
        if r.status_code == 204 or not r.text.strip():
            return ""
        r.raise_for_status()
        return r.text
    except requests.HTTPError as exc:
        status_code = getattr(exc.response, "status_code", None)
        if status_code == 400:
            # Try again with only the essentials (ids or bbox),
            # default format is already "raw".
            fallback_params = {}
            for key in ("ids", "bbox"):
                if key in params:
                    fallback_params[key] = params[key]

            r2 = requests.get(base_url, params=fallback_params, timeout=10)
            if r2.status_code == 204 or not r2.text.strip():
                return ""
            r2.raise_for_status()
            return r2.text
        else:
            raise


@lru_cache(maxsize=512)
def _lookup_station_coordinates(station_id: str) -> Optional[Tuple[float, float]]:
    """
    Query station info to get lat/lon for a station.

    AviationWeather provides /api/data/stationinfo which can return JSON or GeoJSON-ish
    with coordinates, depending on how it's hit.

    We'll walk whatever we get and try:
    - direct "lat"/"lon" keys, OR
    - GeoJSON-style ["geometry"]["coordinates"] == [lon, lat]

    Returns (lat, lon) or None.
    """
    url = "https://aviationweather.gov/api/data/stationinfo"
    params = {"ids": station_id, "format": "json"}

    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 204 or not resp.content.strip():
            return None
        resp.raise_for_status()
    except requests.RequestException:
        return None

    try:
        data = resp.json()
    except Exception:
        return None

    coords: Optional[Tuple[float, float]] = None

    def walk(obj):
        nonlocal coords
        if coords is not None:
            return

        if isinstance(obj, dict):
            # direct lat/lon keys
            lat_val = obj.get("lat") or obj.get("latitude")
            lon_val = obj.get("lon") or obj.get("longitude") or obj.get("lng")
            if lat_val is not None and lon_val is not None:
                try:
                    coords = (float(lat_val), float(lon_val))
                    return
                except (TypeError, ValueError):
                    pass

            # GeoJSON-ish
            geom = obj.get("geometry")
            if isinstance(geom, dict):
                c = geom.get("coordinates")
                if isinstance(c, (list, tuple)) and len(c) >= 2:
                    # Usually [lon, lat]
                    try:
                        lon_f = float(c[0])
                        lat_f = float(c[1])
                        coords = (lat_f, lon_f)
                        return
                    except (TypeError, ValueError):
                        pass

            # Recurse
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    walk(v)

        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    walk(data)
    return coords


def _haversine_distance_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Standard haversine, but returns nautical miles.
    """
    rlat1 = math.radians(lat1)
    rlon1 = math.radians(lon1)
    rlat2 = math.radians(lat2)
    rlon2 = math.radians(lon2)

    dlat = rlat2 - rlat1
    dlon = rlon2 - rlon1

    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return EARTH_RADIUS_NM * c


def _make_bbox(lat: float, lon: float, radius_nm: float) -> str:
    """
    Build a bounding box string that AviationWeather understands.

    IMPORTANT: AviationWeather wants bbox as "lat0,lon0,lat1,lon1", not lon/lat flipped,
    and their own example looks like: "40,-90,45,-85". :contentReference[oaicite:3]{index=3}

    We'll approximate a radius_nm circle by converting NM to deg lat/lon.
    1 degree latitude ≈ 60 NM.
    """
    dlat = radius_nm / 60.0

    # Longitude degrees shrink with cos(latitude). Avoid div/0 up near the poles.
    cos_lat = math.cos(math.radians(lat))
    if abs(cos_lat) < 1e-6:
        dlon = radius_nm / 60.0
    else:
        dlon = radius_nm / (60.0 * cos_lat)

    min_lat = lat - dlat
    max_lat = lat + dlat
    min_lon = lon - dlon
    max_lon = lon + dlon

    # AviationWeather bbox=lat0,lon0,lat1,lon1
    return f"{min_lat:.4f},{min_lon:.4f},{max_lat:.4f},{max_lon:.4f}"


###############################################################################
# Nearby / fallback TAF logic
###############################################################################

def _fetch_nearby_taf_report(station_id: str) -> Optional[Dict[str, object]]:
    """
    For stations that don't issue a TAF:
    - Look up that station's lat/lon
    - Grow a search radius (60nm → 90nm → 120nm → 180nm)
    - For each radius, call TAF API with a bbox around that point
    - Pick the closest TAF we find
    - Return it as a TAF dict with is_fallback=True and fallback_distance_nm added
    """
    coords = _lookup_station_coordinates(station_id.upper())
    if not coords:
        return None

    base_lat, base_lon = coords
    best_entry = None
    best_dist = None

    for radius_nm in FALLBACK_TAF_SEARCH_RADII_NM:
        bbox_str = _make_bbox(base_lat, base_lon, radius_nm)

        taf_text = _fetch_taf_text(
            {
                "bbox": bbox_str,
                "time": "issue",   # ask API to organize by issuance time
                "format": "raw",   # get raw bulletins back like 'TAF CYHZ ...'
            }
        )

        taf_list = _parse_all_tafs(taf_text)
        if not taf_list:
            continue

        for taf in taf_list:
            taf_station = taf.get("station")
            if not taf_station:
                continue

            their_coords = _lookup_station_coordinates(taf_station)
            if not their_coords:
                continue

            dist_nm = _haversine_distance_nm(
                base_lat, base_lon, their_coords[0], their_coords[1]
            )

            # If this is the closest so far, keep it
            if best_entry is None or dist_nm < (best_dist or 1e9):
                candidate = dict(taf)
                candidate["is_fallback"] = True
                candidate["fallback_distance_nm"] = dist_nm
                best_entry = candidate
                best_dist = dist_nm

        # As soon as we found *any* TAF in this radius, stop growing the radius.
        if best_entry is not None:
            break

    return best_entry


###############################################################################
# Public entry point
###############################################################################

def get_taf_reports(icao_codes: List[str]) -> Dict[str, List[Dict[str, object]]]:
    """
    Main function your Streamlit app calls.

    Input:
        ["CYLS", "CYSA", ...]
    Output:
        {
          "CYLS": [ { taf_dict }, ... ],
          "CYSA": [ { taf_dict }, ... ],
        }

    Each taf_dict has:
        station
        issue_time
        issue_time_display
        valid_from_display
        valid_to_display
        raw                      (full TAF text)
        forecast                 (list, currently [])
        is_fallback              (bool)
        fallback_distance_nm     (float, only present if is_fallback==True)
    """

    # Normalize and dedupe but keep order
    clean_codes = []
    for code in icao_codes:
        if not code:
            continue
        up = code.strip().upper()
        if up and up not in clean_codes:
            clean_codes.append(up)

    if not clean_codes:
        return {}

    results: Dict[str, List[Dict[str, object]]] = {code: [] for code in clean_codes}

    # 1. Try to fetch direct TAFs for all requested stations in one shot
    taf_text = _fetch_taf_text(
        {
            "ids": ",".join(clean_codes),
            "time": "issue",
            "format": "raw",
        }
    )

    taf_list = _parse_all_tafs(taf_text)

    # Group TAFs by the station that issued them
    grouped: Dict[str, List[Dict[str, object]]] = {}
    for taf in taf_list:
        stn = taf.get("station")
        if not stn:
            continue
        grouped.setdefault(stn, []).append(taf)

    # Keep only the newest TAF per station (sort by issue_time desc)
    for stn, tafs in grouped.items():
        tafs_sorted = sorted(
            tafs,
            key=lambda t: t.get("issue_time") or datetime.min,
            reverse=True,
        )
        grouped[stn] = [tafs_sorted[0]]

    # Fill in direct hits
    for code in clean_codes:
        if code in grouped:
            # take newest taf for this code
            direct_taf = dict(grouped[code][0])
            direct_taf["is_fallback"] = False
            direct_taf.pop("fallback_distance_nm", None)
            results[code] = [direct_taf]

    # 2. For any station that *still* has no TAF, try fallback
    for code in clean_codes:
        if results[code]:
            continue  # already have a TAF
        nearby = _fetch_nearby_taf_report(code)
        if nearby:
            results[code] = [nearby]

    return results
