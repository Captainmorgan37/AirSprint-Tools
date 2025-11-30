from __future__ import annotations

import html
import json
import math
import re
import os
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, cast

import pytz

import pandas as pd

import pydeck as pdk
import streamlit as st
import streamlit.components.v1 as components

from flight_leg_utils import (
    FlightDataError,
    build_fl3xx_api_config,
    load_airport_metadata_lookup,
    load_airport_tz_lookup,
    safe_parse_dt,
)
from fl3xx_api import fetch_flight_pax_details
from feasibility import (
    FeasibilityResult,
    run_feasibility_for_booking,
    run_feasibility_phase1,
)
from feasibility.operational_notes import build_operational_notes_fetcher
from feasibility.planning_notes import extract_planning_note_text
from feasibility.lookup import BookingLookupError
from feasibility.quote_lookup import (
    QuoteLookupError,
    fetch_quote_leg_options,
)
from Home import configure_page, password_gate, render_sidebar
from feasibility.models import FullFeasibilityResult
from reserve_calendar_checker import TARGET_DATES

_MAPBOX_TOKEN = st.secrets.get("mapbox_token")  # type: ignore[attr-defined]
if isinstance(_MAPBOX_TOKEN, str) and _MAPBOX_TOKEN.strip():
    os.environ["MAPBOX_API_KEY"] = _MAPBOX_TOKEN.strip()

configure_page(page_title="Feasibility Engine (Dev)")
password_gate()
render_sidebar()

st.title("🧮 DM Feasibility Engine")

st.write(
    """
    Run a DM-ready feasibility scan for pre-booking quote legs or confirmed bookings. Use the
    **Quote ID** tab when evaluating requests that have not yet become bookings, and the
    **Booking Identifier** tab for accepted trips. The engine evaluates aircraft performance,
    airport readiness, crew duty, trip planning, and overflight permit risks, then outputs a
    standardized summary you can paste into OS notes.
    """
)

STATUS_EMOJI = {"PASS": "✅", "CAUTION": "⚠️", "FAIL": "❌"}
SECTION_ORDER = [
    "suitability",
    "day_ops",
    "deice",
    "customs",
    "slot_ppr",
    "osa_ssa",
    "overflight",
    "operational_notes",
]
SECTION_LABELS = {
    "suitability": "Suitability",
    "day_ops": "Day Ops",
    "deice": "Deice",
    "customs": "Customs",
    "slot_ppr": "Slot / PPR",
    "osa_ssa": "OSA / SSA",
    "overflight": "Overflight",
    "operational_notes": "Other Operational Notes",
}
KEY_ISSUE_SECTIONS = {"customs", "day_ops", "deice", "overflight"}
SLOT_COPY_AIRPORTS = {"CYYZ", "CYUL", "CYYC", "CYVR"}
RESERVE_CALENDAR_DATES = set(TARGET_DATES)
_LEG_STYLES_INJECTED = False
_SLOT_COPY_STYLES_INJECTED = False
DEPARTURE_AIRPORT_KEYS = (
    "departure_airport",
    "dep_airport",
    "departureAirport",
    "airportFrom",
    "fromAirport",
)
DEPARTURE_TIME_KEYS = (
    "dep_time",
    "departureTime",
    "departureDateUTC",
    "departureDate",
)


def _extract_airport_code(flight: Mapping[str, Any]) -> Optional[str]:
    for key in DEPARTURE_AIRPORT_KEYS:
        value = flight.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().upper()
    return None


def _extract_departure_time(flight: Mapping[str, Any]) -> Optional[str]:
    for key in DEPARTURE_TIME_KEYS:
        value = flight.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _get_departure_local_date(flight: Optional[Mapping[str, Any]]) -> Optional[date]:
    """Return the local departure calendar date for ``flight`` if available."""

    if not isinstance(flight, Mapping):
        return None

    departure_time = _extract_departure_time(flight)
    if not departure_time:
        return None

    try:
        dep_dt = safe_parse_dt(departure_time)
    except Exception:
        return None

    tz_name: Optional[str] = None
    dep_airport = _extract_airport_code(flight)
    if dep_airport:
        tz_lookup = load_airport_tz_lookup()
        tz_candidate = tz_lookup.get(dep_airport)
        if isinstance(tz_candidate, str) and tz_candidate.strip():
            tz_name = tz_candidate.strip()

    if tz_name:
        try:
            dep_dt = dep_dt.astimezone(pytz.timezone(tz_name))
        except Exception:
            dep_dt = dep_dt.astimezone(pytz.UTC)
    else:
        dep_dt = dep_dt.astimezone(pytz.UTC)

    return dep_dt.date()


def _is_reserve_calendar_departure(flight: Optional[Mapping[str, Any]]) -> bool:
    dep_date = _get_departure_local_date(flight)
    return dep_date in RESERVE_CALENDAR_DATES if dep_date else False


def _find_reserve_calendar_dates(
    legs: Sequence[Mapping[str, Any]] | None,
) -> list[date]:
    """Return sorted reserve calendar dates represented within ``legs``."""

    if not legs:
        return []

    matches: set[date] = set()
    for leg in legs:
        dep_date = _get_departure_local_date(leg)
        if dep_date and dep_date in RESERVE_CALENDAR_DATES:
            matches.add(dep_date)
    return sorted(matches)


def _find_reserve_calendar_dates(
    legs: Sequence[Mapping[str, Any]] | None,
) -> list[date]:
    """Return sorted reserve calendar dates represented within ``legs``."""

    if not legs:
        return []

    matches: set[date] = set()
    for leg in legs:
        if _is_reserve_calendar_departure(leg):
            departure_time = _extract_departure_time(leg)
            if departure_time:
                try:
                    dep_dt = safe_parse_dt(departure_time)
                except Exception:
                    continue
                matches.add(dep_dt.date())
    return sorted(matches)


def _is_club_owner_booking(flight: Optional[Mapping[str, Any]]) -> bool:
    if not isinstance(flight, Mapping):
        return False

    note = extract_planning_note_text(flight)
    if not note:
        return False

    normalized = note.lower()
    return "club owner" in normalized or "owner club" in normalized


def status_icon(status: str) -> str:
    return STATUS_EMOJI.get(status, "❔")


@st.cache_data(show_spinner=False)
def _load_fl3xx_settings() -> Dict[str, Any]:
    try:
        secrets_section = st.secrets.get("fl3xx_api")  # type: ignore[attr-defined]
    except Exception:
        secrets_section = None
    if isinstance(secrets_section, Mapping):
        return {str(key): secrets_section[key] for key in secrets_section}
    if isinstance(secrets_section, dict):
        return dict(secrets_section)
    return {}


def _build_operational_notes_fetcher() -> Optional[
    Callable[[str, Optional[str]], Sequence[Mapping[str, Any]]]
]:
    config = st.session_state.get("feasibility_fl3xx_config")
    if config is None:
        settings = _load_fl3xx_settings()
        if not settings:
            return None
        try:
            config = build_fl3xx_api_config(dict(settings))
        except FlightDataError:
            return None
        st.session_state["feasibility_fl3xx_config"] = config
    try:
        return build_operational_notes_fetcher(config)
    except Exception:
        return None


def _build_pax_details_fetcher() -> Optional[Callable[[str], Mapping[str, Any]]]:
    config = st.session_state.get("feasibility_fl3xx_config")
    if config is None:
        settings = _load_fl3xx_settings()
        if not settings:
            return None
        try:
            config = build_fl3xx_api_config(dict(settings))
        except FlightDataError:
            return None
        st.session_state["feasibility_fl3xx_config"] = config

    def fetcher(flight_id: str) -> Mapping[str, Any]:
        return fetch_flight_pax_details(config, flight_id)

    return fetcher


def _run_feasibility(booking_identifier: str) -> Optional[FeasibilityResult]:
    if not booking_identifier:
        st.warning("Enter a booking identifier to continue.")
        return None

    settings = _load_fl3xx_settings()
    try:
        config = build_fl3xx_api_config(dict(settings))
    except FlightDataError as exc:
        st.error(str(exc))
        return None
    st.session_state["feasibility_fl3xx_config"] = config

    cache = st.session_state.setdefault("feasibility_lookup_cache", {})
    with st.spinner("Fetching flight and running feasibility checks…"):
        try:
            result = run_feasibility_for_booking(config, booking_identifier, cache=cache)
        except BookingLookupError as exc:
            st.warning(str(exc))
            return None
        except Exception as exc:  # pragma: no cover - safety net for Streamlit UI
            st.exception(exc)
            return None
    return result


def _load_quote_options(quote_id: str) -> list[Dict[str, Any]]:
    if not quote_id:
        st.warning("Enter a Quote ID to continue.")
        return []

    settings = _load_fl3xx_settings()
    try:
        config = build_fl3xx_api_config(dict(settings))
    except FlightDataError as exc:
        st.error(str(exc))
        return []
    st.session_state["feasibility_fl3xx_config"] = config

    with st.spinner("Fetching quote and legs from FL3XX…"):
        try:
            options, payload = fetch_quote_leg_options(config, quote_id)
        except QuoteLookupError as exc:
            st.warning(str(exc))
            return []
        except Exception as exc:  # pragma: no cover - defensive UI guard
            st.exception(exc)
            return []

    st.session_state["feasibility_quote_payload"] = payload
    st.success(f"Loaded {len(options)} leg(s) for quote {quote_id}.")
    return options


def _run_full_quote_day(quote: Mapping[str, Any]) -> Optional[FullFeasibilityResult]:
    request_payload: Dict[str, Any] = {"quote": quote}
    fetcher = _build_operational_notes_fetcher()
    if fetcher:
        request_payload["operational_notes_fetcher"] = fetcher
    pax_fetcher = _build_pax_details_fetcher()
    if pax_fetcher:
        request_payload["pax_details_fetcher"] = pax_fetcher
    with st.spinner("Running feasibility checks for entire quote day…"):
        try:
            return run_feasibility_phase1(request_payload)
        except Exception as exc:  # pragma: no cover - UI safeguard
            st.exception(exc)
            return None


def _lookup_airport_coordinates(
    icao: str, *, metadata: Mapping[str, Mapping[str, object]]
) -> Optional[Tuple[float, float]]:
    record = metadata.get(icao.upper()) if metadata else None
    if not isinstance(record, Mapping):
        return None

    try:
        latitude = float(record.get("lat"))  # type: ignore[arg-type]
        longitude = float(record.get("lon"))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None

    if pd.isna(latitude) or pd.isna(longitude):
        return None

    return latitude, longitude


def _format_time(value: Any) -> Optional[str]:
    text = str(value).strip()
    if not text:
        return None
    digits = re.sub(r"\D", "", text)
    if len(digits) == 3:
        digits = f"0{digits}"
    if len(digits) != 4:
        return None
    hours = digits[:2]
    minutes = digits[2:]
    return f"{hours}:{minutes}"


def _format_customs_hours(entries: Any) -> Optional[str]:
    if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
        return None

    formatted: list[str] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        start = _format_time(entry.get("start"))
        end = _format_time(entry.get("end"))
        if not start and not end:
            continue
        range_text = "–".join(filter(None, [start, end]))
        days_value = entry.get("days")
        days: list[str] = []
        if isinstance(days_value, Sequence) and not isinstance(days_value, (str, bytes)):
            days = [str(value).strip() for value in days_value if str(value).strip()]
        if days:
            formatted.append(f"{'/'.join(days)} {range_text}".strip())
        else:
            formatted.append(range_text)

    if not formatted:
        return None

    summary = formatted[0]
    if len(formatted) > 1:
        summary += f" (+{len(formatted) - 1} more)"
    return summary


def _format_after_hours(parsed_customs: Mapping[str, Any]) -> str:
    if parsed_customs.get("customs_afterhours_available"):
        return "Yes"
    if parsed_customs.get("customs_afterhours_not_available"):
        return "No"
    return "Unknown"


def _build_airport_point(
    icao: str,
    *,
    lat: float,
    lon: float,
    color: list[int],
    customs: Mapping[str, Any] | None,
    country: str,
) -> Dict[str, Any]:
    icao_clean = str(icao).strip().upper()
    is_canada = country.lower().startswith("canada") if country else False
    is_us = country.lower() in {"united states", "united states of america", "usa", "us"}
    customs_available = isinstance(customs, Mapping) and bool(customs.get("customs_available"))
    badge = None
    label = icao_clean or "?"
    tooltip_lines: list[str] = [f"<b>{icao_clean}</b>"]

    if customs_available and isinstance(customs, Mapping):
        aoe_type = customs.get("aoe_type")
        hours = _format_customs_hours(customs.get("customs_hours")) or "Unknown"
        if is_canada:
            if isinstance(aoe_type, str) and aoe_type.strip():
                badge = f"✦ {aoe_type.strip()}"
            else:
                badge = "✦ AOE"
            label = f"{icao} {badge}" if badge else icao
            if badge:
                tooltip_lines.append(badge)
            tooltip_lines.append(f"Open Dates/Hours: {hours}")
        elif is_us:
            tooltip_lines.append(f"Open Dates/Hours: {hours}")
            tooltip_lines.append(f"After Hours Available? {_format_after_hours(customs)}")
        else:
            tooltip_lines.append(f"Customs available ({hours})")
    return {
        "icao": icao_clean,
        "label": label,
        "lat": lat,
        "lon": lon,
        "color": color,
        "tooltip": "<br/>".join(tooltip_lines),
        "customs_available": customs_available,
        "customs_badge": badge,
        "customs_radius": 55000,
    }


def _build_route_map_payload(
    legs: Sequence[Mapping[str, Any]] | Any,
) -> Optional[Dict[str, Any]]:
    if not isinstance(legs, Sequence) or isinstance(legs, (str, bytes)) or not legs:
        return None

    metadata = load_airport_metadata_lookup()
    if not metadata:
        return None

    customs_by_airport: dict[str, Mapping[str, Any]] = {}

    ordered_airports: List[str] = []
    for leg in legs:
        if not isinstance(leg, Mapping):
            continue
        departure = leg.get("departure", {}) if isinstance(leg, Mapping) else {}
        arrival = leg.get("arrival", {}) if isinstance(leg, Mapping) else {}
        for side_data in (departure, arrival):
            if not isinstance(side_data, Mapping):
                continue
            icao_code = side_data.get("icao") if isinstance(side_data, Mapping) else None
            parsed_customs = side_data.get("parsed_customs_notes") if isinstance(side_data, Mapping) else None
            if isinstance(icao_code, str) and isinstance(parsed_customs, Mapping):
                customs_by_airport.setdefault(icao_code.strip().upper(), parsed_customs)
        dep_code = departure.get("icao") if isinstance(departure, Mapping) else None
        arr_code = arrival.get("icao") if isinstance(arrival, Mapping) else None
        if isinstance(dep_code, str) and dep_code.strip():
            dep_clean = dep_code.strip().upper()
            if not ordered_airports or ordered_airports[-1] != dep_clean:
                ordered_airports.append(dep_clean)
        if isinstance(arr_code, str) and arr_code.strip():
            arr_clean = arr_code.strip().upper()
            if not ordered_airports or ordered_airports[-1] != arr_clean:
                ordered_airports.append(arr_clean)

    coordinates: List[Tuple[float, float]] = []
    airport_points: List[Dict[str, Any]] = []
    seen_airports: set[str] = set()

    for index, airport in enumerate(ordered_airports):
        coords = _lookup_airport_coordinates(airport, metadata=metadata)
        if not coords:
            continue
        lat, lon = coords
        coordinates.append((lat, lon))
        if airport in seen_airports:
            continue
        seen_airports.add(airport)
        if index == 0:
            color = [255, 200, 0]
        elif index == len(ordered_airports) - 1:
            color = [0, 200, 255]
        else:
            color = [120, 220, 255]

        customs = customs_by_airport.get(airport)
        country = str(metadata.get(airport, {}).get("country") or "").strip()
        airport_points.append(
            _build_airport_point(
                airport,
                lat=lat,
                lon=lon,
                color=color,
                customs=customs if isinstance(customs, Mapping) else None,
                country=country,
            )
        )

    if len(coordinates) < 2:
        return None

    lats = [lat for lat, _ in coordinates]
    lons = [lon for _, lon in coordinates]
    center = (sum(lats) / len(lats), sum(lons) / len(lons))

    path = [[lon, lat] for lat, lon in coordinates]

    return {"airports": airport_points, "path": path, "center": center}


def _estimate_zoom_level(path: Sequence[Sequence[float]]) -> float:
    if not path:
        return 3.5

    def _distance_nm(point_a: Sequence[float], point_b: Sequence[float]) -> float:
        if len(point_a) < 2 or len(point_b) < 2:
            return 0.0
        lon1, lat1 = map(math.radians, point_a[:2])
        lon2, lat2 = map(math.radians, point_b[:2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return 3440.065 * c

    total_distance = 0.0
    for idx in range(len(path) - 1):
        total_distance += _distance_nm(path[idx], path[idx + 1])

    if total_distance > 3500:
        return 2.1
    if total_distance > 2000:
        return 2.6
    if total_distance > 1200:
        return 3.1
    if total_distance > 600:
        return 3.6
    if total_distance > 250:
        return 4.1
    if total_distance > 80:
        return 4.6
    return 5.1


def _build_route_segments(path: Sequence[Sequence[float]]) -> List[Mapping[str, Any]]:
    if not path or len(path) < 2:
        return []

    def _gradient_color(position: float) -> List[int]:
        """Interpolate from cyan -> blue -> white."""

        anchors = [
            (0.0, (0, 220, 255)),
            (0.5, (30, 120, 255)),
            (1.0, (255, 255, 255)),
        ]
        for idx in range(1, len(anchors)):
            start_pos, start_color = anchors[idx - 1]
            end_pos, end_color = anchors[idx]
            if position <= end_pos:
                span = end_pos - start_pos
                if span <= 0:
                    return list(end_color)
                factor = (position - start_pos) / span
                return [
                    int(start_color[i] + (end_color[i] - start_color[i]) * factor)
                    for i in range(3)
                ]
        return list(anchors[-1][1])

    segments: List[Mapping[str, Any]] = []
    num_segments = len(path) - 1
    for idx in range(num_segments):
        progress = idx / max(num_segments - 1, 1)
        segments.append(
            {
                "path": [path[idx], path[idx + 1]],
                "color": _gradient_color(progress),
                "width": 1.8,
                "name": f"Leg {idx + 1}",
                "tooltip": f"Leg {idx + 1}",
            }
        )
    return segments


def st_flight_route_map(route_data: Mapping[str, Any], *, height: int = 430) -> None:
    center = route_data.get("center")
    path = route_data.get("path")
    airports = route_data.get("airports")
    if not (
        isinstance(center, tuple)
        and len(center) == 2
        and isinstance(path, Sequence)
        and isinstance(airports, Sequence)
    ):
        st.caption("Route map unavailable.")
        return

    latitude, longitude = center
    zoom = _estimate_zoom_level(path) if isinstance(path, Sequence) else 3.5
    segments = _build_route_segments(path) if isinstance(path, Sequence) else []

    route_shadow_layer = pdk.Layer(
        "PathLayer",
        [{"path": path, "name": "Route shadow", "width": 2.5}],
        get_path="path",
        get_color=[5, 5, 5, 120],
        get_width="width",
        width_scale=12,
        width_min_pixels=6,
        pickable=False,
        rounded=True,
    )

    route_glow_layer = pdk.Layer(
        "PathLayer",
        [{"path": path, "name": "Route glow", "width": 2.2}],
        get_path="path",
        get_color=[80, 200, 255, 120],
        get_width="width",
        width_scale=10,
        width_min_pixels=5,
        pickable=False,
        rounded=True,
    )

    route_layer = pdk.Layer(
        "PathLayer",
        segments if segments else [{"path": path, "color": [0, 180, 255], "width": 2.0}],
        get_path="path",
        get_color="color",
        get_width="width",
        width_scale=10,
        width_min_pixels=3,
        auto_highlight=True,
        highlight_color=[255, 255, 255, 180],
        pickable=True,
        rounded=True,
    )

    customs_airports = [airport for airport in airports if airport.get("customs_available")]
    customs_layer = None
    if customs_airports:
        customs_layer = pdk.Layer(
            "ScatterplotLayer",
            customs_airports,
            get_position=["lon", "lat"],
            get_fill_color=[255, 214, 102, 70],
            get_line_color=[255, 214, 102, 160],
            stroked=True,
            get_line_width=2200,
            get_radius="customs_radius",
            radius_min_pixels=24,
            line_width_min_pixels=2,
            pickable=True,
            opacity=0.35,
        )

    airports_layer = pdk.Layer(
        "ScatterplotLayer",
        airports,
        get_position=["lon", "lat"],
        get_fill_color="color",
        get_radius=25000,
        pickable=True,
    )

    view_state = pdk.ViewState(
        latitude=latitude,
        longitude=longitude,
        zoom=zoom,
        bearing=0,
        pitch=35,
    )

    layers = [route_shadow_layer, route_glow_layer, route_layer]
    if customs_layer:
        layers.append(customs_layer)
    layers.append(airports_layer)

    deck = pdk.Deck(
        layers=layers,
        initial_view_state=view_state,
        tooltip={"html": "{tooltip}"},
        map_style="mapbox://styles/mapbox/dark-v10",
    )

    st.pydeck_chart(deck, use_container_width=True, height=height)


def _format_minutes(total_minutes: Optional[int]) -> str:
    if total_minutes is None:
        return "n/a"
    hours, minutes = divmod(int(total_minutes), 60)
    return f"{hours:d}h {minutes:02d}m"


def _format_note_text(note: Any) -> str:
    if isinstance(note, str):
        return note.strip()
    if isinstance(note, Mapping):
        for key in ("note", "body", "title", "category", "type"):
            value = note.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return str(note)
    if note is None:
        return ""
    return str(note)


def _render_raw_operational_notes(notes: Sequence[Any] | Any) -> None:
    if not isinstance(notes, Sequence) or isinstance(notes, (str, bytes)) or not notes:
        return

    note_texts: list[str] = []
    alert_flags: list[bool] = []
    for note in notes:
        alert_flag = False
        if isinstance(note, Mapping):
            alert_flag = bool(note.get("alert"))
        note_texts.append(_format_note_text(note))
        alert_flags.append(alert_flag)

    st.markdown("**FL3XX Operational Notes**")
    dataframe = pd.DataFrame({"Note": note_texts})

    def _highlight_alert(row: pd.Series) -> list[str]:
        if alert_flags[row.name]:
            return [
                "background-color: rgba(220, 38, 38, 0.18); color: #ef4444; font-weight: 600;"
            ]
        return [""]

    styled = dataframe.style.apply(_highlight_alert, axis=1)
    st.dataframe(styled, use_container_width=True, hide_index=True)


def _format_hours_entry(entry: Mapping[str, Any]) -> Optional[str]:
    start = str(entry.get("start") or entry.get("closed_from") or "").strip()
    end = str(entry.get("end") or entry.get("closed_to") or "").strip()
    days_value = entry.get("days")
    days: list[str] = []
    if isinstance(days_value, Sequence) and not isinstance(days_value, (str, bytes)):
        for value in days_value:
            if isinstance(value, str) and value.strip() and value.strip().lower() != "unknown":
                days.append(value.strip())
    hours = f"{start}-{end}" if start and end else start or end
    if days and hours:
        return f"{'/'.join(days)} {hours}"
    if hours:
        return hours
    if days:
        return "/".join(days)
    return None


def _format_slot_window(entry: Mapping[str, Any]) -> Optional[str]:
    days_value = entry.get("days")
    days: list[str] = []
    if isinstance(days_value, Sequence) and not isinstance(days_value, (str, bytes)):
        for value in days_value:
            if isinstance(value, str) and value.strip():
                days.append(value.strip())
    start = str(entry.get("start") or "").strip()
    end = str(entry.get("end") or "").strip()
    if not start and not end and not days:
        return None
    window = f"{start}-{end}" if start and end else start or end or ""
    if days and window:
        return f"{'/'.join(days)} {window}"
    if days:
        return "/".join(days)
    return window or None


def _explode_note_text(text: str) -> list[str]:
    normalized = text.replace("•", "\n")
    lines: list[str] = []
    for chunk in normalized.splitlines():
        cleaned = chunk.strip(" -•\t")
        if cleaned:
            lines.append(cleaned)
    return lines


def _normalize_entry(entry: str) -> str:
    return " ".join(entry.split()).casefold()


def _collect_entries(values: Any, *, explode: bool = False) -> list[str]:
    entries: list[str] = []
    seen: set[str] = set()
    if isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
        for value in values:
            if not isinstance(value, str):
                continue
            if explode:
                exploded = _explode_note_text(value)
                for entry in exploded:
                    key = _normalize_entry(entry)
                    if key and key not in seen:
                        seen.add(key)
                        entries.append(entry)
            else:
                cleaned = value.strip()
                key = _normalize_entry(cleaned)
                if cleaned and key not in seen:
                    seen.add(key)
                    entries.append(cleaned)
    return entries


def _render_bullet_section(title: str, lines: Sequence[str]) -> None:
    entries: list[str] = []
    seen: set[str] = set()
    for line in lines:
        if not isinstance(line, str):
            continue
        cleaned = line.strip()
        key = _normalize_entry(cleaned)
        if not cleaned or key in seen:
            continue
        seen.add(key)
        entries.append(cleaned)
    if not entries:
        return
    st.markdown(f"**{title}**")
    for entry in entries:
        st.markdown(f"- {entry}")


def _render_customs_details(
    parsed: Mapping[str, Any] | None, *, planned_time_local: Optional[str] = None
) -> None:
    if not isinstance(parsed, Mapping):
        return
    if not parsed.get("raw_notes"):
        return
    contact_notes = _collect_entries(parsed.get("customs_contact_notes"), explode=True)
    _render_bullet_section("Contact Instructions", contact_notes)
    _render_bullet_section(
        "Crew Requirements",
        _collect_entries(parsed.get("crew_requirements"), explode=True),
    )


def _render_operational_restrictions(parsed: Mapping[str, Any] | None) -> None:
    if not isinstance(parsed, Mapping):
        return
    if not parsed.get("raw_notes"):
        return
    with st.expander("Parsed operational intel (for status)", expanded=False):
        summary_lines: list[str] = []
        if parsed.get("slot_required"):
            lead: list[str] = []
            if parsed.get("slot_lead_days"):
                lead.append(f"{parsed['slot_lead_days']} day lead")
            if parsed.get("slot_lead_hours"):
                lead.append(f"{parsed['slot_lead_hours']} hour lead")
            detail = "Slot required"
            if lead:
                detail += f" ({', '.join(lead)})"
            summary_lines.append(detail)
        if parsed.get("slot_time_windows"):
            windows: list[str] = []
            for entry in parsed.get("slot_time_windows", []):
                if isinstance(entry, Mapping):
                    formatted = _format_slot_window(entry)
                    if formatted:
                        windows.append(formatted)
            if windows:
                summary_lines.append(f"Slot windows: {', '.join(windows)}")
        if parsed.get("ppr_required"):
            lead: list[str] = []
            if parsed.get("ppr_lead_days"):
                lead.append(f"{parsed['ppr_lead_days']} day notice")
            if parsed.get("ppr_lead_hours"):
                lead.append(f"{parsed['ppr_lead_hours']} hour notice")
            detail = "PPR required"
            if lead:
                detail += f" ({', '.join(lead)})"
            summary_lines.append(detail)
        if parsed.get("deice_unavailable"):
            summary_lines.append("Deice NOT available per notes")
        elif parsed.get("deice_limited"):
            summary_lines.append("Deice limited in notes")
        if parsed.get("winter_sensitivity"):
            summary_lines.append("Winter sensitivity / contamination risk")
        if parsed.get("fuel_available") is False:
            summary_lines.append("Fuel unavailable per notes")
        if parsed.get("night_ops_allowed") is False:
            summary_lines.append("Night operations prohibited")
        if parsed.get("curfew"):
            curfew = parsed.get("curfew")
            if isinstance(curfew, Mapping):
                start = curfew.get("from") or curfew.get("start") or curfew.get("closed_from")
                end = curfew.get("to") or curfew.get("end") or curfew.get("closed_to")
                window = f"{start}-{end}" if start and end else start or end or "in effect"
                summary_lines.append(f"Curfew: {window}")
            else:
                summary_lines.append("Curfew in effect")
        hours_entries: list[str] = []
        for entry in parsed.get("hours_of_operation", []):
            if isinstance(entry, Mapping):
                formatted = _format_hours_entry(entry)
                if formatted:
                    hours_entries.append(formatted)
        if hours_entries:
            summary_lines.append(f"Hours: {', '.join(hours_entries)}")
        _render_bullet_section("Operational Intel", summary_lines)
        _render_bullet_section("Deice Notes", _collect_entries(parsed.get("deice_notes"), explode=True))
        _render_bullet_section("Winter Notes", _collect_entries(parsed.get("winter_notes"), explode=True))
        _render_bullet_section(
            "Weather Limitations",
            _collect_entries(parsed.get("weather_limitations"), explode=True),
        )
        _render_bullet_section("Slot Notes", _collect_entries(parsed.get("slot_notes"), explode=True))
        _render_bullet_section("PPR Notes", _collect_entries(parsed.get("ppr_notes"), explode=True))
        _render_bullet_section("Hours / Curfew Notes", _collect_entries(parsed.get("hour_notes"), explode=True))
        _render_bullet_section(
            "Runway Limits",
            _collect_entries(parsed.get("runway_limitations"), explode=True),
        )
        _render_bullet_section(
            "Aircraft Type Limits",
            _collect_entries(parsed.get("aircraft_type_limits"), explode=True),
        )
        _render_bullet_section(
            "Other Operational Restrictions",
            _collect_entries(parsed.get("generic_restrictions"), explode=True),
        )


def _render_category_block(
    label: str, category: Mapping[str, Any], *, expanded: bool | None = None
) -> None:
    status = str(category.get("status", "PASS"))
    summary = category.get("summary") or status
    issues = [str(issue) for issue in category.get("issues", []) if issue]
    extra_note = None
    if "Operational Notes" in label and issues:
        extra_note = issues[0]

    detail_suffix = f"  \n - {extra_note}" if extra_note else ""
    st.markdown(f"**{label}:** {status_icon(status)} {summary}{detail_suffix}")
    expanded_state = expanded if expanded is not None else status != "PASS"
    if issues:
        with st.expander(f"{label} details", expanded=expanded_state):
            for issue in issues:
                st.markdown(f"- {issue}")


def _render_aircraft_category(
    category: Mapping[str, Any] | None, *, expanded: bool | None = None
) -> None:
    if not isinstance(category, Mapping):
        return
    status = str(category.get("status", "PASS"))
    summary = category.get("summary") or status
    header = f"{status_icon(status)} Aircraft – {summary}"
    issues = [str(issue) for issue in category.get("issues", []) if issue]
    expanded_state = expanded if expanded is not None else status != "PASS"
    with st.expander(header, expanded=expanded_state):
        st.write(f"Status: **{status}**")
        if issues:
            for issue in issues:
                st.markdown(f"- {issue}")
        else:
            st.write("No issues recorded.")


def _leg_visual_tokens(label: str) -> Mapping[str, str]:
    side_lower = label.lower()
    is_departure = side_lower == "departure"
    return {
        "accent": "#2E84D0" if is_departure else "#5EBA7D",
        "band": "#E7F1FB" if is_departure else "#E7F7EE",
        "border": "rgba(46, 132, 208, 0.25)" if is_departure else "rgba(94, 186, 125, 0.25)",
        "icon": "⬆️" if is_departure else "⬇️",
        "label": label.upper(),
    }


def _inject_leg_styles() -> None:
    global _LEG_STYLES_INJECTED
    if _LEG_STYLES_INJECTED:
        return
    st.markdown(
        """
        <style>
            .leg-card {
                border-radius: 14px;
                border: 1px solid var(--leg-border, #d0d7de);
                margin-bottom: 1rem;
                overflow: hidden;
                box-shadow: 0 6px 20px rgba(0, 0, 0, 0.06);
            }
            .leg-card__band {
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 0.75rem;
                padding: 0.75rem 1rem;
                background: var(--leg-band, #eef3f8);
                border-bottom: 1px solid var(--leg-border, #d0d7de);
            }
            .leg-card__left {
                display: flex;
                align-items: center;
                gap: 0.75rem;
            }
            .leg-card__icon {
                font-size: 1.4rem;
                line-height: 1;
            }
            .leg-card__title {
                font-size: 0.9rem;
                font-weight: 800;
                letter-spacing: 0.06em;
                color: #0b1f33;
            }
            .leg-card__icao {
                font-size: 1.1rem;
                font-weight: 700;
                color: #0b1f33;
            }
            .leg-card__chips {
                display: flex;
                align-items: center;
                gap: 0.4rem;
                flex-wrap: wrap;
            }
            .leg-card__chip {
                display: inline-flex;
                align-items: center;
                gap: 0.35rem;
                padding: 0.3rem 0.85rem;
                border-radius: 999px;
                background: rgba(255, 255, 255, 0.65);
                border: 1px solid var(--leg-border, #d0d7de);
                font-weight: 700;
                font-size: 1.05rem;
                color: #0b1f33;
            }
            .leg-card__chip-outline {
                background: transparent;
                border: 1px dashed var(--leg-border, #d0d7de);
            }
            .leg-card__body {
                position: relative;
                padding: 1rem 1rem 0.25rem 1.25rem;
                background: radial-gradient(circle at 20% 20%, rgba(255,255,255,0.9), #f8fbff);
            }
            .leg-card__timeline {
                position: absolute;
                inset: 0 auto 0 0.35rem;
                width: 6px;
                border-radius: 999px;
                background: linear-gradient(180deg, var(--leg-accent, #4b7bec), rgba(255,255,255,0));
                opacity: 0.75;
            }
            .leg-card__body > div {
                position: relative;
                z-index: 1;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
    _LEG_STYLES_INJECTED = True


def _render_leg_side(label: str, side: Mapping[str, Any]) -> None:
    _inject_leg_styles()
    tokens = _leg_visual_tokens(label)
    icao = side.get("icao", "???") if isinstance(side, Mapping) else "???"
    planned_time_local = None
    header_source: Optional[str] = None
    if isinstance(side, Mapping):
        planned_value = side.get("planned_time_local") or side.get("plannedTimeLocal")
        if planned_value:
            planned_time_local = str(planned_value)

        header_source = planned_time_local or side.get("local_date")

    header_chip = header_source or "Local time pending"

    if header_source:
        try:
            tz_abbrev: Optional[str] = None
            dt_source = header_source.strip()
            tz_match = re.match(r"^(.*?)(?:\s+([A-Za-z]{2,5}))?$", dt_source)
            if tz_match:
                dt_source = tz_match.group(1).strip()
                tz_abbrev = tz_match.group(2)

            parsed_dt = safe_parse_dt(dt_source)
            weekday = parsed_dt.strftime("%A")
            month = parsed_dt.strftime("%B")
            day = parsed_dt.day
            year = parsed_dt.year
            tz_abbrev = tz_abbrev or parsed_dt.strftime("%Z") or "UTC"

            def _ordinal(n: int) -> str:
                if 10 <= n % 100 <= 20:
                    suffix = "th"
                else:
                    suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
                return f"{n}{suffix}"

            header_chip = f"{weekday}, {month} {_ordinal(day)}, {year} {parsed_dt.strftime('%H:%M')} {tz_abbrev}"
        except Exception:
            pass
    with st.container():
        st.markdown(
            f"""
            <div class="leg-card" style="--leg-accent: {tokens['accent']}; --leg-band: {tokens['band']}; --leg-border: {tokens['border']};">
                <div class="leg-card__band">
                    <div class="leg-card__left">
                        <span class="leg-card__icon">{tokens['icon']}</span>
                        <div>
                            <div class="leg-card__title">{tokens['label']}</div>
                            <div class="leg-card__icao">{icao}</div>
                        </div>
                    </div>
                    <div class="leg-card__chips">
                        <span class="leg-card__chip">{header_chip}</span>
                    </div>
                </div>
                <div class="leg-card__body">
                    <div class="leg-card__timeline"></div>
                    <div>
            """,
            unsafe_allow_html=True,
        )

        for key in SECTION_ORDER:
            display = SECTION_LABELS.get(key, key.title())
            category = side.get(key) if isinstance(side, Mapping) else None
            if isinstance(category, Mapping):
                _render_category_block(
                    display,
                    category,
                    expanded=False if key == "operational_notes" else None,
                )
        parsed_customs = side.get("parsed_customs_notes") if isinstance(side, Mapping) else None
        _render_customs_details(
            parsed_customs if isinstance(parsed_customs, Mapping) else None,
            planned_time_local=planned_time_local,
        )
        parsed_ops = side.get("parsed_operational_restrictions") if isinstance(side, Mapping) else None
        _render_operational_restrictions(parsed_ops if isinstance(parsed_ops, Mapping) else None)
        raw_notes = side.get("raw_operational_notes") if isinstance(side, Mapping) else None
        _render_raw_operational_notes(raw_notes)

        st.markdown(
            """
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _collect_key_issues(result: Mapping[str, Any]) -> List[str]:
    issues: List[str] = []
    duty = result.get("duty") if isinstance(result, Mapping) else None
    if isinstance(duty, Mapping):
        duty_status = duty.get("status", "PASS")
        if duty_status in {"CAUTION", "FAIL"}:
            summary = duty.get("summary") or f"Duty {duty_status.title()}"
            issues.append(f"Duty: {summary}")

    legs = result.get("legs") if isinstance(result, Mapping) else None
    if isinstance(legs, Sequence):
        for index, leg in enumerate(legs, start=1):
            if not isinstance(leg, Mapping):
                continue
            aircraft = leg.get("aircraft")
            if isinstance(aircraft, Mapping):
                status = aircraft.get("status", "PASS")
                if status in {"CAUTION", "FAIL"}:
                    summary = aircraft.get("summary") or status
                    issues.append(f"Leg {index} Aircraft: {summary}")
            weight_balance = leg.get("weightBalance")
            if isinstance(weight_balance, Mapping):
                status = weight_balance.get("status", "PASS")
                if status in {"CAUTION", "FAIL"}:
                    summary = weight_balance.get("summary") or status
                    issues.append(f"Leg {index} Weight & Balance: {summary}")
            for side_name in ("departure", "arrival"):
                side = leg.get(side_name)
                if not isinstance(side, Mapping):
                    continue
                icao = side.get("icao", "???")
                for key in SECTION_ORDER:
                    category = side.get(key)
                    if not isinstance(category, Mapping):
                        continue
                    status = category.get("status", "PASS")
                    if status == "PASS":
                        continue
                    display = SECTION_LABELS.get(key, key.title())
                    summary = category.get("summary") or status
                    label = f"{side_name.title()} {icao} {display}"
                    if status == "FAIL" or (status == "CAUTION" and key in KEY_ISSUE_SECTIONS):
                        issues.append(f"{label}: {summary}")
    return issues


def _format_slot_json_time(planned_time_local: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not planned_time_local:
        return None, None
    try:
        parsed_dt = safe_parse_dt(str(planned_time_local))
    except Exception:
        return None, None

    formatted_date = parsed_dt.strftime("%d%b").upper()
    time_label = parsed_dt.strftime("%H%M")

    return formatted_date, time_label


def _collect_slot_copy_payloads(result: Mapping[str, Any]) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    legs = result.get("legs") if isinstance(result, Mapping) else None
    default_aircraft = str(result.get("aircraft_type") or "").upper() if isinstance(result, Mapping) else ""

    if not isinstance(legs, Sequence):
        return payloads

    for index, leg in enumerate(legs, start=1):
        if not isinstance(leg, Mapping):
            continue
        leg_aircraft = str(leg.get("aircraft_type") or default_aircraft).upper()
        for side_key, operation in (("departure", "departure"), ("arrival", "arrival")):
            side = leg.get(side_key)
            if not isinstance(side, Mapping):
                continue
            icao = str(side.get("icao") or "").upper()
            if icao not in SLOT_COPY_AIRPORTS:
                continue

            slot_ppr = side.get("slot_ppr") if isinstance(side, Mapping) else None
            slot_status = slot_ppr.get("status") if isinstance(slot_ppr, Mapping) else None
            parsed_restrictions = side.get("parsed_operational_restrictions")
            slot_required = bool(parsed_restrictions.get("slot_required")) if isinstance(parsed_restrictions, Mapping) else False

            if slot_status != "FAIL" and not slot_required:
                continue

            planned_time_local = side.get("planned_time_local") or side.get("plannedTimeLocal")
            local_date, local_time = _format_slot_json_time(planned_time_local)

            counterpart = leg.get("arrival" if operation == "departure" else "departure")
            other_airport = None
            if isinstance(counterpart, Mapping):
                other_airport_value = counterpart.get("icao") or counterpart.get("airport")
                other_airport = str(other_airport_value).upper() if other_airport_value else None

            payload: dict[str, object] = {
                "label": f"Leg {index} {operation.title()} {icao}",
                "json": {
                    "operation": operation,
                    "airport": icao,
                },
            }

            if local_date:
                payload["json"]["date"] = local_date
            if local_time:
                payload["json"]["time"] = local_time
            if other_airport:
                payload["json"]["other_airport"] = other_airport
                if operation == "departure":
                    payload["json"]["dest"] = other_airport
                else:
                    payload["json"]["orig"] = other_airport
            if leg_aircraft:
                payload["json"]["ac_type"] = leg_aircraft

            payloads.append(payload)

    return payloads


def _inject_slot_copy_styles() -> None:
    global _SLOT_COPY_STYLES_INJECTED
    if _SLOT_COPY_STYLES_INJECTED:
        return

    st.markdown(
        """
        <style>
            .slot-copy-banner {
                display: flex;
                align-items: center;
                justify-content: flex-end;
                gap: 0.5rem;
                padding: 0.35rem 0.6rem;
                background: linear-gradient(120deg, #0f172a, #111827);
                border-radius: 12px;
                box-shadow: 0 12px 24px rgba(0, 0, 0, 0.18);
            }
            .slot-copy-button {
                background: linear-gradient(135deg, #f97316, #fb923c);
                color: #0b1f33;
                border: none;
                padding: 0.55rem 0.9rem;
                border-radius: 999px;
                font-weight: 800;
                letter-spacing: 0.01em;
                font-size: 0.92rem;
                box-shadow: 0 8px 18px rgba(249, 115, 22, 0.32);
                cursor: pointer;
                transition: transform 120ms ease, box-shadow 120ms ease, background 120ms ease;
                width: auto;
                white-space: nowrap;
            }
            .slot-copy-button:hover {
                transform: translateY(-1px);
                box-shadow: 0 12px 22px rgba(249, 115, 22, 0.48);
                background: linear-gradient(135deg, #fb923c, #f97316);
            }
            .slot-copy-status {
                font-size: 0.85rem;
                font-weight: 700;
                color: #e5e7eb;
                text-shadow: 0 1px 2px rgba(0, 0, 0, 0.35);
            }
            .slot-copy-label {
                color: #cbd5e1;
                font-weight: 600;
                margin-right: auto;
                display: flex;
                align-items: center;
                gap: 0.25rem;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
    _SLOT_COPY_STYLES_INJECTED = True


def _render_slot_copy_controls(container, payloads: Sequence[Mapping[str, object]]) -> None:
    if not payloads:
        return

    _inject_slot_copy_styles()
    options = list(payloads)
    selected_payload = options[0]
    widget_suffix = st.session_state.get("slot_copy_counter", 0) + 1
    st.session_state["slot_copy_counter"] = widget_suffix
    button_id = f"slot-copy-btn-{widget_suffix}"
    text_id = f"slot-copy-text-{widget_suffix}"
    status_id = f"slot-copy-status-{widget_suffix}"

    if len(options) > 1:
        labels = [str(item.get("label", f"Option {idx+1}")) for idx, item in enumerate(options)]
        selected_label = container.selectbox("Slot JSON target", labels, key="slot-json-target")
        label_map = {label: payload for label, payload in zip(labels, options)}
        selected_payload = label_map.get(selected_label, selected_payload)

    payload_json = selected_payload.get("json") if isinstance(selected_payload, Mapping) else None
    if not isinstance(payload_json, Mapping):
        return

    json_text = json.dumps(payload_json, indent=2)
    escaped_json = html.escape(json_text)
    safe_json_for_js = json.dumps(json_text)

    with container:
        components.html(
            f"""
            <div class="slot-copy-banner">
                <span class="slot-copy-label">📋 OCS Slot JSON</span>
                <button id="{button_id}" class="slot-copy-button" type="button">
                    📋 Copy OCS Slot JSON
                </button>
                <span id="{status_id}" class="slot-copy-status"></span>
                <textarea id="{text_id}" style="position:absolute; left:-1000px; top:-1000px; height:1px; width:1px;">
                    {escaped_json}
                </textarea>
            </div>
            <script>
                (function() {{
                    const button = document.getElementById("{button_id}");
                    const textArea = document.getElementById("{text_id}");
                    const status = document.getElementById("{status_id}");
                    if (!button || !textArea) return;

                    const setStatus = (label, isError = false) => {{
                        if (status) {{
                            status.textContent = label;
                            status.style.color = isError ? "#fecdd3" : "#bbf7d0";
                        }}
                        const original = button.dataset.label || button.innerText;
                        button.innerText = label;
                        setTimeout(() => (button.innerText = original), 1600);
                    }};

                    const fallbackCopy = (value) => {{
                        try {{
                            textArea.value = value;
                            textArea.removeAttribute("disabled");
                            textArea.style.position = "absolute";
                            textArea.style.left = "-1000px";
                            textArea.style.top = "-1000px";
                            textArea.style.width = "1px";
                            textArea.style.height = "1px";
                            textArea.select();
                            textArea.setSelectionRange(0, value.length);
                            const successful = document.execCommand("copy");
                            setStatus(successful ? "Copied! ✅" : "Copy failed", !successful);
                        }} catch (err) {{
                            setStatus("Copy failed", true);
                        }}
                    }};

                    const copyPayload = () => {{
                        const value = {safe_json_for_js};
                        if (navigator.clipboard && window.isSecureContext) {{
                            navigator.clipboard.writeText(value).then(
                                () => setStatus("Copied! ✅"),
                                () => fallbackCopy(value)
                            );
                            return;
                        }}
                        fallbackCopy(value);
                    }};

                    button.dataset.label = button.innerText;
                    button.addEventListener("click", copyPayload);
                }})();
            </script>
            """,
            height=78,
        )
    container.caption(
        f"Copies the slot request payload for **{selected_payload.get('label', 'selected leg')}** to your clipboard.",
        help="Paste directly into OCS after clicking the copy button.",
    )


def _collect_operational_note_highlights(
    legs: Sequence[Mapping[str, Any]] | Any,
) -> List[Mapping[str, str]]:
    highlights: List[Mapping[str, str]] = []
    if not isinstance(legs, Sequence):
        return highlights

    for index, leg in enumerate(legs, start=1):
        if not isinstance(leg, Mapping):
            continue
        departure = leg.get("departure", {}) if isinstance(leg, Mapping) else {}
        arrival = leg.get("arrival", {}) if isinstance(leg, Mapping) else {}
        dep_code = departure.get("icao", "???") if isinstance(departure, Mapping) else "???"
        arr_code = arrival.get("icao", "???") if isinstance(arrival, Mapping) else "???"
        leg_label = f"Leg {index} ({dep_code}→{arr_code})"

        for side_name, side_label in (("departure", "Departure"), ("arrival", "Arrival")):
            side = leg.get(side_name)
            if not isinstance(side, Mapping):
                continue
            operational_notes = side.get("operational_notes")
            if not isinstance(operational_notes, Mapping):
                continue
            status = operational_notes.get("status", "PASS")
            if status == "PASS":
                continue
            summary = operational_notes.get("summary") or status
            issues = [str(issue) for issue in operational_notes.get("issues", []) if issue]
            first_issue = issues[0] if issues else ""
            highlights.append(
                {
                    "leg_label": leg_label,
                    "side_label": side_label,
                    "summary": str(summary),
                    "issue": first_issue,
                }
            )

    return highlights


def _format_workflow_label(result: Mapping[str, Any]) -> Optional[str]:
    workflow = str(result.get("workflow") or "").strip()
    workflow_custom = str(result.get("workflow_custom_name") or "").strip()
    if workflow_custom and workflow and workflow_custom.lower() != workflow.lower():
        return f"{workflow_custom} ({workflow})"
    return workflow_custom or workflow or None


def _render_full_quote_result(result: FullFeasibilityResult) -> None:
    legs = result.get("legs", [])
    duty = result.get("duty", {})
    overall_status = result.get("overall_status", "PASS")
    emoji = STATUS_EMOJI.get(overall_status, "")
    workflow_label = _format_workflow_label(result)

    route_map_payload = _build_route_map_payload(legs)

    st.markdown("---")
    summary_col, map_col = st.columns([1.6, 1])

    with summary_col:
        if workflow_label:
            st.markdown(
                f"<div style='font-weight:800; font-size:1.1rem; color:#d97706;'>Workflow: {workflow_label}</div>",
                unsafe_allow_html=True,
            )
        st.subheader(f"{emoji} Full Quote Day Status: {overall_status}")
        st.caption(
            f"{result.get('bookingIdentifier', 'Unknown Quote')} • {len(legs)} leg(s) • {result.get('aircraft_type', 'Unknown Aircraft')}"
        )
        flight_category = result.get("flight_category")
        if flight_category:
            st.caption(f"Flight Category: {flight_category}")

        reserve_dates = _find_reserve_calendar_dates(legs)
        if reserve_dates:
            formatted_dates = ", ".join(date_obj.strftime("%Y-%m-%d") for date_obj in reserve_dates)
            st.warning(
                f"Reserve calendar day detected ({formatted_dates}). Ensure club workflows and availability are confirmed."
            )

        summary = result.get("summary")
        if summary:
            formatted = summary.strip().replace("\n", "  \n")
            st.markdown(formatted)

        validation_issues = [
            str(issue).strip()
            for issue in result.get("validation_checks", [])
            if isinstance(issue, str) and issue.strip()
        ]
        validation_failures = {
            str(issue).strip()
            for issue in result.get("issues", [])
            if isinstance(issue, str) and issue.strip()
        }
        st.markdown("**Validation Checks**")
        if validation_issues:
            for entry in validation_issues:
                if entry in validation_failures:
                    st.markdown(
                        f"- <span style='color:#b91c1c'>{entry}</span>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(f"- {entry}")
        else:
            st.caption(
                "Planning note routes and requested aircraft type align with the quoted legs."
            )

        operational_note_flags = _collect_operational_note_highlights(legs)
        if operational_note_flags:
            st.markdown("**Operational Notes Flags**")
            for entry in operational_note_flags:
                st.markdown(
                    f"{entry['leg_label']} {entry['side_label']} Operational Notes: {entry['summary']}"
                )
                if entry["issue"]:
                    st.markdown(f" • {entry['issue']}")

        key_issues = _collect_key_issues(result)
        slot_copy_payloads = _collect_slot_copy_payloads(result) if overall_status == "FAIL" else []
        if slot_copy_payloads:
            header_col, button_col = st.columns([1.2, 1])
            with header_col:
                st.subheader("Key Issues")
            with button_col:
                _render_slot_copy_controls(button_col, slot_copy_payloads)
        else:
            st.subheader("Key Issues")

        if key_issues:
            for issue in key_issues:
                st.markdown(f"- {issue}")
        else:
            st.caption(
                "No customs, day-ops, deice, duty, or permit cautions detected."
            )

    with map_col:
        st.markdown("##### Route preview")
        if route_map_payload:
            st_flight_route_map(route_map_payload)
        else:
            st.caption("Route map unavailable for this quote.")

    with st.expander("Duty Day Evaluation", expanded=duty.get("status") != "PASS"):
        status = duty.get("status", "PASS")
        col1, col2, col3 = st.columns(3)
        col1.metric("Duty Status", f"{status_icon(status)} {status}")
        col2.metric("Total Duty", _format_minutes(duty.get("total_duty")))
        col3.metric("Turn Segments", len(duty.get("turn_times", [])))
        st.write(f"- Start: {duty.get('duty_start_local') or 'Unknown'}")
        st.write(f"- End: {duty.get('duty_end_local') or 'Unknown'}")
        if duty.get("split_duty_possible"):
            st.write("- Split duty window available (≥ 6h ground).")
        if duty.get("reset_duty_possible"):
            st.write("- Reset possible (≥ 11h15 ground).")
        if duty.get("issues"):
            st.write("- Issues:")
            for entry in duty.get("issues", []):
                st.write(f"  • {entry}")

    for index, leg in enumerate(legs, start=1):
        departure = leg.get("departure", {}) if isinstance(leg, Mapping) else {}
        arrival = leg.get("arrival", {}) if isinstance(leg, Mapping) else {}
        aircraft = leg.get("aircraft") if isinstance(leg, Mapping) else None
        weight_balance = leg.get("weightBalance") if isinstance(leg, Mapping) else None
        dep_code = departure.get("icao", "???")
        arr_code = arrival.get("icao", "???")
        dep_local_date = _get_departure_local_date(leg)
        reserve_flag = dep_local_date in RESERVE_CALENDAR_DATES if dep_local_date else False
        header_suffix = " (Reserve Calendar Day)" if reserve_flag else ""
        header = f"Leg {index}: {dep_code} → {arr_code}{header_suffix}"
        with st.expander(header, expanded=False):
            if reserve_flag and dep_local_date:
                st.warning(
                    f"Departing on reserve calendar day {dep_local_date.strftime('%Y-%m-%d')}."
                    " Confirm club availability and workflows before release."
                )
            if isinstance(aircraft, Mapping):
                _render_aircraft_category(aircraft, expanded=False)
            if isinstance(weight_balance, Mapping):
                wb_status = weight_balance.get("status", "PASS")
                wb_summary = weight_balance.get("summary") or wb_status
                header = f"{status_icon(wb_status)} Weight & Balance – {wb_summary}"
                with st.expander(header, expanded=False):
                    st.write(f"Status: **{wb_status}**")
                    _render_weight_balance_details(weight_balance)
            _render_leg_side("Departure", departure)
            _render_leg_side("Arrival", arrival)

    with st.expander("Raw full quote result"):
        st.json(result)

quote_tab, booking_tab = st.tabs(["Quote ID", "Booking Identifier"])

with quote_tab:
    st.subheader("Search via Quote ID")
    st.caption(
        "Use this to evaluate feasibility before a booking exists. The dev engine always runs"
        " every leg in the quote so you consistently get duty-day coverage; expand the legs"
        " in the results below for per-segment breakdowns."
    )

    with st.form("quote-form", clear_on_submit=False):
        quote_input = st.text_input("Quote ID", placeholder="e.g. 3621613").strip()
        quote_submitted = st.form_submit_button("Load Quote")

    if quote_submitted:
        options = _load_quote_options(quote_input)
        if options:
            st.session_state["feasibility_quote_options"] = options
        quote_payload = st.session_state.get("feasibility_quote_payload")
        if isinstance(quote_payload, Mapping):
            st.session_state["feasibility_loaded_quote_id"] = quote_input
            st.session_state["feasibility_should_run_full_quote"] = True

    quote_options = st.session_state.get("feasibility_quote_options", [])
    quote_payload = st.session_state.get("feasibility_quote_payload")

    if quote_options:
        st.markdown("**Loaded Legs**")
        for option in quote_options:
            leg_info = option.get("leg", {}) if isinstance(option, Mapping) else {}
            label = option.get("label", "Leg") if isinstance(option, Mapping) else "Leg"
            pax = leg_info.get("pax") or "n/a"
            block = leg_info.get("blockTime") or leg_info.get("flightTime") or "n/a"
            st.caption(f"{label}: PAX {pax} · Block {block} minutes")
    else:
        st.info("Load a quote to view available legs for feasibility analysis.")

    quote_loaded = isinstance(quote_payload, Mapping)

    if quote_loaded:
        quote_reserve_dates = _find_reserve_calendar_dates(
            quote_payload.get("legs") if isinstance(quote_payload, Mapping) else None
        )
        if quote_reserve_dates:
            formatted_dates = ", ".join(
                date_obj.strftime("%Y-%m-%d") for date_obj in quote_reserve_dates
            )
            st.warning(
                f"Reserve calendar day detected in quote legs ({formatted_dates})."
                " Expect club workflows and confirm availability before proceeding."
            )
    with st.expander("Loaded quote payload"):
        if quote_loaded:
            st.json(quote_payload)
        else:
            st.caption("Load a quote to view the payload and enable multi-leg checks.")

    st.markdown("#### Evaluate Full Quote Day")
    if not quote_loaded:
        st.info("Load a quote to enable multi-leg feasibility checks.")

    run_full_quote = st.button(
        "Run Feasibility for Quote (All Legs)",
        key="run-full-quote",
        type="primary",
        disabled=not quote_loaded,
        help="Feasibility now runs automatically after loading a quote; use this to rerun manually.",
    )

    should_run_full_quote = st.session_state.pop(
        "feasibility_should_run_full_quote", False
    )

    if (run_full_quote or should_run_full_quote) and quote_loaded:
        full_day_result = _run_full_quote_day(quote_payload)
        if full_day_result:
            st.session_state["feasibility_last_full_quote_result"] = full_day_result

with booking_tab:
    st.subheader("Search via Booking Identifier")
    with st.form("booking-form", clear_on_submit=False):
        booking_input = st.text_input("Booking Identifier", placeholder="e.g. ILARD").strip().upper()
        submitted = st.form_submit_button("Run Feasibility")

    if submitted:
        result = _run_feasibility(booking_input)
        if result:
            st.session_state["feasibility_last_result"] = result

stored_result = st.session_state.get("feasibility_last_result")
full_quote_result = st.session_state.get("feasibility_last_full_quote_result")

def _render_category(name: str, category) -> None:
    emoji = STATUS_EMOJI.get(category.status, "")
    header = f"{emoji} {name.title()} – {category.summary or category.status}"
    with st.expander(header, expanded=category.status != "PASS"):
        st.write(f"Status: **{category.status}**")
        if category.issues:
            st.markdown("\n".join(f"- {issue}" for issue in category.issues))
        else:
            st.write("No issues recorded.")

        if name == "weightBalance":
            _render_weight_balance_details(category)


def _render_weight_balance_details(category) -> None:
    details = None
    if isinstance(category, Mapping):
        details = category.get("details")
    else:
        details = getattr(category, "details", None)
    if not isinstance(details, Mapping):
        return

    payload = {
        "Season": details.get("season"),
        "PAX Weight": details.get("paxWeight"),
        "Cargo Weight": details.get("cargoWeight"),
        "Total Payload": details.get("totalPayload"),
        "Max Allowed": details.get("maxAllowed"),
        "PAX Count": details.get("paxCount"),
    }

    metrics = [
        ("Season", payload["Season"]),
        ("PAX Weight", payload["PAX Weight"]),
        ("Cargo Weight", payload["Cargo Weight"]),
        ("Total Payload", payload["Total Payload"]),
        ("Max Allowed", payload["Max Allowed"]),
        ("PAX Count", payload["PAX Count"]),
    ]

    cols = st.columns(3)
    for idx, (label, value) in enumerate(metrics):
        col = cols[idx % 3]
        if value is None:
            col.metric(label, "n/a")
        else:
            col.metric(label, value)

    breakdown = details.get("paxBreakdown") if isinstance(details, Mapping) else None
    if isinstance(breakdown, Mapping) and breakdown:
        st.markdown("**Passenger Weights Applied**")
        cols = st.columns(4)
        for idx, label in enumerate(["Male", "Female", "Child", "Infant"]):
            col = cols[idx]
            count = breakdown.get(label, 0)
            col.metric(label, count)

    pax_keys = details.get("paxPayloadKeys") if isinstance(details, Mapping) else None
    if pax_keys:
        st.caption(f"PAX payload keys: {pax_keys}")

    with st.expander("Debug: Raw weight balance details"):
        st.json(details)


if stored_result and isinstance(stored_result, FeasibilityResult):
    overall_emoji = STATUS_EMOJI.get(stored_result.overall_status, "")
    st.subheader(f"{overall_emoji} Overall Status: {stored_result.overall_status}")
    st.caption(f"Generated at {stored_result.timestamp}")

    if _is_reserve_calendar_departure(stored_result.flight):
        if _is_club_owner_booking(stored_result.flight):
            st.error("Club owner booking on reserve calendar day.")
        else:
            st.warning("Flight taking place on reserve calendar day.")

    for name, category in stored_result.categories.items():
        _render_category(name, category)

    st.markdown("### Notes for OS")
    st.code(stored_result.notes_for_os or "No notes", language="markdown")

    with st.expander("Raw result JSON"):
        st.json(stored_result.as_dict(include_flight=False))

    if stored_result.flight:
        with st.expander("Source flight payload"):
            st.json(stored_result.flight)
else:
    st.info("Load a quote or submit a booking identifier to generate a feasibility report.")

if isinstance(full_quote_result, Mapping):
    _render_full_quote_result(cast(FullFeasibilityResult, full_quote_result))
