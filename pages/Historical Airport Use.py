"""Streamlit app to summarize historical airport usage."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import date, timedelta
from typing import Any, Dict, Iterable, Mapping, Tuple

import pandas as pd
import streamlit as st

from Home import configure_page, password_gate, render_sidebar
from fl3xx_api import fetch_flights
from flight_leg_utils import (
    ARRIVAL_AIRPORT_COLUMNS,
    DEPARTURE_AIRPORT_COLUMNS,
    build_fl3xx_api_config,
    filter_out_subcharter_rows,
    load_airport_metadata_lookup,
    normalize_fl3xx_payload,
)
from historical_airport_use_utils import (
    airport_country_code,
    airport_matches_focus,
    extract_airport_code,
    is_positioning_leg,
    leg_duration_hours,
)

configure_page(page_title="Historical Airport Use")
password_gate()
render_sidebar()

st.title("🧾 Historical Airport Use")
st.write(
    """
    Pull historical flights from FL3XX and tally departure airports across every leg.
    The fetch runs in ~3-day chunks to avoid API limits, ignores add/remove line
    placeholders, and filters out subcharters.
    """
)

CHUNK_DAYS = 3
FOCUS_OPTIONS = {
    "Atlantic Canada": "atlantic_canada",
    "Caribbean": "caribbean",
    "Europe": "europe",
}


def _settings_digest(settings: Mapping[str, Any]) -> str:
    normalised = json.dumps(settings, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(normalised.encode("utf-8")).hexdigest()


def _chunk_ranges(start: date, end_inclusive: date) -> Iterable[Tuple[date, date]]:
    end_exclusive = end_inclusive + timedelta(days=1)
    current = start
    while current < end_exclusive:
        chunk_end = min(current + timedelta(days=CHUNK_DAYS), end_exclusive)
        yield current, chunk_end
        current = chunk_end


def _normalise_date_range(selection: Any, default_start: date, default_end: date) -> Tuple[date, date]:
    if isinstance(selection, tuple):
        if len(selection) == 2:
            start, end = selection
        elif len(selection) == 1:
            start = end = selection[0]
        else:
            start, end = default_start, default_end
    else:
        start = end = selection if isinstance(selection, date) else default_start

    if start > end:
        start, end = end, start

    return start, end


_PLACEHOLDER_PREFIXES = {"ADD", "REMOVE"}


def _is_add_line_leg(leg: Mapping[str, Any]) -> bool:
    tail_value = leg.get("tail")
    if tail_value is None:
        return False
    tail_text = str(tail_value).strip()
    if not tail_text:
        return False
    first_word = tail_text.split()[0].upper()
    return first_word in _PLACEHOLDER_PREFIXES


def _filter_out_add_lines(rows: Iterable[Mapping[str, Any]]) -> Tuple[list[dict[str, Any]], int]:
    filtered: list[dict[str, Any]] = []
    skipped = 0
    for row in rows:
        if _is_add_line_leg(row):
            skipped += 1
            continue
        filtered.append(dict(row))
    return filtered, skipped


@st.cache_data(show_spinner=True, ttl=300, hash_funcs={dict: lambda _: "0"})
def _load_legs(
    settings_digest: str,
    _settings: Dict[str, Any],
    *,
    from_date: date,
    to_date: date,
) -> Tuple[list[dict[str, Any]], Dict[str, Any]]:
    _ = settings_digest
    settings = dict(_settings)
    config = build_fl3xx_api_config(settings)
    flights: list[dict[str, Any]] = []
    chunk_meta: list[dict[str, Any]] = []

    for chunk_start, chunk_end in _chunk_ranges(from_date, to_date):
        chunk_flights, meta = fetch_flights(config, from_date=chunk_start, to_date=chunk_end)
        flights.extend(chunk_flights)
        chunk_meta.append(meta)

    normalized_rows, normalization_stats = normalize_fl3xx_payload({"items": flights})
    non_subcharter_rows, skipped_subcharter = filter_out_subcharter_rows(normalized_rows)
    filtered_rows, skipped_add = _filter_out_add_lines(non_subcharter_rows)

    metadata = {
        "flights_returned": len(flights),
        "legs_after_filter": len(filtered_rows),
        "skipped_subcharter": skipped_subcharter,
        "skipped_add_lines": skipped_add,
        "chunks": chunk_meta,
        "normalization": normalization_stats,
    }
    return filtered_rows, metadata


def _enrich_legs_for_analysis(legs: list[dict[str, Any]], airport_lookup: Mapping[str, Mapping[str, Any]]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for leg in legs:
        dep_code = extract_airport_code(leg, ("departure_airport",) + tuple(DEPARTURE_AIRPORT_COLUMNS))
        arr_code = extract_airport_code(leg, ("arrival_airport",) + tuple(ARRIVAL_AIRPORT_COLUMNS))
        duration = leg_duration_hours(leg)
        records.append(
            {
                "dep_time": leg.get("dep_time"),
                "arrival_time": leg.get("arrival_time"),
                "tail": leg.get("tail"),
                "flightType": leg.get("flightType") or leg.get("flight_type"),
                "dep_airport": dep_code,
                "arr_airport": arr_code,
                "dep_country": airport_country_code(dep_code, airport_lookup),
                "arr_country": airport_country_code(arr_code, airport_lookup),
                "is_pos": is_positioning_leg(leg),
                "duration_hours": duration,
            }
        )
    return pd.DataFrame(records)


def _is_pax_flight_type(value: Any) -> bool:
    text = str(value or "").strip().upper()
    return text == "PAX" or "PASSENGER" in text


def _build_average_flight_length_summary(
    analysis_df: pd.DataFrame,
    airport_lookup: Mapping[str, Mapping[str, Any]],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    regions = {"Overall": "overall", **FOCUS_OPTIONS}

    for label, focus_key in regions.items():
        subset = analysis_df
        if focus_key != "overall":
            subset = subset[
                subset.apply(
                    lambda row: airport_matches_focus(row["dep_airport"], airport_lookup, focus_key)
                    or airport_matches_focus(row["arr_airport"], airport_lookup, focus_key),
                    axis=1,
                )
            ]

        subset = subset[subset["duration_hours"].notna()]
        pax_subset = subset[subset["is_pax"]]
        pos_subset = subset[subset["is_pos"]]

        rows.append(
            {
                "Region": label,
                "PAX legs": int(len(pax_subset)),
                "Avg PAX flight length (hours)": round(float(pax_subset["duration_hours"].mean()), 2)
                if not pax_subset.empty
                else None,
                "POS legs": int(len(pos_subset)),
                "Avg POS flight length (hours)": round(float(pos_subset["duration_hours"].mean()), 2)
                if not pos_subset.empty
                else None,
            }
        )

    return pd.DataFrame(rows)


today = date.today()
default_start = date(today.year - 1, 1, 1)
default_end = date(today.year - 1, 12, 31)

with st.form("historical_airport_use_form"):
    date_selection = st.date_input(
        "Departure window",
        value=(default_start, default_end),
        help="Select the inclusive date range to scan. Defaults to last calendar year.",
    )
    submit_fetch = st.form_submit_button("Fetch historical usage", width="stretch")

if submit_fetch:
    settings = dict(st.secrets.get("fl3xx_api", {}))  # type: ignore[attr-defined]
    if not settings:
        st.error("FL3XX API credentials are missing. Update `.streamlit/secrets.toml` and try again.")
        st.stop()
    if not settings.get("api_token") and not settings.get("auth_header"):
        st.error(
            "FL3XX API credentials are incomplete. Ensure `api_token` or `auth_header` is set in Streamlit secrets."
        )
        st.stop()

    settings_digest = _settings_digest(settings)
    start_date, end_date = _normalise_date_range(date_selection, default_start, default_end)

    with st.spinner("Fetching flights in 3-day chunks..."):
        legs, metadata = _load_legs(settings_digest, settings, from_date=start_date, to_date=end_date)

    st.session_state["historical_airport_use_legs"] = legs
    st.session_state["historical_airport_use_metadata"] = metadata
    st.session_state["historical_airport_use_range"] = (start_date, end_date)

legs = st.session_state.get("historical_airport_use_legs")
metadata = st.session_state.get("historical_airport_use_metadata", {})
range_value = st.session_state.get("historical_airport_use_range", (default_start, default_end))

if legs:
    counts = Counter()
    for leg in legs:
        code = extract_airport_code(leg, ("departure_airport",) + tuple(DEPARTURE_AIRPORT_COLUMNS))
        if code:
            counts[code] += 1

    if not counts:
        st.info("No departure airport codes were found in the filtered legs.")
    else:
        table = (
            pd.DataFrame([{"Airport": code, "Departures": count} for code, count in counts.items()])
            .sort_values(["Departures", "Airport"], ascending=[False, True])
            .reset_index(drop=True)
        )

        st.subheader("Airport usage")
        st.dataframe(table, use_container_width=True, hide_index=True)

    airport_lookup = load_airport_metadata_lookup()
    analysis_df = _enrich_legs_for_analysis(legs, airport_lookup)
    analysis_df["is_pax"] = analysis_df["flightType"].apply(_is_pax_flight_type) & (~analysis_df["is_pos"])

    st.markdown("---")
    st.subheader("Average flight length")
    avg_length_df = _build_average_flight_length_summary(analysis_df, airport_lookup)
    st.dataframe(avg_length_df, use_container_width=True, hide_index=True)

    chart_df = avg_length_df.set_index("Region")[["Avg PAX flight length (hours)", "Avg POS flight length (hours)"]]
    st.bar_chart(chart_df)

    st.subheader("Advanced POS analysis")
    selected_focus = st.selectbox("Focus region", options=list(FOCUS_OPTIONS.keys()), index=0)
    focus_key = FOCUS_OPTIONS[selected_focus]

    pos_df = analysis_df[analysis_df["is_pos"]].copy()
    pos_focus_df = pos_df[
        pos_df.apply(
            lambda row: airport_matches_focus(row["dep_airport"], airport_lookup, focus_key)
            or airport_matches_focus(row["arr_airport"], airport_lookup, focus_key),
            axis=1,
        )
    ].copy()

    pos_hours = float(pos_focus_df["duration_hours"].dropna().sum()) if not pos_focus_df.empty else 0.0
    st.write(
        {
            "selected_focus": selected_focus,
            "pos_legs_total": int(len(pos_df)),
            "pos_legs_touching_focus": int(len(pos_focus_df)),
            "pos_hours_touching_focus": round(pos_hours, 2),
        }
    )

    st.caption("This lets you switch between Atlantic Canada, Caribbean, and Europe without re-fetching data.")

    if not pos_focus_df.empty:
        by_dep_airport = (
            pos_focus_df.groupby("dep_airport", dropna=True)
            .agg(pos_legs=("dep_airport", "size"), pos_hours=("duration_hours", "sum"))
            .reset_index()
            .sort_values(["pos_legs", "dep_airport"], ascending=[False, True])
        )
        by_country = (
            pos_focus_df.assign(country=pos_focus_df["dep_country"].fillna("Unknown"))
            .groupby("country")
            .agg(pos_legs=("country", "size"), pos_hours=("duration_hours", "sum"))
            .reset_index()
            .sort_values(["pos_legs", "country"], ascending=[False, True])
        )

        st.markdown("**POS focus legs by departure airport**")
        st.dataframe(by_dep_airport, use_container_width=True, hide_index=True)

        st.markdown("**POS focus legs by departure country**")
        st.dataframe(by_country, use_container_width=True, hide_index=True)

    st.download_button(
        "Download enriched legs CSV",
        data=analysis_df.to_csv(index=False).encode("utf-8"),
        file_name="historical_airport_use_enriched.csv",
        mime="text/csv",
    )

    st.markdown("---")
    st.subheader("Fetch details")
    start_date, end_date = range_value
    st.write(
        {
            "date_range": f"{start_date.isoformat()} → {end_date.isoformat()}",
            "legs_after_filter": metadata.get("legs_after_filter"),
            "flights_returned": metadata.get("flights_returned"),
            "skipped_subcharter": metadata.get("skipped_subcharter"),
            "skipped_add_lines": metadata.get("skipped_add_lines"),
            "chunks": len(metadata.get("chunks", [])),
        }
    )
elif submit_fetch:
    st.info("No matching flights were found for the selected window after filtering.")
