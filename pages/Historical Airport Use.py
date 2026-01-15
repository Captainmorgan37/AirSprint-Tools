"""Streamlit app to summarize historical airport usage."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import date, timedelta
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st

from Home import configure_page, password_gate, render_sidebar
from fl3xx_api import fetch_flights
from flight_leg_utils import (
    DEPARTURE_AIRPORT_COLUMNS,
    build_fl3xx_api_config,
    filter_out_subcharter_rows,
    normalize_fl3xx_payload,
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


def _coerce_airport_code(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, Mapping):
        for key in ("icao", "iata", "code", "airport", "name"):
            nested = value.get(key)
            if nested not in (None, ""):
                return _coerce_airport_code(nested)
        return None
    text = str(value).strip().upper()
    return text or None


def _extract_departure_code(leg: Mapping[str, Any]) -> Optional[str]:
    for key in ("departure_airport",) + tuple(DEPARTURE_AIRPORT_COLUMNS):
        if key in leg:
            code = _coerce_airport_code(leg.get(key))
            if code:
                return code
    return None


@st.cache_data(show_spinner=True, ttl=300, hash_funcs={dict: lambda _: "0"})
def _load_legs(
    settings_digest: str,
    settings: Dict[str, Any],
    *,
    from_date: date,
    to_date: date,
) -> Tuple[list[dict[str, Any]], Dict[str, Any]]:
    _ = settings_digest

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
    settings = dict(st.secrets)
    settings_digest = _settings_digest(settings)
    start_date, end_date = _normalise_date_range(date_selection, default_start, default_end)

    with st.spinner("Fetching flights in 3-day chunks..."):
        legs, metadata = _load_legs(settings_digest, settings, from_date=start_date, to_date=end_date)

    if not legs:
        st.info("No matching flights were found for the selected window after filtering.")
    else:
        counts = Counter()
        for leg in legs:
            code = _extract_departure_code(leg)
            if code:
                counts[code] += 1

        if not counts:
            st.info("No departure airport codes were found in the filtered legs.")
        else:
            table = (
                pd.DataFrame(
                    [
                        {"Airport": code, "Departures": count}
                        for code, count in counts.items()
                    ]
                )
                .sort_values(["Departures", "Airport"], ascending=[False, True])
                .reset_index(drop=True)
            )

            st.subheader("Airport usage")
            st.dataframe(table, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.subheader("Fetch details")
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
