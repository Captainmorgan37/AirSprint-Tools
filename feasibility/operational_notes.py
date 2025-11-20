"""Helpers for fetching FL3XX airport operational notes."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from fl3xx_api import Fl3xxApiConfig, fetch_operational_notes


def _normalize_note_payload(note: Mapping[str, Any]) -> Mapping[str, Any]:
    normalized = dict(note)
    text = normalized.get("note")
    if not isinstance(text, str) or not text.strip():
        for key in ("body", "title"):
            candidate = normalized.get(key)
            if isinstance(candidate, str) and candidate.strip():
                normalized["note"] = candidate.strip()
                break
    elif text.strip() != text:
        normalized["note"] = text.strip()
    return normalized


WIDE_RANGE_START = date(1900, 1, 1)
WIDE_RANGE_END = date(2100, 1, 1)


def fetch_airport_notes(
    config: Fl3xxApiConfig,
    icao: str,
    *,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
    session: Optional[Any] = None,
) -> List[Mapping[str, Any]]:
    """Fetch and normalize operational notes for an airport.

    FL3XX often stores notes without start/end dates. When a quote-specific date is
    available callers can pass a tight range (quote date through the following day). If a
    date is unavailable we fall back to a wide window to ensure timeless notes are
    returned. Results are normalized to always include a ``note`` field for downstream
    parsers.
    """

    code = (icao or "").strip().upper()
    if not code:
        return []

    start = from_date or WIDE_RANGE_START
    end = to_date or WIDE_RANGE_END
    if end <= start:
        end = start + timedelta(days=1)

    notes = fetch_operational_notes(
        config,
        code,
        from_date=start,
        to_date=end,
        session=session,
    )
    return [_normalize_note_payload(note) for note in notes if isinstance(note, Mapping)]


def build_operational_notes_fetcher(
    config: Fl3xxApiConfig,
) -> Callable[[str, Optional[str]], Sequence[Mapping[str, Any]]]:
    """Return a callable that fetches and caches airport operational notes."""

    cache: Dict[Tuple[str, date, date], List[Mapping[str, Any]]] = {}

    def fetcher(icao: str, _date_local: Optional[str]) -> Sequence[Mapping[str, Any]]:
        code = (icao or "").strip().upper()
        if not code:
            return []
        start = WIDE_RANGE_START
        end = WIDE_RANGE_END
        if _date_local:
            try:
                quote_date = date.fromisoformat(_date_local)
            except ValueError:
                quote_date = None
            else:
                start = quote_date
                end = quote_date + timedelta(days=2)
        key = (code, start, end)
        if key in cache:
            return cache[key]
        try:
            notes = fetch_airport_notes(config, code, from_date=start, to_date=end)
        except Exception:
            normalized: List[Mapping[str, Any]] = []
        else:
            normalized = notes
        cache[key] = normalized
        return normalized

    return fetcher
