"""Helpers for fetching FL3XX airport operational notes."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
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


def build_operational_notes_fetcher(
    config: Fl3xxApiConfig,
) -> Callable[[str, Optional[str]], Sequence[Mapping[str, Any]]]:
    """Return a callable that fetches and caches airport operational notes."""

    cache: Dict[Tuple[str, str], List[Mapping[str, Any]]] = {}

    def fetcher(icao: str, _date_local: Optional[str]) -> Sequence[Mapping[str, Any]]:
        code = (icao or "").strip().upper()
        if not code:
            return []
        today = datetime.now(timezone.utc).date()
        tomorrow = today + timedelta(days=1)
        cache_key = (code, today.isoformat())
        if cache_key in cache:
            return cache[cache_key]
        try:
            notes = fetch_operational_notes(
                config,
                code,
                from_date=today,
                to_date=tomorrow,
            )
        except Exception:
            normalized: List[Mapping[str, Any]] = []
        else:
            normalized = [_normalize_note_payload(note) for note in notes if isinstance(note, Mapping)]
        cache[cache_key] = normalized
        return normalized

    return fetcher
