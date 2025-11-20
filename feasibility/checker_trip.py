"""Trip planning checks covering Jeppesen / OSA / SSA rules."""

from __future__ import annotations

from typing import Any, List, Mapping, Optional, Sequence

from flight_leg_utils import load_airport_metadata_lookup

from jeppesen_itp_utils import (
    ALLOWED_COUNTRY_IDENTIFIERS,
    country_display_name,
    normalize_country_name,
)

from .airport_notes_parser import CUSTOMS_NOTE_KEYWORDS, ParsedCustoms, parse_customs_notes
from .common import (
    OSA_CATEGORY,
    SSA_CATEGORY,
    classify_flight_category,
    extract_airport_code,
    get_country_for_airport,
)
from .schemas import CategoryResult


def _is_high_risk_country(country: Optional[str]) -> bool:
    if not country:
        return False
    high_risk = {
        "RUSSIA",
        "CHINA",
        "SAUDI ARABIA",
        "IRAN",
        "IRAQ",
        "SYRIA",
        "CUBA",
    }
    return country.upper() in high_risk


def evaluate_trip(
    flight: Mapping[str, Any],
    *,
    airport_lookup: Optional[Mapping[str, Mapping[str, Optional[str]]]] = None,
) -> CategoryResult:
    lookup = airport_lookup or load_airport_metadata_lookup()

    dep = extract_airport_code(flight, arrival=False)
    arr = extract_airport_code(flight, arrival=True)

    dep_country = get_country_for_airport(dep, lookup)
    arr_country = get_country_for_airport(arr, lookup)

    issues: List[str] = []
    flags: List[str] = []

    classification = classify_flight_category(dep, arr, lookup)
    if classification.category in {SSA_CATEGORY, OSA_CATEGORY}:
        flags.append(f"{classification.category} sector; Jeppesen planning required.")
        issues.extend(classification.reasons)

    is_international = bool(dep_country and arr_country and dep_country != arr_country)
    if is_international:
        customs_texts = _extract_customs_note_texts(flight)
        if customs_texts:
            parsed_customs = parse_customs_notes(customs_texts)
            issues.extend(_summarize_customs_parser(parsed_customs))
            flags.extend(_customs_parser_flags(parsed_customs, customs_texts))

        jeppesen_note = _build_jeppesen_customs_note(dep_country, arr_country)
        if jeppesen_note:
            flags.append(jeppesen_note)

    for country in (dep_country, arr_country):
        if _is_high_risk_country(country):
            flags.append(f"Operations in {country} trigger Jeppesen oversight.")

    if flags:
        summary = flags[0]
        issues.extend(flags)
        return CategoryResult(status="CAUTION", summary=summary, issues=issues)

    return CategoryResult(status="PASS", summary="Trip planning in compliance", issues=issues or ["No Jeppesen triggers detected."])


def _extract_customs_note_texts(flight: Mapping[str, Any]) -> List[str]:
    candidates: List[str] = []
    note_fields = (
        "customsNotes",
        "customs_notes",
        "arrivalCustomsNotes",
        "departureCustomsNotes",
        "airportNotes",
        "operationalNotes",
    )
    for field in note_fields:
        value = flight.get(field)
        candidates.extend(_iter_note_texts(value))
    filtered: List[str] = []
    for text in candidates:
        lower = text.lower()
        if any(keyword in lower for keyword in CUSTOMS_NOTE_KEYWORDS):
            filtered.append(text)
    # Preserve order while removing duplicates.
    seen: set[str] = set()
    unique_texts: List[str] = []
    for note in filtered:
        if note not in seen:
            unique_texts.append(note)
            seen.add(note)
    return unique_texts


def _iter_note_texts(value: Any) -> List[str]:
    texts: List[str] = []
    if value is None:
        return texts
    if isinstance(value, str):
        text = value.strip()
        if text:
            texts.append(text)
        return texts
    if isinstance(value, Mapping):
        parts: List[str] = []
        for key in ("body", "note", "text", "title", "message"):
            entry = value.get(key)
            if isinstance(entry, str):
                entry_text = entry.strip()
                if entry_text:
                    parts.append(entry_text)
        if parts:
            texts.append("; ".join(parts))
        return texts
    if isinstance(value, Sequence):
        for item in value:
            texts.extend(_iter_note_texts(item))
        return texts
    text = str(value).strip()
    if text:
        texts.append(text)
    return texts


def _summarize_customs_parser(parsed: ParsedCustoms) -> List[str]:
    issues: List[str] = []
    if parsed.get("canpass_only"):
        issues.append("Operational notes: CANPASS-only clearance.")
        issues.extend(parsed.get("canpass_notes") or [])
    prior_hours = parsed.get("customs_prior_notice_hours")
    if prior_hours:
        issues.append(f"Customs requires {prior_hours} hours prior notice per notes.")
    prior_days = parsed.get("customs_prior_notice_days")
    if prior_days:
        issues.append(f"Customs requires {prior_days} day notice per notes.")
    if parsed.get("customs_afterhours_available"):
        issues.append("Afterhours customs available per notes; verify call-out requirements.")
        issues.extend(parsed.get("customs_afterhours_requirements") or [])
    if parsed.get("customs_contact_required"):
        issues.append("Customs contact required per notes.")
        issues.extend(parsed.get("customs_contact_notes") or [])
    location = parsed.get("location_to_clear")
    if location:
        issues.append(f"Clear customs at {location}.")
        issues.extend(parsed.get("location_notes") or [])
    issues.extend(parsed.get("pax_requirements") or [])
    issues.extend(parsed.get("crew_requirements") or [])
    return [issue for issue in issues if issue]


def _customs_parser_flags(parsed: ParsedCustoms, notes: Sequence[str]) -> List[str]:
    flags: List[str] = []
    if parsed.get("canpass_only"):
        flags.append("Customs parser indicates CANPASS-only clearance.")
    if _customs_notes_indicate_restriction(notes):
        flags.append("Customs notes indicate restrictions; confirm availability.")
    elif not parsed.get("customs_available"):
        flags.append("Customs parser could not confirm customs availability; verify entry procedures.")
    return flags


def _customs_notes_indicate_restriction(notes: Sequence[str]) -> bool:
    restriction_keywords = (
        "closed",
        "not available",
        "unavailable",
        "suspend",
        "suspended",
        "no customs",
        "no aoe",
        "cannot accept",
        "limited",
    )
    for note in notes:
        lower = note.lower()
        if any(keyword in lower for keyword in restriction_keywords):
            return True
    return False


def _build_jeppesen_customs_note(
    dep_country: Optional[str], arr_country: Optional[str]
) -> Optional[str]:
    outside_regions: List[str] = []
    missing_metadata = False
    for country in (dep_country, arr_country):
        normalized = normalize_country_name(country)
        if not normalized:
            missing_metadata = True
            continue
        if normalized not in ALLOWED_COUNTRY_IDENTIFIERS:
            display = country_display_name(country) or (country.upper() if country else "Unknown region")
            if display not in outside_regions:
                outside_regions.append(display)
    if outside_regions:
        if len(outside_regions) == 1:
            region_text = outside_regions[0]
        else:
            region_text = ", ".join(outside_regions)
        return f"International sector includes {region_text}; confirm Jeppesen customs support."
    if missing_metadata:
        return "International sector includes an unknown region; confirm Jeppesen customs support."
    return None
