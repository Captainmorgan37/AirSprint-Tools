"""Helpers for classifying and parsing airport operational notes."""

from __future__ import annotations

import re
from typing import Mapping, Sequence, Tuple, TypedDict

from .schemas import CategoryResult, CategoryStatus


class ParsedRestrictions(TypedDict):
    winter_sensitivity: bool
    winter_notes: list[str]

    deice_limited: bool
    deice_unavailable: bool
    deice_notes: list[str]

    fuel_available: bool | None
    fuel_notes: list[str]

    ppr_required: bool
    ppr_lead_days: int | None
    ppr_lead_hours: int | None
    ppr_notes: list[str]

    slot_required: bool
    slot_lead_days: int | None
    slot_lead_hours: int | None
    slot_validity_minutes: int | None
    slot_time_windows: list[dict[str, object]]
    slot_notes: list[str]

    hours_of_operation: list[dict[str, object]]
    night_ops_allowed: bool | None
    curfew: dict[str, str] | None
    hour_notes: list[str]

    runway_limitations: list[str]
    aircraft_type_limits: list[str]
    surface_contamination: bool

    weather_limitations: list[str]
    generic_restrictions: list[str]
    raw_notes: list[str]


class ParsedCustoms(TypedDict):
    customs_available: bool
    customs_hours: list[dict[str, object]]
    customs_afterhours_available: bool
    customs_afterhours_not_available: bool
    customs_afterhours_requirements: list[str]
    customs_prior_notice_hours: int | None
    customs_prior_notice_days: int | None
    customs_contact_required: bool
    customs_contact_notes: list[str]

    aoe_type: str | None
    aoe_notes: list[str]

    canpass_only: bool
    canpass_notes: list[str]

    location_to_clear: str | None
    location_notes: list[str]

    pax_requirements: list[str]
    crew_requirements: list[str]

    general_customs_notes: list[str]
    raw_notes: list[str]


_STATUS_PRIORITY: Mapping[CategoryStatus, int] = {
    "PASS": 0,
    "INFO": 1,
    "CAUTION": 2,
    "FAIL": 3,
}

SLOT_KEYWORDS = ("slot", "slots", "reservation", "reservations", "ecvrs", "ocs")
WINTER_PATTERNS = (
    re.compile(r"\bwinter\b", re.IGNORECASE),
    re.compile(r"\bsnow\b", re.IGNORECASE),
    re.compile(r"\bice\b", re.IGNORECASE),
    re.compile(r"\bcontaminated\b", re.IGNORECASE),
    re.compile(r"\brwy contamination\b", re.IGNORECASE),
    re.compile(r"\blimited winter maintenance\b", re.IGNORECASE),
)
FUEL_KEYWORDS = ("fuel not available", "fuel unavailable", "no fuel")
DEICE_KEYWORDS = ("deice", "antice", "de-ice")
NIGHT_KEYWORDS = ("night ops", "no night ops", "night operations", "night landings", "curfew", "sunset")
RUNWAY_KEYWORDS = ("rwy", "runway", "landings", "departures")
WEATHER_LIMITATION_KEYWORDS = (
    "good weather only",
    "vfr only",
    "vfr weather",
    "visual flight rules",
    "visual conditions only",
    "vmc only",
)
RUNWAY_RESTRICTION_TERMS = (
    "closed",
    "clsd",
    "closure",
    "not available",
    "unavailable",
    "restricted",
    "limitation",
)
RUNWAY_CONTAMINATION_TERMS = ("contaminated", "contamination")
NON_RESTRICTIVE_GENERIC_PATTERNS = (
    re.compile(r"\bturn time\b", re.IGNORECASE),
    re.compile(r"\bclosest airport\b", re.IGNORECASE),
    re.compile(r"^notes?:", re.IGNORECASE),
    re.compile(r"\bfbo information\b", re.IGNORECASE),
)
CUSTOMS_NOTE_KEYWORDS = (
    "customs",
    "canpass",
    "aoe",
    "cbsa",
    "cbp",
    "eapis",
    "e-apis",
    "ap is",
    "landing rights",
    "clear customs",
    "clearing customs",
    "customs information",
    "customs procedure",
)
CUSTOMS_CATEGORY_PRIORITY: tuple[str, ...] = (
    "canpass",
    "afterhours",
    "notice",
    "hours",
    "location",
    "contact",
    "pax",
    "crew",
    "general",
)
PPR_TRUE_PATTERNS = [
    r"\bppr\b",
    r"prior permission required",
    r"prior approval required",
    r"private airport\b",
]

SLOT_DAYS_OUT_RE = re.compile(r"(\d+)\s*days?\s*out")
SLOT_HOURS_RE = re.compile(r"(\d+)\s*(?:h|hrs|hours)\s*(?:before|prior)")
SLOT_VALIDITY_RE = re.compile(r"\+/-\s*(\d+)\s*min")
WITHIN_HOUR_RE = re.compile(r"within the hour")
PPR_DAYS_OUT_RE = re.compile(r"(\d+)\s*days?\s*(?:notice|prior)")
PPR_HOURS_OUT_RE = re.compile(r"(\d+)\s*(?:h|hrs|hours)\s*(?:notice|prior)")
CLOSED_BETWEEN_RE = re.compile(r"closed between\s*(\d{3,4})[-–](\d{3,4})")
RUNWAY_NUM_RE = re.compile(r"rwy\s*(\d{2}[lrc]?)", re.IGNORECASE)
ACFT_LIMIT_RE = re.compile(r"(embraer|cj2|cj3|legacy|challenger|global)", re.IGNORECASE)

HOURS_RE = re.compile(r"(\d{3,4})\s*[-–]\s*(\d{3,4})")
PRIOR_HOURS_RE = re.compile(r"(\d+)\s*(?:hours|hrs)\s*(?:notice|prior)")
PRIOR_DAYS_RE = re.compile(r"(\d+)\s*(?:days?)\s*(?:notice|prior)")
LOCATION_RE = re.compile(
    r"(?:location:|clear at|report to|proceed to|meet officer at|customs located at)\s*([A-Za-z0-9\-\s]+)",
    re.IGNORECASE,
)


def _combine_status(existing: CategoryStatus, candidate: CategoryStatus) -> CategoryStatus:
    return candidate if _STATUS_PRIORITY[candidate] > _STATUS_PRIORITY[existing] else existing


def note_text(note: Mapping[str, object]) -> str:
    """Extract the human-readable text body from an FL3XX airport note."""

    value = note.get("note")
    if isinstance(value, str) and value.strip():
        return value.strip()

    for key in ("title", "body", "category", "type"):
        v = note.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()

    return ""


def _empty_parsed_restrictions() -> ParsedRestrictions:
    return {
        "winter_sensitivity": False,
        "winter_notes": [],
        "deice_limited": False,
        "deice_unavailable": False,
        "deice_notes": [],
        "fuel_available": None,
        "fuel_notes": [],
        "ppr_required": False,
        "ppr_lead_days": None,
        "ppr_lead_hours": None,
        "ppr_notes": [],
        "slot_required": False,
        "slot_lead_days": None,
        "slot_lead_hours": None,
        "slot_validity_minutes": None,
        "slot_time_windows": [],
        "slot_notes": [],
        "hours_of_operation": [],
        "night_ops_allowed": None,
        "curfew": None,
        "hour_notes": [],
        "runway_limitations": [],
        "aircraft_type_limits": [],
        "surface_contamination": False,

        "weather_limitations": [],
        "generic_restrictions": [],
        "raw_notes": [],
    }


def _empty_parsed_customs() -> ParsedCustoms:
    return {
        "customs_available": False,
        "customs_hours": [],
        "customs_afterhours_available": False,
        "customs_afterhours_not_available": False,
        "customs_afterhours_requirements": [],
        "customs_prior_notice_hours": None,
        "customs_prior_notice_days": None,
        "customs_contact_required": False,
        "customs_contact_notes": [],
        "aoe_type": None,
        "aoe_notes": [],
        "canpass_only": False,
        "canpass_notes": [],
        "location_to_clear": None,
        "location_notes": [],
        "pax_requirements": [],
        "crew_requirements": [],
        "general_customs_notes": [],
        "raw_notes": [],
    }


def _contains_keyword(text: str, keywords: Sequence[str]) -> bool:
    lower = text.lower()
    return any(keyword in lower for keyword in keywords)


def _contains_winter_keyword(text: str) -> bool:
    if not text:
        return False
    return any(pattern.search(text) for pattern in WINTER_PATTERNS)


def _is_customs_contact_instruction(text: str) -> bool:
    """Detect whether a note explicitly directs contacting customs/CBSA.

    Some notes list parking radio calls or generic contact numbers near the word
    "customs" (e.g., describing a "Customs Shack" location). To avoid false
    positives, only treat a segment as a contact requirement when the contact
    verb and a customs-related term appear in the *same* sentence/segment.
    """

    lower = text.lower()

    contact_verbs = ("call", "phone", "contact", "notify")
    customs_terms = ("customs", "cbsa", "cbp", "officer", "border")

    def _has_contact(segment: str) -> bool:
        return any(verb in segment for verb in contact_verbs)

    def _has_customs(segment: str) -> bool:
        return any(term in segment for term in customs_terms)

    if not _has_contact(lower):
        return False

    segments = re.split(r"[\.\n;•]+", lower)
    return any(_has_contact(segment) and _has_customs(segment) for segment in segments)


def _classify_customs_note(text: str) -> set[str]:
    lower = text.lower()
    categories: set[str] = set()
    if re.search(HOURS_RE, lower):
        categories.add("hours")
    if "after hours" in lower or "afterhours" in lower:
        categories.add("afterhours")
    if re.search(PRIOR_HOURS_RE, lower) or re.search(PRIOR_DAYS_RE, lower):
        categories.add("notice")
    if re.search(LOCATION_RE, lower):
        categories.add("location")
    if any(k in lower for k in ("call", "phone", "contact", "notify")):
        categories.add("contact")
    if "pax" in lower or "passenger" in lower:
        categories.add("pax")
    if "crew" in lower:
        categories.add("crew")
    if "canpass" in lower:
        categories.add("canpass")
    if not categories:
        categories.add("general")
    return categories


def _select_primary_customs_category(categories: set[str]) -> str:
    for category in CUSTOMS_CATEGORY_PRIORITY:
        if category in categories:
            return category
    return "general"


def _has_weight_limitation(text: str) -> bool:
    lower = text.lower()
    if "weight" not in lower:
        return False
    return any(
        keyword in lower
        for keyword in (
            "limit",
            "restricted",
            "restriction",
            "max",
            "maximum",
            "mlw",
            "lbs",
            "ton",
        )
    )


def is_ppr_note(text: str) -> bool:
    lower = text.lower()
    return any(re.search(pattern, lower) for pattern in PPR_TRUE_PATTERNS)


def _classify_operational_note(note: str) -> set[str]:
    categories: set[str] = set()
    lower = note.lower()
    if _contains_keyword(lower, SLOT_KEYWORDS):
        categories.add("slot")
    if is_ppr_note(lower):
        categories.add("ppr")
    if _contains_winter_keyword(note):
        categories.add("winter")
    if _contains_keyword(lower, DEICE_KEYWORDS):
        categories.add("deice")
    if _contains_keyword(lower, FUEL_KEYWORDS):
        categories.add("fuel")
    if _contains_keyword(lower, NIGHT_KEYWORDS) or "day operations only" in lower or "day ops" in lower:
        categories.add("night")
    if _contains_keyword(lower, WEATHER_LIMITATION_KEYWORDS):
        categories.add("weather")
    if _contains_keyword(lower, RUNWAY_KEYWORDS):
        categories.add("runway")
    return categories


_CATEGORY_PRIORITY: tuple[str, ...] = (
    "slot",
    "ppr",
    "deice",
    "winter",
    "fuel",
    "night",
    "weather",
    "runway",
)


def _select_primary_category(categories: set[str]) -> str | None:
    for category in _CATEGORY_PRIORITY:
        if category in categories:
            return category
    return None


def split_customs_operational_notes(
    notes: Sequence[Mapping[str, object]]
) -> Tuple[list[str], list[str]]:
    customs: list[str] = []
    operational: list[str] = []
    for note in notes:
        text = note_text(note)
        if not text:
            continue
        lower = text.lower()
        if _should_ignore_operational_note(text, lower=lower):
            continue
        is_customs = any(keyword in lower for keyword in CUSTOMS_NOTE_KEYWORDS)
        if lower.startswith("crew notes"):
            continue
        if not is_customs and "crew" in lower:
            crew_customs_terms = (
                "customs",
                "cbp",
                "cbsa",
                "eapis",
                "e-apis",
                "ap is",
                "canpass",
                "aoe",
                "landing rights",
            )
            if any(term in lower for term in crew_customs_terms):
                is_customs = True
        if is_customs:
            customs.append(text)
        else:
            operational.append(text)
    return customs, operational


def _should_ignore_operational_note(text: str, *, lower: str | None = None) -> bool:
    lowered = lower or text.lower()
    if not lowered:
        return False
    if lowered.startswith("crew notes"):
        return True
    if lowered.startswith("contact instructions"):
        return True
    if "rental car" in lowered and "tesla" in lowered:
        return True
    if ("yyc" in lowered or "nvc" in lowered) and (
        lowered.startswith("operational instruction")
        or "contact person updated" in lowered
        or "engine run" in lowered
        or "repositioning back from cyyc" in lowered
    ):
        return True
    if "marketing@airsprint.com" in lowered:
        return True
    if "boeing field" in lowered and "seattle city centre" in lowered:
        return True
    return False


def parse_operational_restrictions(notes: Sequence[str]) -> ParsedRestrictions:
    parsed = _empty_parsed_restrictions()
    for raw in notes:
        text = raw.strip()
        if not text:
            continue
        lower = text.lower()
        if _should_ignore_operational_note(text, lower=lower):
            continue
        parsed["raw_notes"].append(text)
        categories = _classify_operational_note(text)
        if CLOSED_BETWEEN_RE.search(lower):
            categories.add("night")
        primary = _select_primary_category(categories)
        if "contaminated" in lower:
            parsed["surface_contamination"] = True
        if "slot" in categories:
            _extract_slot_details(
                text,
                parsed,
                # Slot note text is still captured, but _extract_slot_details trims
                # out unrelated caution verbiage before storing the note text.
                add_note=primary == "slot",
            )
        if "ppr" in categories:
            _extract_ppr_details(text, parsed, add_note=primary == "ppr")
        if "winter" in categories:
            _extract_winter_details(text, parsed, add_note=primary == "winter")
        if "deice" in categories:
            _extract_deice_details(text, parsed, add_note=primary == "deice")
        if "fuel" in categories:
            _extract_fuel_details(text, parsed, add_note=primary == "fuel")
        if "night" in categories:
            _extract_night_details(text, parsed, add_note=primary == "night")
        if "runway" in categories:
            _extract_runway_details(text, parsed, add_note=True)
            _extract_aircraft_limits(text, parsed, add_note=True)
        if "weather" in categories:
            _extract_weather_limitations(text, parsed)
        if not categories:
            _extract_generic(text, parsed)
    return parsed


def _extract_slot_details(
    note: str, out: ParsedRestrictions, *, add_note: bool = True
) -> None:
    lower = note.lower()
    out["slot_required"] = True
    if match := SLOT_DAYS_OUT_RE.search(lower):
        value = int(match.group(1))
        current = out["slot_lead_days"] or 0
        out["slot_lead_days"] = max(current, value) or value
    if match := SLOT_HOURS_RE.search(lower):
        value = int(match.group(1))
        current = out["slot_lead_hours"] or 0
        out["slot_lead_hours"] = max(current, value) or value
    if match := SLOT_VALIDITY_RE.search(lower):
        out["slot_validity_minutes"] = int(match.group(1))
    if WITHIN_HOUR_RE.search(lower):
        out["slot_validity_minutes"] = 60
    if add_note:
        lines = [line.strip() for line in note.splitlines() if line.strip()]
        slot_lines: list[str] = []
        for line in lines:
            if not _contains_keyword(line, SLOT_KEYWORDS):
                continue
            cleaned = re.split(r"\bcautions?\b", line, flags=re.IGNORECASE)[0].strip(" -:;\t")
            slot_lines.append(cleaned or line)
        if slot_lines:
            out["slot_notes"].append("\n".join(slot_lines))
        else:
            out["slot_notes"].append(note)


def _extract_ppr_details(
    note: str, out: ParsedRestrictions, *, add_note: bool = True
) -> None:
    lower = note.lower()
    out["ppr_required"] = True
    if match := PPR_DAYS_OUT_RE.search(lower):
        out["ppr_lead_days"] = int(match.group(1))
    if match := PPR_HOURS_OUT_RE.search(lower):
        out["ppr_lead_hours"] = int(match.group(1))
    if add_note:
        out["ppr_notes"].append(note)


def _extract_winter_details(
    note: str, out: ParsedRestrictions, *, add_note: bool = True
) -> None:
    out["winter_sensitivity"] = True
    if add_note:
        out["winter_notes"].append(note)


def _extract_deice_details(
    note: str, out: ParsedRestrictions, *, add_note: bool = True
) -> None:
    lower = note.lower()
    if "not available" in lower or "no deice" in lower or "no de-ice" in lower:
        out["deice_unavailable"] = True
    if _contains_limited_deice(lower):
        out["deice_limited"] = True
    if add_note:
        out["deice_notes"].append(note)


def _contains_limited_deice(text: str) -> bool:
    """Return True when the note explicitly calls out limited deice support."""

    phrases = (
        "limited deice",
        "limited de-ice",
        "limited deicing",
        "deice limited",
        "de-ice limited",
        "deicing limited",
    )
    if any(phrase in text for phrase in phrases):
        return True

    proximity_patterns = (
        r"limited[^\n]{0,30}de-?ic",
        r"de-?ic[^\n]{0,30}limited",
    )
    return any(re.search(pattern, text) for pattern in proximity_patterns)


def _extract_weather_limitations(note: str, out: ParsedRestrictions) -> None:
    out["weather_limitations"].append(note)


def _extract_fuel_details(
    note: str, out: ParsedRestrictions, *, add_note: bool = True
) -> None:
    lower = note.lower()
    if any(keyword in lower for keyword in FUEL_KEYWORDS):
        out["fuel_available"] = False
    if add_note:
        out["fuel_notes"].append(note)


def _extract_night_details(
    note: str, out: ParsedRestrictions, *, add_note: bool = True
) -> None:
    lower = note.lower()
    if out["night_ops_allowed"] is None:
        out["night_ops_allowed"] = True
    if (
        "day operations only" in lower
        or "no night ops" in lower
        or "night landings prohibited" in lower
    ):
        out["night_ops_allowed"] = False
    if match := CLOSED_BETWEEN_RE.search(lower):
        start, end = match.groups()
        out["hours_of_operation"].append({"closed_from": start, "closed_to": end})
    if "curfew" in lower:
        out["curfew"] = {"raw": note}
    if add_note:
        out["hour_notes"].append(note)


def _extract_runway_details(
    note: str, out: ParsedRestrictions, *, add_note: bool = True
) -> None:
    lower = note.lower()
    actionable = False
    relevant_lines: list[str] = []
    if _contains_keyword(lower, RUNWAY_RESTRICTION_TERMS):
        actionable = True
    if _has_weight_limitation(lower):
        actionable = True
    if _contains_keyword(lower, RUNWAY_CONTAMINATION_TERMS):
        out["surface_contamination"] = True
        actionable = True
    for line in note.splitlines():
        line_lower = line.lower()
        if any(keyword in line_lower for keyword in ("rwy", "runway", "caution", "hot spot", "hotspot", "hold short")):
            cleaned = line.strip()
            if cleaned:
                relevant_lines.append(cleaned)
    if relevant_lines:
        actionable = True
    if actionable and add_note:
        out["runway_limitations"].append("\n".join(relevant_lines) if relevant_lines else note)


def _extract_aircraft_limits(
    note: str, out: ParsedRestrictions, *, add_note: bool = True
) -> None:
    lower = note.lower()
    if ACFT_LIMIT_RE.search(lower) and ("only" in lower or "restricted" in lower):
        if add_note:
            out["aircraft_type_limits"].append(note)


def _extract_generic(note: str, out: ParsedRestrictions) -> None:
    if len(note.split()) > 3:
        out["generic_restrictions"].append(note)


def _is_non_restrictive_generic(note: str) -> bool:
    return any(pattern.search(note) for pattern in NON_RESTRICTIVE_GENERIC_PATTERNS)


DAY_KEYWORDS = (
    ("mon", "Mon"),
    ("tue", "Tue"),
    ("wed", "Wed"),
    ("thu", "Thu"),
    ("fri", "Fri"),
    ("sat", "Sat"),
    ("sun", "Sun"),
)


def _detect_days(lower: str) -> list[str]:
    days: list[str] = []
    for token, label in DAY_KEYWORDS:
        if token in lower:
            days.append(label)
    return days


def _is_plausible_time_range_value(value: str) -> bool:
    digits = re.sub(r"\D", "", value)
    if len(digits) == 3:
        digits = f"0{digits}"
    if len(digits) != 4:
        return False
    try:
        hours = int(digits[:2])
        minutes = int(digits[2:])
    except ValueError:
        return False
    if hours > 24 or minutes > 59:
        return False
    if hours == 24 and minutes != 0:
        return False
    return True


def parse_customs_notes(notes: Sequence[str]) -> ParsedCustoms:
    parsed = _empty_parsed_customs()
    for raw in notes:
        text = raw.strip()
        if not text:
            continue
        lower = text.lower()
        if lower.startswith("crew notes"):
            continue
        parsed["raw_notes"].append(text)
        categories = _classify_customs_note(text)
        primary = _select_primary_customs_category(categories)
        if "customs" in lower or "clearing customs" in lower or "aoe" in lower:
            parsed["customs_available"] = True
        if re.search(r"\baoe\s*/\s*canpass\b", lower):
            parsed["aoe_type"] = "AOE/CANPASS"
            parsed["aoe_notes"].append(text)
        elif re.search(r"\baoe\s*/\s*15\b", lower):
            parsed["aoe_type"] = "AOE/15"
            parsed["aoe_notes"].append(text)
        elif "aoe" in lower:
            parsed["aoe_type"] = parsed["aoe_type"] or "AOE"
            parsed["aoe_notes"].append(text)
        if "24/7" in lower or "24 hrs" in lower or "24hours" in lower or "24 hours" in lower:
            parsed["customs_hours"].append({"start": "0000", "end": "2400", "days": ["Daily"]})
        if "canpass" in lower:
            if "only" in lower or "canpass arrival" in lower or "arrival by canpass" in lower:
                parsed["canpass_only"] = True
            if primary == "canpass":
                parsed["canpass_notes"].append(text)
        if match := re.search(r"primary location:\s*([^\n\.]+)", text, re.IGNORECASE):
            location = match.group(1).strip()
            parsed["location_to_clear"] = location or parsed["location_to_clear"]
            parsed["location_notes"].append(text)
        elif match := re.search(r"secondary location:\s*([^\n\.]+)", text, re.IGNORECASE):
            location = match.group(1).strip()
            if location and not parsed["location_to_clear"]:
                parsed["location_to_clear"] = location
            parsed["location_notes"].append(text)
        for match in re.finditer(HOURS_RE, lower):
            # Skip phone-number style matches like 760-318-3880 where another
            # dash-and-digits segment immediately follows the match.
            trailing = lower[match.end() :]
            if re.match(r"\s*-\s*\d{2,4}", trailing):
                continue

            start, end = match.groups()
            if not (_is_plausible_time_range_value(start) and _is_plausible_time_range_value(end)):
                continue

            days = _detect_days(lower)
            if not days:
                days = ["unknown"]
            parsed["customs_hours"].append({"start": start, "end": end, "days": days})
        mentions_after_hours = "after hours" in lower or "afterhours" in lower
        mentions_no_after_hours = any(
            phrase in lower
            for phrase in (
                "no after hours",
                "no after-hours",
                "after hours not available",
                "afterhours not available",
                "after hours unavailable",
                "afterhours unavailable",
            )
        )
        if mentions_no_after_hours:
            parsed["customs_afterhours_not_available"] = True
        if mentions_after_hours and not mentions_no_after_hours:
            parsed["customs_afterhours_available"] = True
            if primary == "afterhours":
                parsed["customs_afterhours_requirements"].append(text)
        if match := re.search(PRIOR_HOURS_RE, lower):
            parsed["customs_prior_notice_hours"] = int(match.group(1))
        if match := re.search(PRIOR_DAYS_RE, lower):
            parsed["customs_prior_notice_days"] = int(match.group(1))
        if _is_customs_contact_instruction(text):
            parsed["customs_contact_required"] = True
            if primary == "contact":
                parsed["customs_contact_notes"].append(text)
        if match := re.search(LOCATION_RE, lower):
            parsed["location_to_clear"] = match.group(1).strip()
            if primary == "location":
                parsed["location_notes"].append(text)
        if "pax" in lower or "passenger" in lower:
            if primary == "pax":
                parsed["pax_requirements"].append(text)
        if "crew" in lower:
            if primary == "crew":
                parsed["crew_requirements"].append(text)
        if primary == "afterhours":
            pass
        elif primary == "contact":
            pass
        elif primary == "location":
            pass
        elif primary == "pax":
            pass
        elif primary == "crew":
            pass
        elif primary == "canpass":
            pass
        else:
            parsed["general_customs_notes"].append(text)
    return parsed


def summarize_operational_notes(
    icao: str,
    notes: Sequence[Mapping[str, object]],
    parsed_restrictions: ParsedRestrictions | None = None,
    parsed_customs: ParsedCustoms | None = None,
) -> CategoryResult:
    _, operational_texts = split_customs_operational_notes(notes) if notes else ([], [])
    if parsed_restrictions is None:
        parsed_restrictions = parse_operational_restrictions(operational_texts)

    has_raw_notes = bool(operational_texts)
    if parsed_restrictions and parsed_restrictions["raw_notes"]:
        has_raw_notes = True
    if not has_raw_notes:
        return CategoryResult(status="PASS", summary="No operational notes", issues=[])
    status: CategoryStatus = "PASS"
    issues: list[str] = []

    def add_issue(message: str, *, severity: CategoryStatus = "INFO") -> None:
        nonlocal status

        issues.append(message)
        status = _combine_status(status, severity)

    restrictions = parsed_restrictions or _empty_parsed_restrictions()
    raw_notes = [note.lower() for note in restrictions.get("raw_notes", [])]

    for entry in restrictions["hours_of_operation"]:
        start = entry.get("closed_from") or entry.get("start")
        end = entry.get("closed_to") or entry.get("end")
        if start or end:
            window = f"{start}-{end}" if start and end else start or end
            add_issue(
                f"Hours of Operation - Airport closed between {window}",
                severity="CAUTION",
            )

    if restrictions["curfew"]:
        curfew = restrictions["curfew"]
        window: str | None = None
        if isinstance(curfew, Mapping):
            start = curfew.get("from") or curfew.get("start") or curfew.get("closed_from")
            end = curfew.get("to") or curfew.get("end") or curfew.get("closed_to")
            if start or end:
                window = f"{start}-{end}" if start and end else start or end
        detail = f"Curfew in effect{f' ({window})' if window else ''}"
        add_issue(detail, severity="CAUTION")

    for weather_note in restrictions["weather_limitations"]:
        lower = weather_note.lower()
        if "good weather" in lower or "vfr" in lower:
            add_issue("Good Weather (VFR) only per operational notes", severity="CAUTION")

    if any("wet runway" in note for note in raw_notes):
        add_issue("Wet runway may limit operations", severity="INFO")

    if any("duty pilot approval required" in note for note in raw_notes):
        add_issue("Duty Pilot approval required", severity="INFO")

    if any("limited winter maintenance" in note for note in raw_notes):
        add_issue("Limited winter maintenance", severity="INFO")

    if restrictions["surface_contamination"] or any("runway contamination" in note for note in raw_notes):
        add_issue("Runway contamination (snow/ice) may limit operations", severity="INFO")

    if any("pic" in note and "duty pilot" in note for note in raw_notes):
        add_issue("PIC to contact Duty Pilot prior to operation", severity="INFO")

    tfr_keywords = ("tfr", "temporary flight restriction")
    if any(keyword in note and "no tfr" not in note for keyword in tfr_keywords for note in raw_notes):
        add_issue("Active Temporary Flight Restriction (TFR) noted", severity="CAUTION")

    summary = "Operational notes present (no tracked flags)" if has_raw_notes else "No operational notes"
    if issues:
        summary = "Operational notes require review"

    return CategoryResult(status=status, summary=summary, issues=issues)

