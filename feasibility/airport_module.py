"""Dedicated airport feasibility module used by the Feasibility Engine."""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, TypedDict

import pytz

from deice_info_helper import DeiceRecord, get_deice_record
from flight_leg_utils import load_airport_metadata_lookup, safe_parse_dt

from .airport_notes_parser import (
    ParsedCustoms,
    ParsedRestrictions,
    note_text,
    parse_customs_notes,
    parse_operational_restrictions,
    split_customs_operational_notes,
    summarize_operational_notes,
)
from .common import extract_airport_code
from .data_access import AirportCategoryRecord, CustomsRule, load_airport_categories, load_customs_rules
from .schemas import CategoryResult, CategoryStatus

AirportMetadataLookup = Mapping[str, Mapping[str, Optional[str]]]


class LegContext(TypedDict, total=False):
    leg_id: str
    departure_icao: str
    arrival_icao: str
    departure_date_utc: str
    arrival_date_utc: str
    pax: int
    block_time_minutes: int
    flight_time_minutes: int
    distance_nm: float
    workflow: str
    workflow_custom_name: str
    notes: str
    planning_notes: str
    warnings: Sequence[Mapping[str, Any]]
    aircraft_type: str
    aircraft_category: str
    route_countries: Sequence[str]
    fir_codes: Sequence[str]
    departure_country: str
    arrival_country: str
    is_international: bool


class OperationalNote(TypedDict, total=False):
    note: Optional[str]
    category: Optional[str]
    type: Optional[str]
    title: Optional[str]
    body: Optional[str]
    valid_from: Optional[str]
    valid_to: Optional[str]


@dataclass(frozen=True)
class AirportProfile:
    icao: str
    name: Optional[str]
    longest_runway_ft: Optional[int]
    is_approved_for_ops: bool
    category: Optional[str]
    elevation_ft: Optional[int]
    country: Optional[str]


@dataclass(frozen=True)
class DeiceProfile:
    icao: str
    deice_available: Optional[bool]
    notes: Optional[str]


@dataclass(frozen=True)
class CustomsProfile:
    icao: str
    service_type: Optional[str]
    notes: Optional[str]


@dataclass(frozen=True)
class OsaSsaProfile:
    icao: str
    region: str
    requires_jepp: bool


@dataclass(frozen=True)
class SlotPprProfile:
    icao: str
    slot_required: bool
    ppr_required: bool
    slot_lead_days: Optional[int]
    ppr_lead_days: Optional[int]
    notes: Optional[str]


@dataclass(frozen=True)
class OverflightRules:
    permit_lead_days: Mapping[str, int]


OSA_SSA_PROFILE_OVERRIDES: Mapping[str, Dict[str, object]] = {
    "CYEG": {"region": "CANADA_DOMESTIC", "requires_jepp": False},
    "KPSP": {"region": "SSA", "requires_jepp": False},
}


@dataclass
class AirportSideResult:
    icao: str
    suitability: CategoryResult
    deice: CategoryResult
    customs: CategoryResult
    slot_ppr: CategoryResult
    osa_ssa: CategoryResult
    overflight: CategoryResult
    operational_notes: CategoryResult
    parsed_operational_restrictions: ParsedRestrictions
    parsed_customs_notes: ParsedCustoms
    raw_operational_notes: List[str] = field(default_factory=list)

    def iter_category_results(self) -> Sequence[Tuple[str, CategoryResult]]:
        return (
            ("Suitability", self.suitability),
            ("Deice", self.deice),
            ("Customs", self.customs),
            ("Slot / PPR", self.slot_ppr),
            ("OSA / SSA", self.osa_ssa),
            ("Overflight", self.overflight),
            ("Operational Notes", self.operational_notes),
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "icao": self.icao,
            "suitability": self.suitability.as_dict(),
            "deice": self.deice.as_dict(),
            "customs": self.customs.as_dict(),
            "slot_ppr": self.slot_ppr.as_dict(),
            "osa_ssa": self.osa_ssa.as_dict(),
            "overflight": self.overflight.as_dict(),
            "operational_notes": self.operational_notes.as_dict(),
            "parsed_operational_restrictions": dict(self.parsed_operational_restrictions),
            "parsed_customs_notes": dict(self.parsed_customs_notes),
            "raw_operational_notes": list(self.raw_operational_notes),
        }


@dataclass
class AirportFeasibilityResult:
    leg_id: str
    departure: AirportSideResult
    arrival: AirportSideResult

    def iter_all_categories(self) -> Sequence[Tuple[str, CategoryResult]]:
        entries: List[Tuple[str, CategoryResult]] = []
        for prefix, side in (("Departure", self.departure), ("Arrival", self.arrival)):
            for label, result in side.iter_category_results():
                entries.append((f"{prefix} {label}", result))
        return entries

    def as_dict(self) -> Dict[str, Any]:
        return {
            "leg_id": self.leg_id,
            "departure": self.departure.as_dict(),
            "arrival": self.arrival.as_dict(),
        }


RUNWAY_REQUIREMENTS_FT: Mapping[str, int] = {
    "VERY_LIGHT_JET": 3500,
    "LIGHT_JET": 4000,
    "MIDSIZE_JET": 4500,
    "SUPER_MIDSIZE_JET": 5000,
    "HEAVY_JET": 5500,
    "ULTRA_LONG_RANGE_JET": 6000,
}

SLOT_KEYWORDS = ("SLOT", "COORDINATION", "ATC SLOT")
PPR_KEYWORDS = ("PPR", "PRIOR PERMISSION")

_RUNWAYS_PATH = Path(__file__).resolve().parents[1] / "runways.csv"
_STATUS_PRIORITY: Mapping[CategoryStatus, int] = {"PASS": 0, "CAUTION": 1, "FAIL": 2}


def _default_operational_notes_fetcher(icao: str, date_local: Optional[str]) -> Sequence[Mapping[str, Any]]:
    return []


@lru_cache(maxsize=1)
def _load_longest_runways() -> Mapping[str, int]:
    lengths: Dict[str, int] = {}
    if not _RUNWAYS_PATH.exists():
        return lengths
    try:
        with _RUNWAYS_PATH.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                airport_ident = (row.get("airport_ident") or "").strip().upper()
                if not airport_ident:
                    continue
                length_raw = row.get("length_ft")
                try:
                    length_ft = int(float(length_raw)) if length_raw else None
                except ValueError:
                    length_ft = None
                if not length_ft:
                    continue
                previous = lengths.get(airport_ident)
                if previous is None or length_ft > previous:
                    lengths[airport_ident] = length_ft
    except Exception:
        return {}
    return lengths


def _normalize_icao(value: Optional[str]) -> Optional[str]:
    if not isinstance(value, str):
        return None
    text = value.strip().upper()
    return text or None


def _get_timezone_provider(lookup: AirportMetadataLookup) -> Callable[[str], Optional[str]]:
    def provider(icao: str) -> Optional[str]:
        record = lookup.get(icao.upper())
        if isinstance(record, Mapping):
            tz = record.get("tz")
            if isinstance(tz, str) and tz.strip():
                return tz.strip()
        return None

    return provider


def _local_date_string(dt_str: Optional[str], tz_name: Optional[str]) -> Optional[str]:
    if not dt_str:
        return None
    try:
        dt = safe_parse_dt(dt_str)
    except Exception:
        return None
    if tz_name:
        try:
            tz = pytz.timezone(tz_name)
            dt = dt.astimezone(tz)
        except Exception:
            pass
    else:
        dt = dt.astimezone(pytz.UTC)
    return dt.date().isoformat()


def _normalize_datetime(dt_str: Optional[str]) -> Optional[str]:
    if not dt_str:
        return None
    try:
        dt = safe_parse_dt(dt_str)
    except Exception:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_parse_optional(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return safe_parse_dt(dt_str)
    except Exception:
        return None


def build_leg_context_from_flight(
    flight: Mapping[str, Any],
    *,
    airport_metadata: Optional[AirportMetadataLookup] = None,
) -> Optional[LegContext]:
    dep = _normalize_icao(extract_airport_code(flight, arrival=False))
    arr = _normalize_icao(extract_airport_code(flight, arrival=True))
    if not dep or not arr:
        return None

    metadata = airport_metadata or load_airport_metadata_lookup()
    dep_meta = metadata.get(dep, {}) if metadata else {}
    arr_meta = metadata.get(arr, {}) if metadata else {}

    def _coerce_int(value: Any) -> Optional[int]:
        try:
            if value is None:
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    def _coerce_float(value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    def _normalize_sequence(value: Any) -> Tuple[str, ...]:
        if isinstance(value, (list, tuple, set)):
            raw_values = value
        elif isinstance(value, str):
            raw_values = [part.strip() for part in value.split(",")]
        else:
            return ()
        normalized: List[str] = []
        for item in raw_values:
            if not isinstance(item, str):
                continue
            cleaned = item.strip()
            if not cleaned:
                continue
            normalized.append(cleaned.upper())
        return tuple(normalized)

    leg: LegContext = {
        "leg_id": str(flight.get("flightId") or flight.get("id") or "").strip() or "",
        "departure_icao": dep,
        "arrival_icao": arr,
        "departure_date_utc": _normalize_datetime(
            flight.get("dep_time") or flight.get("departureTime") or flight.get("departureDateUTC")
        )
        or "",
        "arrival_date_utc": _normalize_datetime(
            flight.get("arrivalTime") or flight.get("arrivalDateUTC") or flight.get("arrivalDate")
        )
        or "",
        "pax": _coerce_int(flight.get("pax")) or 0,
        "block_time_minutes": _coerce_int(flight.get("blockTime") or flight.get("plannedBlockTime")) or 0,
        "flight_time_minutes": _coerce_int(flight.get("flightTime")) or 0,
        "distance_nm": _coerce_float(flight.get("distanceNm") or flight.get("distance")) or 0.0,
        "workflow": str(flight.get("workflow") or "").strip(),
        "workflow_custom_name": str(flight.get("workflowCustomName") or "").strip(),
        "notes": str(flight.get("notes") or flight.get("legNotes") or "").strip(),
        "planning_notes": str(flight.get("planningNotes") or "").strip(),
        "warnings": tuple(flight.get("warnings") or ()),
        "aircraft_type": str(flight.get("aircraftType") or flight.get("aircraft") or "").strip(),
        "aircraft_category": str(flight.get("aircraftCategory") or "").strip().upper(),
        "route_countries": _normalize_sequence(flight.get("routeCountries")),
        "fir_codes": _normalize_sequence(flight.get("firCodes")),
        "departure_country": str(dep_meta.get("country") or "").strip(),
        "arrival_country": str(arr_meta.get("country") or "").strip(),
    }

    dep_country = leg.get("departure_country")
    arr_country = leg.get("arrival_country")
    leg["is_international"] = bool(dep_country and arr_country and dep_country != arr_country)
    return leg


def _build_airport_profile(
    icao: str,
    *,
    airport_categories: Mapping[str, AirportCategoryRecord],
    metadata: AirportMetadataLookup,
) -> AirportProfile:
    longest_runways = _load_longest_runways()
    category_record = airport_categories.get(icao)
    metadata_record = metadata.get(icao, {}) if metadata else {}
    return AirportProfile(
        icao=icao,
        name=None,
        longest_runway_ft=longest_runways.get(icao),
        is_approved_for_ops=True,
        category=category_record.category if category_record else None,
        elevation_ft=None,
        country=str(metadata_record.get("country") or "").strip() or None,
    )


def _build_deice_profile(icao: str) -> DeiceProfile:
    record: Optional[DeiceRecord] = get_deice_record(icao=icao)
    if not record:
        return DeiceProfile(icao=icao, deice_available=None, notes=None)
    return DeiceProfile(icao=icao, deice_available=record.has_deice, notes=record.deice_info)


def _build_customs_profile(icao: str, customs_rules: Mapping[str, CustomsRule]) -> Optional[CustomsProfile]:
    record = customs_rules.get(icao)
    if not record:
        return None
    return CustomsProfile(icao=icao, service_type=record.service_type, notes=record.notes)


def _build_osa_ssa_profile(icao: str, airport_categories: Mapping[str, AirportCategoryRecord]) -> OsaSsaProfile:
    category_record = airport_categories.get(icao)
    override = OSA_SSA_PROFILE_OVERRIDES.get(icao.upper())
    region = (category_record.category if category_record else "DOMESTIC") or "DOMESTIC"
    if override and isinstance(override.get("region"), str):
        region = str(override["region"]).strip() or region
    region_upper = region.upper()
    requires_override = None
    if override is not None:
        for key in ("requires_jepp", "requires_jeppesen"):
            value = override.get(key)
            if isinstance(value, bool):
                requires_override = value
                break
    requires_jepp = bool(requires_override) if requires_override is not None else False
    return OsaSsaProfile(icao=icao, region=region_upper, requires_jepp=requires_jepp)


def _extract_lead_days(notes: Optional[str]) -> Optional[int]:
    if not notes:
        return None
    match = re.search(r"(\d{1,2})\s*(?:DAY|DAYS|HR|HOURS)", notes, re.IGNORECASE)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _build_slot_ppr_profile(icao: str, airport_categories: Mapping[str, AirportCategoryRecord]) -> SlotPprProfile:
    record = airport_categories.get(icao)
    notes = record.notes if record else None
    notes_upper = notes.upper() if notes else ""
    slot_required = any(keyword in notes_upper for keyword in SLOT_KEYWORDS)
    ppr_required = any(keyword in notes_upper for keyword in PPR_KEYWORDS)
    # Airports classified as OSA/SSA often require coordination
    if record and record.category in {"OSA", "SSA"}:
        slot_required = True
        ppr_required = True
    lead_days = _extract_lead_days(notes)
    return SlotPprProfile(
        icao=icao,
        slot_required=slot_required,
        ppr_required=ppr_required,
        slot_lead_days=lead_days,
        ppr_lead_days=lead_days,
        notes=notes,
    )


def get_overflight_rules() -> OverflightRules:
    return OverflightRules(
        permit_lead_days={
            "CUBA": 3,
            "MEXICO": 1,
            "RUSSIA": 7,
            "CHINA": 7,
            "GREENLAND": 3,
        }
    )


def evaluate_airport_feasibility_for_leg(
    leg: LegContext,
    *,
    tz_provider: Optional[Callable[[str], Optional[str]]] = None,
    operational_notes_fetcher: Optional[Callable[[str, Optional[str]], Sequence[Mapping[str, Any]]]] = None,
    airport_metadata: Optional[AirportMetadataLookup] = None,
    airport_categories: Optional[Mapping[str, AirportCategoryRecord]] = None,
    customs_rules: Optional[Mapping[str, CustomsRule]] = None,
    now: Optional[datetime] = None,
) -> AirportFeasibilityResult:
    airport_categories = airport_categories or load_airport_categories()
    customs_rules = customs_rules or load_customs_rules()
    airport_metadata = airport_metadata or load_airport_metadata_lookup()
    tz_provider = tz_provider or _get_timezone_provider(airport_metadata)
    fetcher = operational_notes_fetcher or _default_operational_notes_fetcher
    reference_time = now or datetime.now(timezone.utc)

    dep_icao = leg["departure_icao"]
    arr_icao = leg["arrival_icao"]

    dep_profile = _build_airport_profile(dep_icao, airport_categories=airport_categories, metadata=airport_metadata)
    arr_profile = _build_airport_profile(arr_icao, airport_categories=airport_categories, metadata=airport_metadata)

    dep_deice = _build_deice_profile(dep_icao)
    arr_deice = _build_deice_profile(arr_icao)

    dep_customs = _build_customs_profile(dep_icao, customs_rules)
    arr_customs = _build_customs_profile(arr_icao, customs_rules)

    dep_osa = _build_osa_ssa_profile(dep_icao, airport_categories)
    arr_osa = _build_osa_ssa_profile(arr_icao, airport_categories)

    dep_slot = _build_slot_ppr_profile(dep_icao, airport_categories)
    arr_slot = _build_slot_ppr_profile(arr_icao, airport_categories)

    dep_tz = tz_provider(dep_icao) if tz_provider else None
    arr_tz = tz_provider(arr_icao) if tz_provider else None
    dep_date_local = _local_date_string(leg.get("departure_date_utc"), dep_tz)
    arr_date_local = _local_date_string(leg.get("arrival_date_utc"), arr_tz)

    dep_notes = list(fetcher(dep_icao, dep_date_local))
    arr_notes = list(fetcher(arr_icao, arr_date_local))

    overflight_rules = get_overflight_rules()
    overflight_result = evaluate_overflight(overflight_rules, leg, now=reference_time)

    departure_side = evaluate_airport_side(
        icao=dep_icao,
        date_local=dep_date_local,
        leg=leg,
        airport_profile=dep_profile,
        deice_profile=dep_deice,
        customs_profile=dep_customs,
        osa_ssa_profile=dep_osa,
        slot_ppr_profile=dep_slot,
        operational_notes=dep_notes,
        overflight_result=overflight_result,
        side="DEP",
    )
    arrival_side = evaluate_airport_side(
        icao=arr_icao,
        date_local=arr_date_local,
        leg=leg,
        airport_profile=arr_profile,
        deice_profile=arr_deice,
        customs_profile=arr_customs,
        osa_ssa_profile=arr_osa,
        slot_ppr_profile=arr_slot,
        operational_notes=arr_notes,
        overflight_result=overflight_result,
        side="ARR",
    )

    return AirportFeasibilityResult(leg_id=leg.get("leg_id", ""), departure=departure_side, arrival=arrival_side)


def evaluate_airport_side(
    *,
    icao: str,
    date_local: Optional[str],
    leg: LegContext,
    airport_profile: AirportProfile,
    deice_profile: DeiceProfile,
    customs_profile: Optional[CustomsProfile],
    osa_ssa_profile: OsaSsaProfile,
    slot_ppr_profile: SlotPprProfile,
    operational_notes: Sequence[Mapping[str, Any]],
    overflight_result: CategoryResult,
    side: str,
) -> AirportSideResult:
    customs_texts, operational_texts = split_customs_operational_notes(operational_notes)
    parsed_customs_notes = parse_customs_notes(customs_texts)
    parsed_operational_restrictions = parse_operational_restrictions(operational_texts)
    raw_note_texts: List[str] = []
    for entry in operational_notes:
        text = note_text(entry)
        if text:
            raw_note_texts.append(text)

    suitability = evaluate_suitability(airport_profile, leg, operational_notes, side)
    deice = evaluate_deice(
        deice_profile,
        operational_notes,
        leg,
        side,
        parsed_restrictions=parsed_operational_restrictions,
    )
    customs = evaluate_customs(
        customs_profile,
        leg,
        side,
        operational_notes,
        parsed_customs=parsed_customs_notes,
    )
    slot_ppr = evaluate_slot_ppr(slot_ppr_profile, leg, side, date_local)
    osa_ssa = evaluate_osa_ssa(osa_ssa_profile, leg, side)
    operational_notes_result = summarize_operational_notes(
        icao,
        operational_notes,
        parsed_operational_restrictions,
        parsed_customs_notes,
    )

    return AirportSideResult(
        icao=icao,
        suitability=suitability,
        deice=deice,
        customs=customs,
        slot_ppr=slot_ppr,
        osa_ssa=osa_ssa,
        overflight=overflight_result,
        operational_notes=operational_notes_result,
        parsed_operational_restrictions=parsed_operational_restrictions,
        parsed_customs_notes=parsed_customs_notes,
        raw_operational_notes=raw_note_texts,
    )


def evaluate_suitability(
    airport_profile: AirportProfile,
    leg: LegContext,
    operational_notes: Sequence[Mapping[str, Any]],
    side: str,
) -> CategoryResult:
    issues: List[str] = []
    status: CategoryStatus = "PASS"
    summary = "Airport approved"

    if not airport_profile.is_approved_for_ops:
        status = "FAIL"
        summary = "Airport not approved"
        issues.append("Airport marked as not approved for AirSprint operations.")

    required_length = RUNWAY_REQUIREMENTS_FT.get(leg.get("aircraft_category", ""), 4300)
    longest = airport_profile.longest_runway_ft
    if longest is None:
        issues.append("No runway data available; verify manually.")
        status = _combine_status(status, "CAUTION")
        summary = "Missing runway intel"
    elif longest < required_length:
        status = "FAIL"
        summary = "Insufficient runway length"
        issues.append(
            f"Longest runway {longest:,} ft < required {required_length:,} ft for {leg.get('aircraft_category') or 'aircraft'}."
        )
    elif longest - required_length < 500:
        status = _combine_status(status, "CAUTION")
        summary = "Runway margin tight"
        issues.append(
            f"Runway margin only {longest - required_length:,} ft; monitor performance numbers."
        )

    closure_keywords = ("closed", "closure", "no ga", "curfew")
    for note in operational_notes:
        body = " ".join(str(note.get(key) or "") for key in ("note", "title", "body")).lower()
        if any(keyword in body for keyword in closure_keywords):
            status = "FAIL"
            summary = "Operational closure in effect"
            issues.append("Operational notes indicate closures or curfews impacting this leg.")
            break

    return CategoryResult(status=status, summary=summary, issues=issues)


def evaluate_deice(
    deice_profile: DeiceProfile,
    operational_notes: Sequence[Mapping[str, Any]],
    leg: LegContext,
    side: str,
    *,
    parsed_restrictions: ParsedRestrictions | None = None,
) -> CategoryResult:
    issues: List[str] = []
    status: CategoryStatus = "PASS"
    summary = "Deice available"

    if deice_profile.deice_available is False:
        status = "CAUTION"
        summary = "Deice unavailable"
        issues.append("Deice program indicates no deice at this airport.")
    elif deice_profile.deice_available is None:
        summary = "Unknown deice status"
        issues.append("No deice intel available; confirm if icing conditions likely.")

    restrictions = parsed_restrictions
    if restrictions:
        if restrictions["deice_unavailable"]:
            status = _combine_status(status, "CAUTION")
            summary = "Operational note: deice unavailable"
        elif restrictions["deice_limited"]:
            status = _combine_status(status, "CAUTION")
            summary = "Operational note: deice limited"
        if restrictions["winter_sensitivity"]:
            issues.append("Operational notes highlight winter sensitivity.")
        for note in restrictions["deice_notes"]:
            issues.append(f"Deice note: {note}")
    else:
        note_text = " ".join(
            str(note.get("note") or note.get("body") or note.get("title") or "")
            for note in operational_notes
        ).lower()
        if "deice" in note_text and "out" in note_text:
            status = _combine_status(status, "CAUTION")
            summary = "Operational note: deice impacted"
            issues.append("Operational notes reference deice outages; confirm support.")

    if deice_profile.notes:
        issues.append(deice_profile.notes)

    return CategoryResult(status=status, summary=summary, issues=issues)


def _format_customs_hours_entry(entry: Mapping[str, Any]) -> Optional[str]:
    start = str(entry.get("start") or "").strip()
    end = str(entry.get("end") or "").strip()
    days_value = entry.get("days")
    day_parts: List[str] = []
    if isinstance(days_value, (list, tuple, set)):
        for value in days_value:
            if isinstance(value, str):
                cleaned = value.strip()
                if cleaned and cleaned.lower() != "unknown":
                    day_parts.append(cleaned)
    hours = f"{start}-{end}" if start and end else start or end
    if day_parts and hours:
        return f"{'/'.join(day_parts)} {hours}"
    if hours:
        return hours
    if day_parts:
        return "/".join(day_parts)
    return None


def _build_customs_summary(base_summary: str, parsed: Optional[ParsedCustoms]) -> str:
    if not parsed:
        return base_summary

    details: List[str] = []
    if parsed["customs_hours"]:
        hours_entry = parsed["customs_hours"][0]
        formatted = _format_customs_hours_entry(hours_entry)
        if formatted:
            details.append(formatted)

    notice_requirements: List[str] = []
    if parsed["customs_prior_notice_hours"]:
        notice_requirements.append(f"{parsed['customs_prior_notice_hours']}h notice")
    if parsed["customs_prior_notice_days"]:
        notice_requirements.append(f"{parsed['customs_prior_notice_days']} day notice")

    notice_text = " and ".join(notice_requirements)
    if parsed["customs_afterhours_available"]:
        detail = "After-hours possible"
        if notice_text:
            detail += f" with {notice_text}"
        details.append(detail)
    elif notice_text:
        details.append(f"Requires {notice_text}")

    if not details:
        return base_summary

    detail_text = "; ".join(details)
    if base_summary.lower().startswith("customs available"):
        return f"{base_summary} {detail_text}".strip()
    return f"{base_summary} — {detail_text}"


def evaluate_customs(
    customs_profile: Optional[CustomsProfile],
    leg: LegContext,
    side: str,
    operational_notes: Sequence[Mapping[str, Any]],
    *,
    parsed_customs: ParsedCustoms | None = None,
) -> CategoryResult:
    issues: List[str] = []
    status: CategoryStatus = "PASS"
    summary = "Customs available"

    if side == "DEP":
        return CategoryResult(status=status, summary="Not required for departure", issues=issues)

    if not leg.get("is_international"):
        summary = "Domestic leg"
        return CategoryResult(status=status, summary=summary, issues=issues)

    if customs_profile is None:
        status = "CAUTION"
        summary = "No customs intel"
        issues.append("No customs record available; confirm with airport directly.")
    else:
        service_type = customs_profile.service_type or "UNKNOWN"
        issues.append(f"Customs service type: {service_type}.")
        if service_type.upper() == "NONE":
            status = "FAIL"
            summary = "No customs service"
            issues.append("International arrival requires customs; arrange alternate field.")
        elif "CANPASS" in service_type.upper() and side == "ARR":
            status = _combine_status(status, "CAUTION")
            summary = "CANPASS arrival"
            issues.append("Ensure CANPASS paperwork and notice are completed.")
        if customs_profile.notes:
            issues.append(customs_profile.notes)

    parsed = parsed_customs
    if parsed:
        if parsed["canpass_only"]:
            status = _combine_status(status, "CAUTION")
            summary = "CANPASS arrival"
            issues.append("Operational notes: CANPASS-only clearance.")
        if parsed["customs_prior_notice_hours"]:
            status = _combine_status(status, "CAUTION")
            issues.append(
                f"Customs requires {parsed['customs_prior_notice_hours']} hours prior notice."
            )
        if parsed["customs_prior_notice_days"]:
            status = _combine_status(status, "CAUTION")
            issues.append(
                f"Customs requires {parsed['customs_prior_notice_days']} day notice."
            )
        if parsed["customs_contact_required"]:
            status = _combine_status(status, "CAUTION")
            issues.append("Customs contact required per notes.")
            issues.extend(parsed["customs_contact_notes"])
        if parsed["customs_afterhours_available"]:
            issues.append("Afterhours customs available; verify call-out requirements.")
            issues.extend(parsed["customs_afterhours_requirements"])
        if parsed["location_to_clear"]:
            issues.append(f"Clear customs at {parsed['location_to_clear']}.")
            issues.extend(parsed["location_notes"])
        if parsed["pax_requirements"]:
            status = _combine_status(status, "CAUTION")
            issues.extend(parsed["pax_requirements"])
        if parsed["crew_requirements"]:
            status = _combine_status(status, "CAUTION")
            issues.extend(parsed["crew_requirements"])
        summary = _build_customs_summary(summary, parsed)
    else:
        note_text = " ".join(
            str(note.get("note") or note.get("body") or note.get("title") or "")
            for note in operational_notes
        ).lower()
        if "customs" in note_text and ("closed" in note_text or "limited" in note_text):
            status = "FAIL"
            summary = "Customs restricted"
            issues.append("Operational notes report customs unavailability.")

    return CategoryResult(status=status, summary=summary, issues=issues)


def evaluate_slot_ppr(
    slot_ppr_profile: SlotPprProfile,
    leg: LegContext,
    side: str,
    date_local: Optional[str],
) -> CategoryResult:
    issues: List[str] = []
    status: CategoryStatus = "PASS"
    summary = "No slot/PPR requirement"

    now = datetime.now(timezone.utc)
    target_dt = _safe_parse_optional(leg.get("departure_date_utc")) if side == "DEP" else _safe_parse_optional(leg.get("arrival_date_utc"))
    hours_until_event: Optional[float] = None
    if target_dt:
        hours_until_event = (target_dt - now).total_seconds() / 3600

    def _check_requirement(required: bool, label: str, lead_days: Optional[int]) -> None:
        nonlocal status, summary
        if not required:
            return
        required_summary = f"{label} required"
        summary = required_summary
        status = _combine_status(status, "CAUTION")
        issues.append(f"{label} required for {slot_ppr_profile.icao}.")
        if lead_days is not None and hours_until_event is not None:
            lead_hours = lead_days * 24
            if hours_until_event < lead_hours:
                status = _combine_status(status, "FAIL")
                issues.append(f"Inside {lead_days}-day {label.lower()} window; action immediately.")

    _check_requirement(slot_ppr_profile.slot_required, "Slot", slot_ppr_profile.slot_lead_days)
    _check_requirement(slot_ppr_profile.ppr_required, "PPR", slot_ppr_profile.ppr_lead_days)

    if slot_ppr_profile.notes:
        issues.append(slot_ppr_profile.notes)

    return CategoryResult(status=status, summary=summary, issues=issues)


def evaluate_osa_ssa(
    osa_ssa_profile: OsaSsaProfile,
    leg: LegContext,
    side: str,
) -> CategoryResult:
    issues: List[str] = []
    status: CategoryStatus = "PASS"
    summary = f"Routing classified as {osa_ssa_profile.region}"

    if osa_ssa_profile.requires_jepp:
        status = "CAUTION"
        summary = f"{osa_ssa_profile.region} — Jeppesen required"
        issues.append("Jeppesen ITP task required for this leg.")
    else:
        summary = f"Routing classified as {osa_ssa_profile.region}. Jeppesen not required by profile."

    return CategoryResult(status=status, summary=summary, issues=issues)


def evaluate_overflight(
    overflight_rules: OverflightRules,
    leg: LegContext,
    *,
    now: Optional[datetime] = None,
) -> CategoryResult:
    issues: List[str] = []
    status: CategoryStatus = "PASS"
    summary = "No overflight permits detected"

    now = now or datetime.now(timezone.utc)
    departure_dt = _safe_parse_optional(leg.get("departure_date_utc"))

    route_countries = [country for country in leg.get("route_countries", []) if isinstance(country, str)]
    if not route_countries:
        for key in ("departure_country", "arrival_country"):
            country = leg.get(key)
            if isinstance(country, str) and country:
                route_countries.append(country.upper())

    flagged: List[str] = []
    for country in route_countries:
        country_upper = country.upper()
        if country_upper in overflight_rules.permit_lead_days:
            flagged.append(country_upper)

    if not flagged:
        return CategoryResult(status=status, summary=summary, issues=issues)

    summary = "Overflight permits required"
    status = "CAUTION"
    for country in flagged:
        lead_days = overflight_rules.permit_lead_days.get(country, 0)
        issues.append(f"{country} permit lead: {lead_days} day(s).")
        if departure_dt is not None:
            hours_until = (departure_dt - now).total_seconds() / 3600
            if hours_until < lead_days * 24:
                status = _combine_status(status, "FAIL")
                issues.append(f"Departing inside {lead_days}-day lead for {country} permit.")

    return CategoryResult(status=status, summary=summary, issues=issues)


def _combine_status(current: CategoryStatus, candidate: CategoryStatus) -> CategoryStatus:
    return candidate if _STATUS_PRIORITY[candidate] > _STATUS_PRIORITY[current] else current

