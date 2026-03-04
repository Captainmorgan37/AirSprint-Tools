"""Utilities for parsing owner-facing services from FL3XX payloads."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, List, Mapping, Optional, Sequence


def _normalise_status(value: Any) -> str:
    if value is None:
        return "UNKNOWN"
    text = str(value).strip().upper()
    return text or "UNKNOWN"


def _coerce_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _iter_mappings(value: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        yield value
        return

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            if isinstance(item, Mapping):
                yield item


def _format_person_name(person: Mapping[str, Any]) -> Optional[str]:
    components = []
    for key in ("firstName", "first_name"):
        value = person.get(key)
        if value:
            components.append(str(value).strip())
            break

    for key in ("lastName", "last_name"):
        value = person.get(key)
        if value:
            components.append(str(value).strip())
            break

    if components:
        return " ".join(part for part in components if part)

    fallback_keys = ("accountName", "nickname", "name", "personnelNumber", "id")
    for key in fallback_keys:
        value = person.get(key)
        text = _coerce_text(value)
        if text:
            return text

    return None


@dataclass
class OwnerServiceEntry:
    """Normalised representation of a single owner-facing service."""

    status: str
    description: str
    notes: Optional[str] = None

    def __post_init__(self) -> None:
        self.status = _normalise_status(self.status)
        self.description = _coerce_text(self.description) or ""
        self.notes = _coerce_text(self.notes)

    @property
    def is_complete(self) -> bool:
        return self.status == "OK"


@dataclass
class OwnerServicesSummary:
    """Collection of owner-facing services grouped by category."""

    departure_catering: List[OwnerServiceEntry] = field(default_factory=list)
    arrival_catering: List[OwnerServiceEntry] = field(default_factory=list)
    departure_ground_transport: List[OwnerServiceEntry] = field(default_factory=list)
    arrival_ground_transport: List[OwnerServiceEntry] = field(default_factory=list)

    @classmethod
    def from_payload(cls, payload: Any) -> "OwnerServicesSummary":
        summary = cls()

        if not isinstance(payload, Mapping):
            return summary

        summary.departure_catering.extend(
            _extract_owner_catering(payload.get("catering"))
        )
        summary.arrival_catering.extend(
            _extract_owner_catering(payload.get("arrivalCatering"))
        )
        summary.departure_ground_transport.extend(
            _extract_owner_transportation(payload.get("departureGroundTransportation"))
        )
        summary.arrival_ground_transport.extend(
            _extract_owner_transportation(payload.get("arrivalGroundTransportation"))
        )

        return summary

    def all_entries(self) -> List[OwnerServiceEntry]:
        return [
            *self.departure_catering,
            *self.arrival_catering,
            *self.departure_ground_transport,
            *self.arrival_ground_transport,
        ]

    @property
    def has_owner_services(self) -> bool:
        return bool(self.all_entries())

    @property
    def needs_attention(self) -> bool:
        return any(not entry.is_complete for entry in self.all_entries())


@dataclass
class OwnerServiceAuditEntry:
    """Row-level audit representation for service cost and receipt follow-up."""

    category: str
    direction: str
    status: str
    description: str
    notes: Optional[str] = None
    amount: Optional[float] = None
    currency: Optional[str] = None
    receipt_status: str = "Unknown"
    vendor: Optional[str] = None


def extract_owner_service_audit_entries(payload: Any) -> List[OwnerServiceAuditEntry]:
    """Extract owner-facing services into audit-ready rows."""

    if not isinstance(payload, Mapping):
        return []

    entries: List[OwnerServiceAuditEntry] = []
    service_sections = (
        ("Departure Catering", payload.get("catering"), "Departure", "Catering"),
        ("Arrival Catering", payload.get("arrivalCatering"), "Arrival", "Catering"),
        (
            "Departure Transport",
            payload.get("departureGroundTransportation"),
            "Departure",
            "Ground Transport",
        ),
        (
            "Arrival Transport",
            payload.get("arrivalGroundTransportation"),
            "Arrival",
            "Ground Transport",
        ),
    )

    for label, raw_items, direction, category in service_sections:
        for item in _iter_mappings(raw_items):
            if category == "Catering":
                service_for = _coerce_text(item.get("serviceFor") or item.get("service_for"))
                if service_for is None or service_for.lower() != "pax":
                    continue

            description = _coerce_text(
                item.get("details")
                or item.get("description")
                or item.get("type")
                or item.get("label")
                or label
            ) or label

            if category == "Ground Transport":
                person_name = _format_person_name(item.get("person", {})) if isinstance(item.get("person"), Mapping) else None
                if person_name:
                    description = f"{description} – {person_name}"

            amount, currency = _extract_amount_and_currency(item)

            entries.append(
                OwnerServiceAuditEntry(
                    category=category,
                    direction=direction,
                    status=_normalise_status(item.get("status")),
                    description=description,
                    notes=_coerce_text(item.get("notes") or item.get("paxNotes")),
                    amount=amount,
                    currency=currency,
                    receipt_status=_extract_receipt_status(item),
                    vendor=_coerce_text(
                        item.get("vendor")
                        or item.get("supplier")
                        or item.get("provider")
                        or item.get("company")
                    ),
                )
            )

    return entries


def _extract_amount_and_currency(item: Mapping[str, Any]) -> tuple[Optional[float], Optional[str]]:
    amount_candidates = (
        item.get("cost"),
        item.get("price"),
        item.get("amount"),
        item.get("totalCost"),
        item.get("estimatedCost"),
        item.get("quotedCost"),
    )

    amount: Optional[float] = None
    for candidate in amount_candidates:
        amount = _coerce_number(candidate)
        if amount is not None:
            break

    currency = _coerce_text(item.get("currency") or item.get("currencyCode"))

    for nested_key in ("cost", "price", "quote"):
        nested = item.get(nested_key)
        if not isinstance(nested, Mapping):
            continue
        if amount is None:
            amount = _coerce_number(
                nested.get("amount") or nested.get("value") or nested.get("total")
            )
        if currency is None:
            currency = _coerce_text(
                nested.get("currency") or nested.get("currencyCode") or nested.get("code")
            )

    return amount, currency


def _coerce_number(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _extract_receipt_status(item: Mapping[str, Any]) -> str:
    truthy_keys = (
        "hasReceipt",
        "receiptAttached",
        "receiptProvided",
        "invoiceAttached",
        "hasInvoice",
    )
    falsy_keys = ("missingReceipt", "receiptMissing")

    for key in truthy_keys:
        if item.get(key) is True:
            return "Provided"
    for key in falsy_keys:
        if item.get(key) is True:
            return "Missing"

    receipt_value = _coerce_text(item.get("receipt") or item.get("invoice"))
    if receipt_value:
        normalized = receipt_value.lower()
        if normalized in {"yes", "true", "attached", "provided"}:
            return "Provided"
        if normalized in {"no", "false", "missing", "not provided"}:
            return "Missing"

    attachments = item.get("attachments")
    if isinstance(attachments, Sequence) and not isinstance(attachments, (str, bytes, bytearray)):
        if len(list(attachments)) > 0:
            return "Provided"

    return "Unknown"


def _extract_owner_catering(payload: Any) -> List[OwnerServiceEntry]:
    entries: List[OwnerServiceEntry] = []

    for item in _iter_mappings(payload):
        service_for = _coerce_text(item.get("serviceFor") or item.get("service_for"))
        if service_for is None or service_for.lower() != "pax":
            continue

        details = _coerce_text(
            item.get("details")
            or item.get("description")
            or item.get("type")
            or item.get("label")
        )

        if not details:
            meals = _coerce_text(item.get("meals"))
            if meals:
                details = meals

        notes = item.get("notes") or item.get("paxNotes")

        entries.append(
            OwnerServiceEntry(
                status=item.get("status"),
                description=details or "Catering",
                notes=notes,
            )
        )

    return entries


def _extract_owner_transportation(payload: Any) -> List[OwnerServiceEntry]:
    entries: List[OwnerServiceEntry] = []

    for item in _iter_mappings(payload):
        person = item.get("person")
        if not isinstance(person, Mapping):
            continue

        if person.get("pilot") is not False:
            continue

        transport_type = _coerce_text(item.get("type") or item.get("category"))
        person_name = _format_person_name(person)

        description_parts = []
        if transport_type:
            description_parts.append(transport_type)
        if person_name:
            description_parts.append(person_name)

        description = " – ".join(description_parts) if description_parts else "Ground Transport"

        entries.append(
            OwnerServiceEntry(
                status=item.get("status"),
                description=description,
                notes=item.get("notes"),
            )
        )

    return entries


def format_owner_service_entries(entries: Sequence[OwnerServiceEntry]) -> str:
    if not entries:
        return "—"

    all_complete = all(entry.is_complete for entry in entries)
    header = "✅ Complete" if all_complete else "⚠️ Needs attention"

    lines: List[str] = []
    for entry in entries:
        line = f"• {entry.status}"
        if entry.description:
            line += f" – {entry.description}"
        if entry.notes:
            notes = entry.notes.replace("\r\n", "\n").strip()
            if notes:
                indented = "\n  " + notes.replace("\n", "\n  ")
                line += indented
        lines.append(line)

    body = "\n".join(lines)
    return f"{header}\n{body}" if body else header
