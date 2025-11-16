"""Helpers for classifying airport deice capability information."""

from __future__ import annotations

from typing import Dict, Optional, Tuple
import re

from deice_info_helper import DeiceRecord, get_deice_record

__all__ = ["classify_deice_record", "resolve_deice_status"]


_TYPE_I_REGEX = re.compile(r"\btype(?:s)?\s*(?:i|1)\b", re.IGNORECASE)
_TYPE_IV_REGEX = re.compile(r"\btype(?:s)?\s*(?:iv|4)\b", re.IGNORECASE)
_TYPE_I_AND_IV_REGEX = re.compile(
    r"\btype(?:s)?\s*(?:i|1)\s*(?:[/,&]|(?:and))\s*(?:iv|4)\b",
    re.IGNORECASE,
)

_DEICE_STATUS_CACHE: Dict[str, Dict[str, str]] = {}


def _coerce_code(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip().upper()
    return text or None


def _extract_type_mentions(info_text: Optional[str]) -> Tuple[bool, bool]:
    if not info_text:
        return False, False
    has_type1 = bool(_TYPE_I_REGEX.search(info_text))
    has_type4 = bool(_TYPE_IV_REGEX.search(info_text))
    if _TYPE_I_AND_IV_REGEX.search(info_text):
        has_type1 = True
        has_type4 = True
    return has_type1, has_type4


def classify_deice_record(record: Optional[DeiceRecord]) -> Tuple[str, str]:
    if record is None:
        return "unknown", "Deice info unavailable"

    has_type1, has_type4 = _extract_type_mentions(record.deice_info)

    if record.has_deice is False:
        return "none", "No deice available"

    if has_type1 and not has_type4:
        return "partial", "Partial deice (Type I only)"

    if record.has_deice is True or has_type4:
        return "full", "Full deice capability"

    return "unknown", "Deice info unavailable"


def resolve_deice_status(airport_code: Optional[str]) -> Dict[str, str]:
    code = _coerce_code(airport_code)
    if not code:
        return {"code": "unknown", "label": "Deice info unavailable"}
    if code not in _DEICE_STATUS_CACHE:
        record = get_deice_record(icao=code)
        status_code, label = classify_deice_record(record)
        _DEICE_STATUS_CACHE[code] = {"code": status_code, "label": label}
    cached = _DEICE_STATUS_CACHE[code]
    return {"code": cached["code"], "label": cached["label"]}
