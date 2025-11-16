"""Utility helpers for parsing and highlighting arrival weather data."""

from __future__ import annotations

import html
import re
from typing import Any, Iterable, Iterator, List, Optional


_CEILING_CODE_REGEX = re.compile(
    r"\b(BKN|OVC|VV)\s*(\d([\s,]?\d){1,})",
    re.IGNORECASE,
)
_CEILING_SUFFIX_REGEX = re.compile(
    r"^(?:\s*(?:FT\.?|FEET)|['′’])",
    re.IGNORECASE,
)


_HIGHLIGHT_SEVERITY = {"yellow": 1, "red": 2}
_WEATHER_PREFIXES = ("TS", "SH", "DR", "BL")
_WINTRY_CODES = ("SN", "SG", "PL", "IC", "GS", "GR")
_WEATHER_SPLIT_REGEX = re.compile(r"(\s+|,\s*)")


def _parse_fraction(value: str) -> Optional[float]:
    try:
        numerator, denominator = value.split("/", 1)
        return float(numerator) / float(denominator)
    except (ValueError, ZeroDivisionError):
        return None


def _try_float(value: str) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_visibility_value(value) -> Optional[float]:
    if value in (None, "", [], "M"):
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, dict):
        for key in ("value", "visibility", "minValue", "maxValue"):
            if key in value:
                nested_val = _parse_visibility_value(value[key])
                if nested_val is not None:
                    return nested_val
        return None

    if isinstance(value, (list, tuple)):
        for item in value:
            nested_val = _parse_visibility_value(item)
            if nested_val is not None:
                return nested_val
        return None

    text = str(value).strip().upper()
    if not text:
        return None

    if text == "P6SM":
        return 6.0

    if "SM" in text:
        text = text.replace("SM", "").strip()

    mixed_fraction_match = re.search(r"\b(\d+)\s+(\d+/\d+)\b", text)
    if mixed_fraction_match:
        integer_part = mixed_fraction_match.group(1)
        fraction_part = mixed_fraction_match.group(2)
        frac_val = _parse_fraction(fraction_part)
        if frac_val is not None:
            return float(integer_part) + frac_val

    if " " in text:
        parts = text.split()
        total = 0.0
        if parts[0].isdigit():
            total += float(parts[0])
            parts = parts[1:]
        if parts:
            frac_val = _parse_fraction(parts[0])
            if frac_val is not None:
                total += frac_val
                return total
    else:
        frac_val = _parse_fraction(text)
        if frac_val is not None:
            return frac_val

    return _try_float(text)


def _get_visibility_highlight(value) -> Optional[str]:
    vis_value = _parse_visibility_value(value)
    if vis_value is None:
        return None
    if vis_value <= 2.0:
        return "red"
    if vis_value <= 3.0:
        return "yellow"
    return None


def _parse_ceiling_value(value) -> Optional[float]:
    if value in (None, "", [], "M"):
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, (list, tuple)) and value:
        lowest: Optional[float] = None
        for item in value:
            parsed = _parse_ceiling_value(item)
            if parsed is None:
                continue
            if lowest is None or parsed < lowest:
                lowest = parsed
        return lowest

    if isinstance(value, dict):
        for key in ("value", "ceiling", "ceiling_ft_agl"):
            if key in value:
                parsed = _parse_ceiling_value(value[key])
                if parsed is not None:
                    return parsed
        return None

    try:
        text = str(value)
    except Exception:
        return None

    text = text.strip()
    if not text:
        return None

    upper_text = text.upper()
    match = _CEILING_CODE_REGEX.search(upper_text)
    if match:
        height_digits = re.sub(r"\D", "", match.group(2))
        if height_digits:
            height_value = int(height_digits)
            remainder = upper_text[match.end():]
            following = remainder.lstrip()
            if following.startswith(("FT", "FT.", "FEET", "'", "′", "’")):
                return float(height_value)
            if len(height_digits) == 3:
                return float(height_value * 100)
            return float(height_value)

    cleaned = upper_text.replace(",", "")
    for suffix in (" FT", "FT", " FT.", "FT."):
        if cleaned.endswith(suffix):
            cleaned = cleaned[:-len(suffix)].strip()
            break

    try:
        return float(cleaned)
    except ValueError:
        return None


def _get_ceiling_highlight(value) -> Optional[str]:
    ceiling_value = _parse_ceiling_value(value)
    if ceiling_value is None:
        return None
    if ceiling_value <= 2000:
        return "red"
    if ceiling_value <= 3000:
        return "yellow"
    return None


def _should_highlight_weather(value) -> bool:
    if value in (None, ""):
        return False
    if not isinstance(value, str):
        value = str(value)
    return "TS" in value.upper()


def _normalize_weather_code(token: str) -> str:
    normalized = token.strip()
    if not normalized:
        return ""
    normalized = normalized.strip(",;")
    normalized = normalized.strip("[](){}'\"")
    normalized = normalized.strip(",;")
    while normalized and normalized[0] in "+-":
        normalized = normalized[1:]
    if normalized.startswith("VC"):
        normalized = normalized[2:]
    changed = True
    while changed and normalized:
        changed = False
        for prefix in _WEATHER_PREFIXES:
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix) :]
                changed = True
    return normalized


def _iter_weather_tokens(value: Optional[str]) -> Iterator[str]:
    if not value:
        return
    if not isinstance(value, str):
        value = str(value)
    tokens = re.split(r"\s+", value.upper())
    for token in tokens:
        normalized = _normalize_weather_code(token)
        if normalized:
            yield normalized


def _has_freezing_precip(value: Optional[str]) -> bool:
    for token in _iter_weather_tokens(value):
        if token.startswith("FZ"):
            return True
    return False


def _has_wintry_precip(value: Optional[str]) -> bool:
    for token in _iter_weather_tokens(value):
        if token.startswith("FZ"):
            return True
        for code in _WINTRY_CODES:
            if code in token:
                return True
    return False


def _should_highlight_weather_token(token: str, deice_status: str) -> bool:
    if not token:
        return False
    if token.startswith("FZ"):
        return True
    if deice_status in {"none", "unknown"}:
        for code in _WINTRY_CODES:
            if code in token:
                return True
    return False


def _build_weather_value_html(value: Optional[str], deice_status: Optional[str]) -> Optional[str]:
    if value in (None, ""):
        return None
    text = str(value)
    if not text.strip():
        return None
    deice = (deice_status or "full").strip().lower()
    parts: List[str] = []
    highlighted = False
    for segment in _WEATHER_SPLIT_REGEX.split(text):
        if segment is None or segment == "":
            continue
        if _WEATHER_SPLIT_REGEX.fullmatch(segment):
            parts.append(html.escape(segment))
            continue
        normalized = _normalize_weather_code(segment.upper())
        if _should_highlight_weather_token(normalized, deice):
            highlighted = True
            parts.append(_wrap_highlight_html(html.escape(segment), "blue"))
        else:
            parts.append(html.escape(segment))
    if highlighted:
        return "".join(parts)
    return None


def _wrap_highlight_html(text: str, level: Optional[str]) -> str:
    if not level:
        return text
    color_map = {
        "red": "#c41230",
        "yellow": "#b8860b",
        "blue": "#38bdf8",
    }
    color = color_map.get(level, color_map["red"])
    css_classes = ["taf-highlight"]
    if level in ("red", "yellow", "blue"):
        css_classes.append(f"taf-highlight--{level}")
    return f"<span class='{' '.join(css_classes)}' style='color:{color};'>{text}</span>"


def _determine_highlight_level(label: str, value: Any) -> Optional[str]:
    label_lower = label.lower()
    highlight_level: Optional[str] = None
    if "visibility" in label_lower:
        highlight_level = _get_visibility_highlight(value)
    if not highlight_level and ("ceiling" in label_lower or "cloud" in label_lower):
        highlight_level = _get_ceiling_highlight(value)
    if not highlight_level and "weather" in label_lower and _should_highlight_weather(value):
        highlight_level = "red"
    return highlight_level


def _combine_highlight_levels(levels: Iterable[Optional[str]]) -> Optional[str]:
    """Return the strongest highlight level from the provided candidates."""

    best_level: Optional[str] = None
    best_score = -1
    for level in levels:
        if not level:
            continue
        score = _HIGHLIGHT_SEVERITY.get(level, 0)
        if score > best_score:
            best_level = level
            best_score = score
    return best_level


def _format_clouds_value(value: Any) -> str:
    if value in (None, ""):
        return ""

    text = str(value)
    parts: List[str] = []
    last_index = 0
    for match in _CEILING_CODE_REGEX.finditer(text):
        start, end = match.span()
        if start > last_index:
            parts.append(html.escape(text[last_index:start]))
        match_text = text[start:end]
        suffix_text = ""
        following_text = text[end:]
        suffix_match = _CEILING_SUFFIX_REGEX.match(following_text)
        if suffix_match:
            suffix_text = following_text[: suffix_match.end()]
        highlight_source = match_text + suffix_text
        highlight_level = _get_ceiling_highlight(highlight_source)
        escaped_match = html.escape(highlight_source)
        if highlight_level:
            parts.append(_wrap_highlight_html(escaped_match, highlight_level))
        else:
            parts.append(escaped_match)
        last_index = end + len(suffix_text)

    if last_index < len(text):
        parts.append(html.escape(text[last_index:]))

    if parts:
        return "".join(parts)

    return html.escape(text)


__all__ = [
    "_CEILING_CODE_REGEX",
    "_parse_fraction",
    "_try_float",
    "_parse_visibility_value",
    "_get_visibility_highlight",
    "_parse_ceiling_value",
    "_get_ceiling_highlight",
    "_should_highlight_weather",
    "_has_freezing_precip",
    "_has_wintry_precip",
    "_build_weather_value_html",
    "_wrap_highlight_html",
    "_determine_highlight_level",
    "_combine_highlight_levels",
    "_format_clouds_value",
]

