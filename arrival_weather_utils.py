"""Utility helpers for parsing and highlighting arrival weather data."""

from __future__ import annotations

import html
import re
from typing import Any, List, Optional


_CEILING_CODE_REGEX = re.compile(
    r"\b(BKN|OVC|VV)\s*(\d([\s,]?\d){1,})",
    re.IGNORECASE,
)


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


def _wrap_highlight_html(text: str, level: Optional[str]) -> str:
    if not level:
        return text
    color_map = {
        "red": "#c41230",
        "yellow": "#b8860b",
    }
    color = color_map.get(level, color_map["red"])
    css_classes = ["taf-highlight"]
    if level in ("red", "yellow"):
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
        highlight_level = _get_ceiling_highlight(match_text)
        escaped_match = html.escape(match_text)
        if highlight_level:
            parts.append(_wrap_highlight_html(escaped_match, highlight_level))
        else:
            parts.append(escaped_match)
        last_index = end

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
    "_wrap_highlight_html",
    "_determine_highlight_level",
    "_format_clouds_value",
]

