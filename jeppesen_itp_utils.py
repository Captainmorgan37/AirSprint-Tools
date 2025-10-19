"""Shared constants and helpers for the Jeppesen ITP report."""

from __future__ import annotations

from typing import Optional


CARIBBEAN_COUNTRY_NAMES = {
    "anguilla",
    "antigua and barbuda",
    "aruba",
    "bahamas",
    "bahamas, the",
    "barbados",
    "bermuda",
    "bonaire",
    "british virgin islands",
    "cayman islands",
    "cuba",
    "curacao",
    "dominica",
    "dominican republic",
    "french guiana",
    "grenada",
    "guadeloupe",
    "haiti",
    "jamaica",
    "martinique",
    "montserrat",
    "puerto rico",
    "saba",
    "saint barthelemy",
    "saint kitts and nevis",
    "saint lucia",
    "saint martin",
    "saint vincent and the grenadines",
    "sint eustatius",
    "sint maarten",
    "st. barthelemy",
    "st. kitts and nevis",
    "st. lucia",
    "st. martin",
    "st. vincent and the grenadines",
    "trinidad and tobago",
    "turks and caicos islands",
    "u.s. virgin islands",
    "united states virgin islands",
    "virgin islands",
}

CARIBBEAN_COUNTRY_CODES = {
    "ai",  # Anguilla
    "ag",  # Antigua and Barbuda
    "aw",  # Aruba
    "bs",  # Bahamas
    "bb",  # Barbados
    "bm",  # Bermuda
    "bq",  # Bonaire, Sint Eustatius, and Saba (special municipality)
    "vg",  # British Virgin Islands
    "ky",  # Cayman Islands
    "cu",  # Cuba
    "cw",  # Curaçao
    "dm",  # Dominica
    "do",  # Dominican Republic
    "gf",  # French Guiana
    "gd",  # Grenada
    "gp",  # Guadeloupe
    "ht",  # Haiti
    "jm",  # Jamaica
    "mq",  # Martinique
    "ms",  # Montserrat
    "pr",  # Puerto Rico
    "bl",  # Saint Barthélemy
    "kn",  # Saint Kitts and Nevis
    "lc",  # Saint Lucia
    "mf",  # Saint Martin (French part)
    "vc",  # Saint Vincent and the Grenadines
    "sx",  # Sint Maarten (Dutch part)
    "tt",  # Trinidad and Tobago
    "tc",  # Turks and Caicos Islands
    "vi",  # U.S. Virgin Islands
}

ALLOWED_COUNTRY_NAMES = {
    "canada",
    "mexico",
    "united states",
    "united states of america",
    "usa",
}
ALLOWED_COUNTRY_NAMES.update(CARIBBEAN_COUNTRY_NAMES)

ALLOWED_COUNTRY_CODES = {
    "ca",  # Canada
    "mx",  # Mexico
    "us",  # United States
}
ALLOWED_COUNTRY_CODES.update(CARIBBEAN_COUNTRY_CODES)

ALLOWED_COUNTRY_IDENTIFIERS = ALLOWED_COUNTRY_NAMES | ALLOWED_COUNTRY_CODES


def normalize_country_name(name: Optional[str]) -> Optional[str]:
    """Lower-case and strip the provided country name or code."""

    if not name:
        return None
    text = str(name).strip().lower()
    return text or None
