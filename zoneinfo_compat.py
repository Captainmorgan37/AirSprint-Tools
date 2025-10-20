"""Compatibility helpers for loading :class:`ZoneInfo`."""

from __future__ import annotations

import importlib


def _load_zoneinfo_class():
    """Return the ``ZoneInfo`` class from the stdlib or backports package."""

    try:
        zoneinfo_module = importlib.import_module("zoneinfo")
        return zoneinfo_module.ZoneInfo
    except ModuleNotFoundError:
        try:
            backport_module = importlib.import_module("backports.zoneinfo")
            return backport_module.ZoneInfo
        except ModuleNotFoundError as exc:  # pragma: no cover - depends on environment
            raise ImportError(
                "ZoneInfo support requires Python 3.9+ or the 'backports.zoneinfo' package."
            ) from exc


ZoneInfo = _load_zoneinfo_class()

__all__ = ["ZoneInfo"]
