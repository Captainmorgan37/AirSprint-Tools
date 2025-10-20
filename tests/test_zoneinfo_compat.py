"""Tests for the zoneinfo compatibility helpers."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

def test_zoneinfo_compat_provides_zoneinfo():
    from zoneinfo_compat import ZoneInfo

    tz = ZoneInfo("UTC")
    assert tz is not None


def test_zoneinfo_compat_falls_back_to_backport(monkeypatch):
    original_import_module = importlib.import_module

    def fake_import_module(name: str, package: str | None = None):
        if name == "zoneinfo":
            raise ModuleNotFoundError("zoneinfo is unavailable")
        if name == "backports.zoneinfo":
            return SimpleNamespace(ZoneInfo="backport")
        return original_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)

    sys.modules.pop("zoneinfo_compat", None)
    try:
        module = importlib.import_module("zoneinfo_compat")
        assert module.ZoneInfo == "backport"
    finally:
        sys.modules.pop("zoneinfo_compat", None)
        importlib.invalidate_caches()
