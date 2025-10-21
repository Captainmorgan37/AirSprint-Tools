import importlib.util
import sys
from pathlib import Path


def _load_dashboard_module():
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    module_name = "owner_services_dashboard_for_tests"
    spec = importlib.util.spec_from_file_location(
        module_name, root / "pages" / "Owner Services Dashboard.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_highlight_keywords_marks_sensitive_terms():
    module = _load_dashboard_module()

    note_text = "Ensure visa paperwork is secured prior to departure"
    highlighted, matches = module._highlight_keywords(note_text)

    assert matches == ["VISA"]
    assert "<mark>" not in highlighted
    assert highlighted == note_text


def test_extract_service_notes_uses_api_notes_section_only():
    module = _load_dashboard_module()

    payload = {
        "notes": [
            {"type": "General", "note": "Check passports before departure"},
            {"note": "Pets travelling – confirm documentation"},
            {"type": "General", "note": "Check passports before departure"},
        ],
        "catering": [
            {
                "status": "OK",
                "serviceFor": "Pax",
                "details": "Wraps",
                "notes": "No nuts",
            }
        ],
        "departureGroundTransportation": [
            {
                "status": "CONFIRMED",
                "type": "SUV",
                "person": {"firstName": "Jamie", "lastName": "Lee", "pilot": False},
                "notes": "Driver will wait",
            }
        ],
    }

    extracted = module._extract_service_notes(payload)

    assert extracted == [
        ("Owner service note – General", "Check passports before departure"),
        ("Owner service note", "Pets travelling – confirm documentation"),
    ]
