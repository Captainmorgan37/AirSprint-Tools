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



def test_extract_leg_note_blocks_includes_leg_and_planning_notes():
    module = _load_dashboard_module()

    payload = {
        "notes": "Standard leg note",
        "planningNotes": "There may be a special event fee at destination",
    }

    extracted = module._extract_leg_note_blocks(payload)

    assert extracted == [
        ("Leg notes", "Standard leg note"),
        ("Planning notes", "There may be a special event fee at destination"),
    ]


def test_build_sensitive_notes_rows_flags_special_event_terms_from_planning_notes():
    module = _load_dashboard_module()

    def fake_fetch_leg_details(_config, _quote_id, *, session=None):
        return {
            "planningNotes": "Owner advised of special event fee on arrival.",
        }

    def fake_fetch_flight_services(_config, _flight_id, *, session=None):
        return {"notes": []}

    module.fetch_leg_details = fake_fetch_leg_details
    module.fetch_flight_services = fake_fetch_flight_services

    rows = [
        {
            "quoteId": "Q-100",
            "flightId": "F-200",
            "dep_time": "2025-04-07T13:00:00Z",
            "tail": "C-GABC",
            "departure_airport": "CYUL",
            "arrival_airport": "CYYZ",
        }
    ]

    class _DummyConfig:
        pass

    display_rows, warnings, stats = module._build_sensitive_notes_rows(rows, _DummyConfig())

    assert warnings == []
    assert len(display_rows) == 1
    assert display_rows[0]["Matched Special Event Terms"] == "SPECIAL EVENT FEE"
    assert stats["legs_with_special_event_terms"] == 1


def test_build_sensitive_notes_rows_uses_row_level_planning_notes_without_quote_id():
    module = _load_dashboard_module()

    def fake_fetch_leg_details(_config, _quote_id, *, session=None):
        raise AssertionError("leg detail lookup should not run when quote id is missing")

    def fake_fetch_flight_services(_config, _flight_id, *, session=None):
        return {"notes": []}

    module.fetch_leg_details = fake_fetch_leg_details
    module.fetch_flight_services = fake_fetch_flight_services

    rows = [
        {
            "flightId": "F-201",
            "dep_time": "2025-02-06T13:00:00Z",
            "tail": "C-FSDO",
            "departure_airport": "CYOW",
            "arrival_airport": "CYXU",
            "planningNotes": "Please bill KSFO superbowl special event fee to owner",
        }
    ]

    class _DummyConfig:
        pass

    display_rows, warnings, stats = module._build_sensitive_notes_rows(rows, _DummyConfig())

    assert warnings == []
    assert len(display_rows) == 1
    assert display_rows[0]["Matched Special Event Terms"] == "SPECIAL EVENT FEE"
    assert stats["missing_quote_ids"] == 1
    assert stats["legs_with_special_event_terms"] == 1


def test_extract_leg_note_blocks_reads_all_items_from_multi_leg_payload():
    module = _load_dashboard_module()

    payload = [
        {
            "planningNotes": "06FEB CYOW-CYXU-KOAK\nGeneral routing notes",
        },
        {
            "planningNotes": "Please bill KSFO superbowl special event fee to owner - $7,050.00 USD",
        },
    ]

    extracted = module._extract_leg_note_blocks(payload)

    assert extracted == [
        ("Planning notes", "06FEB CYOW-CYXU-KOAK\nGeneral routing notes"),
        (
            "Planning notes",
            "Please bill KSFO superbowl special event fee to owner - $7,050.00 USD",
        ),
    ]
