from owner_services import (
    OwnerServicesSummary,
    format_owner_service_entries,
)


def test_catering_filters_to_pax_only():
    payload = {
        "catering": [
            {"status": "OK", "serviceFor": "Crew", "details": "Crew snacks"},
            {
                "status": "OK",
                "serviceFor": "Pax",
                "details": "Sandwiches",
                "notes": "Gluten free",
            },
        ]
    }

    summary = OwnerServicesSummary.from_payload(payload)

    assert len(summary.departure_catering) == 1
    entry = summary.departure_catering[0]
    assert entry.status == "OK"
    assert entry.description == "Sandwiches"
    assert entry.notes == "Gluten free"
    assert entry.is_complete


def test_ground_transport_requires_owner_person():
    payload = {
        "departureGroundTransportation": [
            {
                "status": "PENDING",
                "type": "Limo",
                "person": {"firstName": "Alex", "lastName": "Smith", "pilot": True},
            },
            {
                "status": "CONFIRMED",
                "type": "SUV",
                "person": {
                    "firstName": "Jamie",
                    "lastName": "Lee",
                    "pilot": False,
                },
            },
        ]
    }

    summary = OwnerServicesSummary.from_payload(payload)

    assert len(summary.departure_ground_transport) == 1
    entry = summary.departure_ground_transport[0]
    assert entry.status == "CONFIRMED"
    assert "Jamie" in entry.description
    assert entry.description.startswith("SUV")
    assert not entry.is_complete


def test_summary_flags_attention_for_non_ok_status():
    payload = {
        "catering": {"status": "PENDING", "serviceFor": "Pax", "details": "Wraps"}
    }

    summary = OwnerServicesSummary.from_payload(payload)

    assert summary.has_owner_services
    assert summary.needs_attention


def test_format_owner_service_entries_renders_header_and_body():
    payload = {
        "arrivalGroundTransportation": {
            "status": "OK",
            "type": "Car Rental",
            "person": {"firstName": "Taylor", "lastName": "Jordan", "pilot": False},
            "notes": "Pickup at FBO",
        }
    }

    summary = OwnerServicesSummary.from_payload(payload)
    formatted = format_owner_service_entries(summary.arrival_ground_transport)

    assert formatted.startswith("✅ Complete")
    assert "Car Rental" in formatted
    assert "Pickup at FBO" in formatted


def test_invalid_payload_yields_empty_summary():
    summary = OwnerServicesSummary.from_payload(None)
    assert not summary.has_owner_services
    assert not summary.needs_attention
    assert format_owner_service_entries(summary.departure_catering) == "—"

