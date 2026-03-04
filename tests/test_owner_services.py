from owner_services import (
    OwnerServicesSummary,
    extract_owner_service_audit_entries,
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



def test_extract_owner_service_audit_entries_includes_cost_and_receipt():
    payload = {
        "catering": [
            {
                "status": "OK",
                "serviceFor": "Pax",
                "details": "Lunch",
                "cost": {"amount": 125.5, "currency": "USD"},
                "receiptAttached": True,
                "vendor": "Sky Catering",
            }
        ]
    }

    entries = extract_owner_service_audit_entries(payload)

    assert len(entries) == 1
    entry = entries[0]
    assert entry.category == "Catering"
    assert entry.direction == "Departure"
    assert entry.amount == 125.5
    assert entry.currency == "USD"
    assert entry.receipt_status == "Provided"
    assert entry.vendor == "Sky Catering"


def test_extract_owner_service_audit_entries_handles_missing_receipt_status():
    payload = {
        "arrivalGroundTransportation": [
            {
                "status": "CONFIRMED",
                "type": "SUV",
                "person": {"firstName": "Pat", "lastName": "Case", "pilot": False},
                "amount": 90,
                "currencyCode": "CAD",
                "missingReceipt": True,
            }
        ]
    }

    entries = extract_owner_service_audit_entries(payload)

    assert len(entries) == 1
    entry = entries[0]
    assert entry.category == "Ground Transport"
    assert entry.direction == "Arrival"
    assert entry.amount == 90
    assert entry.currency == "CAD"
    assert entry.receipt_status == "Missing"


def test_extract_owner_service_audit_entries_filters_transport_by_owner_services():
    payload = {
        "arrivalGroundTransportation": [
            {
                "status": "CONFIRMED",
                "type": "SUV",
                "person": {"firstName": "Owner", "lastName": "Rider", "pilot": False},
                "by": "Owner Services",
            },
            {
                "status": "CONFIRMED",
                "type": "Sedan",
                "person": {"firstName": "Crew", "lastName": "Ride", "pilot": False},
                "by": "Flight support",
            },
        ]
    }

    entries = extract_owner_service_audit_entries(payload)

    assert len(entries) == 1
    assert entries[0].description.startswith("SUV")


def test_extract_owner_service_audit_entries_keeps_owner_services_variants():
    payload = {
        "arrivalGroundTransportation": [
            {
                "status": "CONFIRMED",
                "type": "SUV",
                "person": {"firstName": "Owner", "lastName": "Rider", "pilot": False},
                "by": "Owner Services Team",
            },
        ]
    }

    entries = extract_owner_service_audit_entries(payload)

    assert len(entries) == 1
    assert entries[0].description.startswith("SUV")


def test_extract_owner_service_audit_entries_filters_custom1_and_keeps_custom2():
    payload = {
        "arrivalGroundTransportation": [
            {
                "status": "CONFIRMED",
                "type": "SUV",
                "person": {"firstName": "Owner", "lastName": "Rider", "pilot": False},
                "by": "CUSTOM2",
            },
            {
                "status": "CONFIRMED",
                "type": "Sedan",
                "person": {"firstName": "Crew", "lastName": "Ride", "pilot": False},
                "by": "CUSTOM1",
            },
        ]
    }

    entries = extract_owner_service_audit_entries(payload)

    assert len(entries) == 1
    assert entries[0].description.startswith("SUV")
