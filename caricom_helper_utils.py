from __future__ import annotations

from typing import Any, Callable, Iterable, Mapping


def _route_touches_caricom(
    leg: Mapping[str, Any],
    is_caricom_airport: Callable[[Any], bool],
) -> bool:
    return bool(
        is_caricom_airport(leg.get("departure_airport"))
        or is_caricom_airport(leg.get("arrival_airport"))
    )


def _arrives_in_caricom(
    leg: Mapping[str, Any],
    is_caricom_airport: Callable[[Any], bool],
) -> bool:
    return bool(is_caricom_airport(leg.get("arrival_airport")))


def select_booking_leg_for_caricom(
    matched_legs: Iterable[Mapping[str, Any]],
    is_caricom_airport: Callable[[Any], bool],
) -> dict[str, Any] | None:
    """Select the best leg for a CARICOM workbook from matching booking legs.

    Preference order:
    1) Earliest leg that arrives in CARICOM.
    2) Earliest leg that touches CARICOM.
    3) Earliest leg overall.
    """
    sorted_legs = sorted(matched_legs, key=lambda leg: leg.get("dep_time") or "")
    if not sorted_legs:
        return None

    arriving_candidates = [
        leg for leg in sorted_legs if _arrives_in_caricom(leg, is_caricom_airport)
    ]
    if arriving_candidates:
        return dict(arriving_candidates[0])

    touching_candidates = [
        leg for leg in sorted_legs if _route_touches_caricom(leg, is_caricom_airport)
    ]
    if touching_candidates:
        return dict(touching_candidates[0])

    return dict(sorted_legs[0])
