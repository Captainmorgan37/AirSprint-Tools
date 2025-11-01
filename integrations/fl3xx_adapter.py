"""Adapters for sourcing legs/tails from FL3XX or local demos."""

from __future__ import annotations

from typing import Sequence, Tuple

from core.neg_scheduler.contracts import Leg, Tail


def get_demo_data() -> Tuple[list[Leg], list[Tail]]:
    """Return a deterministic demo dataset for local prototyping."""

    legs = [
        Leg(
            id="F1",
            owner_id="O100",
            dep="CYBW",
            arr="CYVR",
            block_min=120,
            fleet_class="CJ",
            etd_lo=7 * 60,
            etd_hi=8 * 60,
            preferred_etd=7 * 60 + 15,
        ),
        Leg(
            id="F2",
            owner_id="O220",
            dep="CYVR",
            arr="KSEA",
            block_min=45,
            fleet_class="CJ",
            etd_lo=9 * 60,
            etd_hi=10 * 60,
            preferred_etd=9 * 60 + 15,
        ),
        Leg(
            id="F3",
            owner_id="O330",
            dep="CYVR",
            arr="CYUL",
            block_min=300,
            fleet_class="LEG",
            etd_lo=9 * 60 + 30,
            etd_hi=12 * 60,
            preferred_etd=10 * 60,
        ),
        Leg(
            id="F4",
            owner_id="O220",
            dep="KSEA",
            arr="CYBW",
            block_min=90,
            fleet_class="CJ",
            etd_lo=10 * 60,
            etd_hi=12 * 60,
            preferred_etd=10 * 60 + 30,
        ),
        Leg(
            id="F5",
            owner_id="O555",
            dep="CYBW",
            arr="KDEN",
            block_min=160,
            fleet_class="CJ",
            etd_lo=8 * 60,
            etd_hi=9 * 60,
            preferred_etd=8 * 60 + 15,
        ),
    ]

    tails = [
        Tail(id="C-GCJ1", fleet_class="CJ"),
        Tail(id="C-GCJ2", fleet_class="CJ"),
        Tail(id="C-GLEG1", fleet_class="LEG"),
    ]

    return legs, tails


def fetch_from_fl3xx(*, include_owners: Sequence[str] | None = None) -> Tuple[list[Leg], list[Tail]]:
    """Placeholder for the real FL3XX integration.

    Replace this function with calls into the production FL3XX helpers once the
    API contract is locked. The ``include_owners`` parameter mirrors the
    negotiation plan filters so downstream usage does not change when the
    implementation is swapped in.
    """

    # TODO: wire to ``flight_leg_utils`` once authenticated session helpers are ready.
    return get_demo_data()
