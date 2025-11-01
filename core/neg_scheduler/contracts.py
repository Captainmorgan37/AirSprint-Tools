"""Pydantic contracts for the negotiation-aware scheduler domain."""

from __future__ import annotations

from datetime import datetime, time
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator


class Leg(BaseModel):
    """Representation of a flight leg to be scheduled."""

    model_config = ConfigDict(frozen=True)

    id: str = Field(..., description="Identifier coming from FL3XX")
    owner_id: str = Field(..., description="Owner account reference")
    dep: str = Field(..., description="Departure aerodrome")
    arr: str = Field(..., description="Arrival aerodrome")
    block_min: int = Field(..., ge=0, description="Block time in minutes")
    fleet_class: str = Field(..., description="Compatible fleet grouping (e.g. CJ, LEG)")
    etd_lo: int = Field(..., ge=0, le=24 * 60, description="Earliest allowed ETD (minutes from midnight)")
    etd_hi: int = Field(..., ge=0, le=24 * 60, description="Latest allowed ETD (minutes from midnight)")
    preferred_etd: int = Field(..., ge=0, le=24 * 60, description="Preferred ETD (minutes from midnight)")
    requested_start_utc: Optional[datetime] = Field(
        None,
        description="Optional UTC timestamp for reference when displaying asks.",
    )

    @field_validator("etd_hi")
    @classmethod
    def validate_window(cls, hi: int, info: ValidationInfo):  # type: ignore[override]
        lo = info.data.get("etd_lo")
        if lo is not None and hi < lo:
            raise ValueError("etd_hi must be greater than or equal to etd_lo")
        return hi


class Tail(BaseModel):
    """Representation of a tail with its availability window."""

    model_config = ConfigDict(frozen=True)

    id: str
    fleet_class: str
    available_lo: int = Field(0, ge=0, le=24 * 60)
    available_hi: int = Field(24 * 60, ge=0, le=24 * 60)
    maintenance_due: Optional[datetime] = None

    @field_validator("available_hi")
    @classmethod
    def validate_availability(cls, hi: int, info: ValidationInfo):  # type: ignore[override]
        lo = info.data.get("available_lo")
        if lo is not None and hi < lo:
            raise ValueError("available_hi must be greater than or equal to available_lo")
        return hi


def minutes_since_midnight(value: time | datetime) -> int:
    """Utility helper shared by adapters for converting time to minutes."""

    if isinstance(value, datetime):
        value = value.time()
    return value.hour * 60 + value.minute

