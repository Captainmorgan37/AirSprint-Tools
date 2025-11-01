# core/neg_scheduler/contracts.py
from dataclasses import dataclass

@dataclass
class Flight:
    id: str
    origin: str
    dest: str
    duration_min: int
    earliest_etd_min: int
    latest_etd_min: int
    preferred_etd_min: int
    fleet_class: str   # e.g., "CJ", "LEG"
    owner_id: str

@dataclass
class Tail:
    id: str
    fleet_class: str
    available_from_min: int = 0
    available_to_min: int = 24 * 60

