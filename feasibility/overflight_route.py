"""Route helpers for overflight permit checks."""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Sequence, Tuple

LatLon = Tuple[float, float]

# Coarse country polygons used for route-crossing checks. These are intentionally
# approximate and are suitable for early feasibility alerting only.
_COUNTRY_POLYGONS: Dict[str, Sequence[LatLon]] = {
    "CUBA": (
        (19.8, -85.2),
        (23.6, -85.2),
        (23.6, -73.8),
        (19.8, -73.8),
    ),
    "HONDURAS": (
        (12.9, -89.6),
        (16.6, -89.6),
        (16.6, -82.0),
        (12.9, -82.0),
    ),
    "NICARAGUA": (
        (10.6, -88.3),
        (15.2, -88.3),
        (15.2, -82.6),
        (10.6, -82.6),
    ),
    "EL SALVADOR": (
        (13.1, -90.3),
        (14.6, -90.3),
        (14.6, -87.5),
        (13.1, -87.5),
    ),
    "GUATEMALA": (
        (13.5, -92.3),
        (17.9, -92.3),
        (17.9, -88.1),
        (13.5, -88.1),
    ),
}


def find_route_overflight_countries(
    departure: Optional[LatLon],
    arrival: Optional[LatLon],
    *,
    eligible_countries: Iterable[str],
) -> List[str]:
    """Return eligible countries crossed by the straight-line route."""

    if departure is None or arrival is None:
        return []
    dep = _normalize_point(departure)
    arr = _normalize_point(arrival)
    if dep is None or arr is None:
        return []

    countries: List[str] = []
    for country in sorted(set(code.upper().strip() for code in eligible_countries if str(code).strip())):
        polygon = _COUNTRY_POLYGONS.get(country)
        if not polygon:
            continue
        if _line_intersects_polygon(dep, arr, polygon):
            countries.append(country)
    return countries


def _normalize_point(value: Sequence[float]) -> Optional[LatLon]:
    if len(value) < 2:
        return None
    try:
        lat = float(value[0])
        lon = float(value[1])
    except (TypeError, ValueError):
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    return (lat, lon)


def _line_intersects_polygon(start: LatLon, end: LatLon, polygon: Sequence[LatLon]) -> bool:
    if _point_in_polygon(start, polygon) or _point_in_polygon(end, polygon):
        return True

    x1, y1 = start[1], start[0]
    x2, y2 = end[1], end[0]

    points = list(polygon)
    if len(points) < 3:
        return False

    for index, current in enumerate(points):
        nxt = points[(index + 1) % len(points)]
        ex1, ey1 = current[1], current[0]
        ex2, ey2 = nxt[1], nxt[0]
        if _segments_intersect((x1, y1), (x2, y2), (ex1, ey1), (ex2, ey2)):
            return True
    return False


def _point_in_polygon(point: LatLon, polygon: Sequence[LatLon]) -> bool:
    x, y = point[1], point[0]
    inside = False
    points = list(polygon)
    count = len(points)
    if count < 3:
        return False

    j = count - 1
    for i in range(count):
        xi, yi = points[i][1], points[i][0]
        xj, yj = points[j][1], points[j][0]
        intersects = ((yi > y) != (yj > y)) and (
            x < ((xj - xi) * (y - yi) / ((yj - yi) or 1e-12) + xi)
        )
        if intersects:
            inside = not inside
        j = i
    return inside


def _segments_intersect(
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    q1: Tuple[float, float],
    q2: Tuple[float, float],
) -> bool:
    o1 = _orientation(p1, p2, q1)
    o2 = _orientation(p1, p2, q2)
    o3 = _orientation(q1, q2, p1)
    o4 = _orientation(q1, q2, p2)

    if o1 != o2 and o3 != o4:
        return True

    if o1 == 0 and _on_segment(p1, q1, p2):
        return True
    if o2 == 0 and _on_segment(p1, q2, p2):
        return True
    if o3 == 0 and _on_segment(q1, p1, q2):
        return True
    if o4 == 0 and _on_segment(q1, p2, q2):
        return True
    return False


def _orientation(a: Tuple[float, float], b: Tuple[float, float], c: Tuple[float, float]) -> int:
    value = (b[1] - a[1]) * (c[0] - b[0]) - (b[0] - a[0]) * (c[1] - b[1])
    if abs(value) < 1e-10:
        return 0
    return 1 if value > 0 else 2


def _on_segment(a: Tuple[float, float], b: Tuple[float, float], c: Tuple[float, float]) -> bool:
    return (
        min(a[0], c[0]) - 1e-10 <= b[0] <= max(a[0], c[0]) + 1e-10
        and min(a[1], c[1]) - 1e-10 <= b[1] <= max(a[1], c[1]) + 1e-10
    )
