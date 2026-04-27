#!/usr/bin/env python3
"""Distance helpers for filtering listings near SL stops."""
from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TRANSPORT_DB_PATH = ROOT / "sl-db" / "sl_transport.sqlite"
DEFAULT_MAX_DISTANCE_M = 500


@dataclass(frozen=True)
class StopPoint:
    name: str
    lat: float
    lon: float
    stop_area_name: str | None = None
    stop_area_type: str | None = None


def load_stop_points(db_path: Path = TRANSPORT_DB_PATH) -> list[StopPoint]:
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        """
        SELECT name, lat, lon, stop_area_name, stop_area_type
        FROM stop_points
        WHERE lat IS NOT NULL AND lon IS NOT NULL
        """
    ).fetchall()
    conn.close()
    return [StopPoint(name=row[0], lat=row[1], lon=row[2], stop_area_name=row[3], stop_area_type=row[4]) for row in rows]


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * radius * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def nearest_stop(lat: float | None, lon: float | None, stops: list[StopPoint]) -> dict | None:
    if lat is None or lon is None:
        return None
    # Cheap bounding window first; 0.01° is roughly 700-1100m around Stockholm.
    candidates = [s for s in stops if abs(s.lat - lat) <= 0.01 and abs(s.lon - lon) <= 0.02]
    if not candidates:
        candidates = stops
    best_stop = None
    best_dist = float("inf")
    for stop in candidates:
        dist = haversine_m(lat, lon, stop.lat, stop.lon)
        if dist < best_dist:
            best_dist = dist
            best_stop = stop
    if best_stop is None:
        return None
    return {
        "nearest_sl_stop": best_stop.stop_area_name or best_stop.name,
        "nearest_sl_stop_point": best_stop.name,
        "nearest_sl_stop_type": best_stop.stop_area_type,
        "nearest_sl_stop_distance_m": round(best_dist),
    }


def is_near_sl_stop(lat: float | None, lon: float | None, stops: list[StopPoint], max_distance_m: int = DEFAULT_MAX_DISTANCE_M) -> tuple[bool, dict | None]:
    info = nearest_stop(lat, lon, stops)
    if not info:
        return False, None
    return info["nearest_sl_stop_distance_m"] <= max_distance_m, info
