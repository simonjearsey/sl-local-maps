#!/usr/bin/env python3
"""Refresh Hemnet + Booli source captures, dedupe them, and rebuild public data."""
from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BOOLI_OUT = ROOT / "sources" / "booli_graphql_area.json"
HEMNET_OUT = ROOT / "sources" / "hemnet_sl_area.json"
DEFAULT_BOOLI_URL = "https://www.booli.se/sok/till-salu?areaIds=2,118,26&maxListPrice=3000000&minRooms=4"
DEFAULT_HEMNET_URL = "https://www.hemnet.se/bostader?location_ids%5B%5D=17744&location_ids%5B%5D=17745&location_ids%5B%5D=17746&price_max=3000000&rooms_min=4"
MAX_SL_STOP_DISTANCE_M = 500


def run(args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    print("$", " ".join(args), flush=True)
    started = time.perf_counter()
    result = subprocess.run(args, cwd=ROOT, text=True, check=False)
    elapsed = time.perf_counter() - started
    print(f"⏱ {' '.join(args[:2])}: {elapsed:.1f}s", flush=True)
    if check and result.returncode != 0:
        raise SystemExit(result.returncode)
    return result


def main() -> None:
    booli_url = DEFAULT_BOOLI_URL
    hemnet_url = DEFAULT_HEMNET_URL
    search_path = ROOT / "data" / "search-parameters.json"
    if search_path.exists():
        payload = json.loads(search_path.read_text(encoding="utf-8"))
        for item in payload.get("items", []):
            if item.get("source") == "booli" and item.get("url"):
                booli_url = item["url"]
            if item.get("source") == "hemnet" and item.get("url"):
                hemnet_url = item["url"]

    total_started = time.perf_counter()

    # Hemnet is fetchable directly. Capture its embedded Next/Apollo card data,
    # then keep only listings within a short walk of an SL stop point.
    run([
        sys.executable,
        "scripts/collect_hemnet_nextdata.py",
        hemnet_url,
        str(HEMNET_OUT),
        "--max-pages",
        "80",
        "--max-distance-m",
        str(MAX_SL_STOP_DISTANCE_M),
    ])

    # Booli must use direct GraphQL pagination. If this breaks, fail loudly; do
    # not silently fall back to the expensive Chrome/NextData collector.
    run([
        sys.executable,
        "scripts/collect_booli_graphql.py",
        str(BOOLI_OUT),
        "--area-id",
        "2,118,26",
        "--max-price",
        "3000000",
        "--min-rooms",
        "4",
        "--max-pages",
        "100",
        "--max-distance-m",
        str(MAX_SL_STOP_DISTANCE_M),
    ])

    run([sys.executable, "-c", "from listing_pipeline import build_listing_artifacts; rows=build_listing_artifacts(); print(f'Built deduplicated listing artifacts: {len(rows)} rows')"])
    run([sys.executable, "scripts/build_listing_summaries.py"])
    run([sys.executable, "scripts/update_version.py"])
    print(f"⏱ total refresh: {time.perf_counter() - total_started:.1f}s")


if __name__ == "__main__":
    main()
