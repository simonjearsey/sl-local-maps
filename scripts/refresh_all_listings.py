#!/usr/bin/env python3
"""Refresh Hemnet + Booli source captures, dedupe them, and rebuild public data."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BOOLI_OUT = ROOT / "sources" / "booli_upplands_bro.json"
DEFAULT_BOOLI_URL = "https://www.booli.se/sok/till-salu?areaIds=33&maxPrice=3000000&minRooms=4"


def run(args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    print("$", " ".join(args), flush=True)
    result = subprocess.run(args, cwd=ROOT, text=True, check=False)
    if check and result.returncode != 0:
        raise SystemExit(result.returncode)
    return result


def main() -> None:
    booli_url = DEFAULT_BOOLI_URL
    search_path = ROOT / "data" / "search-parameters.json"
    if search_path.exists():
        payload = json.loads(search_path.read_text(encoding="utf-8"))
        for item in payload.get("items", []):
            if item.get("source") == "booli" and item.get("url"):
                booli_url = item["url"]
                break

    # Hemnet is fetchable directly; this records stable per-listing URLs.
    run([sys.executable, "scripts/update_listing_urls.py"])

    # Booli blocks simple HTTP HTML fetches, but the loaded Next.js page exposes
    # normalized Apollo listing data. Capture that through a separate Chrome
    # DevTools profile instead of fragile accessibility scraping.
    booli_result = run([
        sys.executable,
        "scripts/collect_booli_nextdata.py",
        booli_url,
        str(BOOLI_OUT),
        "--max-pages",
        "8",
    ], check=False)
    if booli_result.returncode != 0:
        print("WARNING: Booli refresh failed; keeping previous Booli source snapshot.", file=sys.stderr)

    run([sys.executable, "-c", "from listing_pipeline import build_listing_artifacts; rows=build_listing_artifacts(); print(f'Built deduplicated listing artifacts: {len(rows)} rows')"])
    run([sys.executable, "scripts/build_listing_summaries.py"])
    run([sys.executable, "scripts/update_version.py"])


if __name__ == "__main__":
    main()
