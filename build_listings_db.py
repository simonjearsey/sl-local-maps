#!/usr/bin/env python3
from __future__ import annotations

from collections import Counter

from listing_pipeline import LISTINGS_DB_PATH, LISTINGS_JSON_OUT, build_listing_artifacts


def main() -> None:
    rows = build_listing_artifacts()
    counts = Counter(row["source"] for row in rows)
    mapped = sum(1 for row in rows if row.get("lat") is not None and row.get("lon") is not None)
    print(f"Wrote {LISTINGS_DB_PATH}")
    print(f"Wrote {LISTINGS_JSON_OUT}")
    print(f"Rows: {len(rows)}")
    print(f"Mapped rows: {mapped}")
    print(f"By source: {dict(counts)}")


if __name__ == "__main__":
    main()
