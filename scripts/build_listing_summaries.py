#!/usr/bin/env python3
"""Build small public JSON summaries for listing search metadata and sidebar lists."""
from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
SOURCES = ROOT / "sources"
LISTINGS = DATA / "listings.json"


def load_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def parse_price(value: str | None) -> int | None:
    if not value:
        return None
    nums = re.findall(r"\d+", value.replace("\xa0", " "))
    if not nums:
        return None
    return int("".join(nums))


def format_filter_label(url: str) -> str:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    labels = []
    for key, values in qs.items():
        clean_key = unquote(key).replace("[]", "")
        clean_key = {"maxPrice": "price_max", "maxListPrice": "price_max", "minRooms": "rooms_min"}.get(clean_key, clean_key)
        labels.append(f"{clean_key}: {', '.join(values)}")
    return "; ".join(labels) or url


def load_source_metadata() -> dict[str, dict]:
    metadata = {}
    preferred_booli = SOURCES / "booli_graphql_area.json"
    for path in SOURCES.glob("*.json"):
        if preferred_booli.exists() and path.name.startswith("booli_") and path.name != preferred_booli.name:
            continue
        payload = load_json(path, None)
        if not isinstance(payload, dict):
            continue
        url = payload.get("search_url")
        if not url:
            continue
        metadata[url] = {
            "source": payload.get("source"),
            "elapsed_seconds": payload.get("elapsed_seconds"),
            "pages_seen": payload.get("pages_seen"),
            "raw_items_seen": payload.get("raw_items_seen"),
            "max_sl_stop_distance_m": payload.get("max_sl_stop_distance_m"),
            "captured_items": len(payload.get("items") or []),
        }
    return metadata


def build_search_parameters(items: list[dict]) -> dict:
    source_metadata = load_source_metadata()
    if source_metadata:
        rows = []
        for url, extra in sorted(source_metadata.items(), key=lambda kv: ((kv[1].get("source") or ""), kv[0])):
            source = extra.get("source") or "unknown"
            rows.append({
                "source": source,
                "url": url,
                "parameters": format_filter_label(url),
                **{key: value for key, value in extra.items() if key != "source"},
            })
        return {
            "items": rows,
            "note": "These source searches cover Stockholm, Uppsala, and Södermanland county-level pages, then the local build keeps listings whose coordinates are within 500m of an SL stop point.",
        }

    source_urls: dict[str, set[str]] = {}
    for item in items:
        sources = item.get("sources") or [item.get("source") or "unknown"]
        urls = item.get("source_urls") or ([item.get("source_url")] if item.get("source_url") else [])
        for source in sources:
            for url in urls:
                if url:
                    source_urls.setdefault(source, set()).add(url)

    rows = []
    for source, urls in sorted(source_urls.items()):
        for url in sorted(urls):
            extra = source_metadata.get(url, {})
            rows.append({
                "source": source,
                "url": url,
                "parameters": format_filter_label(url),
                **extra,
            })

    return {
        "items": rows,
        "note": "These source searches cover Stockholm, Uppsala, and Södermanland county-level pages, then the local build keeps listings whose coordinates are within 500m of an SL stop point.",
    }


def build_new_objects(items: list[dict]) -> dict:
    new_items = [item for item in items if item.get("category") == "new"]
    new_items.sort(key=lambda x: (x.get("location") or "", x.get("title") or ""))
    return {
        "count": len(new_items),
        "items": [
            {
                "title": item.get("title"),
                "location": item.get("location"),
                "price": item.get("price"),
                "rooms": item.get("rooms"),
                "size": item.get("size"),
                "source": item.get("source"),
                "sources": item.get("sources") or [item.get("source")],
                "listing_url": item.get("listing_url"),
                "listing_urls": item.get("listing_urls") or [],
                "source_url": item.get("source_url"),
                "source_urls": item.get("source_urls") or [],
                "matched_name": item.get("matched_name"),
            }
            for item in new_items
        ],
    }


def build_sold_items() -> dict:
    # Optional manual/source file format:
    # [{"title":"...", "location":"...", "initial_price":"...", "sold_price":"...", "sold_at":"...", "url":"..."}]
    raw = load_json(SOURCES / "sold_listings.json", [])
    rows = []
    for item in raw:
        initial = parse_price(item.get("initial_price") or item.get("list_price"))
        sold = parse_price(item.get("sold_price") or item.get("final_price"))
        delta = sold - initial if sold is not None and initial is not None else None
        delta_pct = round(delta / initial * 100, 1) if delta is not None and initial else None
        rows.append({**item, "delta_kr": delta, "delta_pct": delta_pct})
    return {
        "count": len(rows),
        "items": rows,
        "note": "No sold-result source has been captured yet." if not rows else "Sold prices compared with original captured listing prices.",
    }


def main() -> None:
    payload = load_json(LISTINGS, {"items": []})
    items = payload.get("items", [])
    (DATA / "search-parameters.json").write_text(json.dumps(build_search_parameters(items), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (DATA / "new-objects.json").write_text(json.dumps(build_new_objects(items), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (DATA / "sold-listings.json").write_text(json.dumps(build_sold_items(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("Built listing summary JSON")


if __name__ == "__main__":
    main()
