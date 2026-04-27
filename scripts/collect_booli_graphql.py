#!/usr/bin/env python3
"""Collect Booli listings using direct persisted GraphQL pagination.

This is the preferred efficient Booli collector. It calls
https://www.booli.se/graphql directly with Booli's public Apollo persisted query
hash for the `search` operation, then filters locally to listings within a
configured distance of an SL stop point. If the GraphQL query stops working, this
script fails loudly; it must not silently fall back to Chrome/page scraping.
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sl_proximity import DEFAULT_MAX_DISTANCE_M, is_near_sl_stop, load_stop_points

GRAPHQL_URL = "https://www.booli.se/graphql"
SEARCH_HASH = "a0750f5336e2164d2537f968136735513e72b46fe2c87854d4019cb6ae218219"
DEFAULT_AREA_ID = "2,118,26"  # Stockholm, Uppsala, Södermanland counties.
DEFAULT_SEARCH_URL = "https://www.booli.se/sok/till-salu?areaIds=2,118,26&maxListPrice=3000000&minRooms=4"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"


def search_url(area_id: str, max_price: str, min_rooms: str) -> str:
    return f"https://www.booli.se/sok/till-salu?areaIds={urllib.parse.quote(area_id)}&maxListPrice={max_price}&minRooms={min_rooms}"


def variables(area_id: str, max_price: str, min_rooms: str, page: int) -> dict[str, Any]:
    return {
        "queryContext": "SERP_LIST_LISTING",
        "limit": 5,
        "input": {
            "filters": [
                {"key": "maxListPrice", "value": str(max_price)},
                {"key": "minRooms", "value": str(min_rooms)},
            ],
            "areaId": str(area_id),
            "sort": "",
            "page": page,
            "ascending": False,
            "excludeAncestors": True,
            "facets": ["upcomingSale"],
        },
    }


def graphql_search(area_id: str, max_price: str, min_rooms: str, page: int, referer: str) -> dict[str, Any]:
    extensions = {
        "clientLibrary": {"name": "@apollo/client", "version": "4.1.6"},
        "persistedQuery": {"version": 1, "sha256Hash": SEARCH_HASH},
    }
    params = urllib.parse.urlencode(
        {
            "operationName": "search",
            "variables": json.dumps(variables(area_id, max_price, min_rooms, page), separators=(",", ":")),
            "extensions": json.dumps(extensions, separators=(",", ":")),
        }
    )
    req = urllib.request.Request(
        f"{GRAPHQL_URL}?{params}",
        headers={
            "accept": "application/graphql-response+json,application/json;q=0.9",
            "content-type": "application/json",
            "api-client": "booli.se",
            "referer": referer,
            "user-agent": USER_AGENT,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Booli GraphQL request failed on page {page}: {exc}") from exc
    if payload.get("errors"):
        raise RuntimeError(f"Booli GraphQL returned errors on page {page}: {payload['errors']}")
    result = (((payload.get("data") or {}).get("forSale")) or {})
    if not result:
        raise RuntimeError(f"Booli GraphQL response missing data.forSale on page {page}")
    return result


def data_point_values(entity: dict[str, Any]) -> list[str]:
    attrs = entity.get("displayAttributes") or {}
    out: list[str] = []
    for point in attrs.get("dataPoints") or []:
        value = (point.get("value") or {}).get("plainText")
        if value:
            out.append(value)
    return out


def municipality(entity: dict[str, Any]) -> str | None:
    return (((entity.get("location") or {}).get("region") or {}).get("municipalityName"))


def listing_from_entity(entity: dict[str, Any], source_url: str) -> dict[str, Any] | None:
    typename = entity.get("__typename")
    if typename == "Project":
        return project_from_entity(entity, source_url)
    if typename != "Listing":
        return None
    datapoints = data_point_values(entity)
    rooms = next((v for v in datapoints if "rum" in v), None)
    size = next((v for v in datapoints if "m²" in v and "tomt" not in v), None)
    tags: list[str] = []
    if entity.get("isNewConstruction"):
        tags.append("Nyproduktionsprojekt")
    location = ", ".join(part for part in [entity.get("descriptiveAreaName"), municipality(entity)] if part)
    rel_url = entity.get("url") or ""
    listing_url = rel_url if rel_url.startswith("http") else f"https://www.booli.se{rel_url}"
    return {
        "source": "booli",
        "source_id": str(entity.get("booliId") or entity.get("id")),
        "source_url": source_url,
        "listing_url": listing_url,
        "title": entity.get("streetAddress") or entity.get("descriptiveAreaName") or f"Booli {entity.get('id')}",
        "location": location,
        "price": ((entity.get("listPrice") or {}).get("formatted")),
        "rooms": rooms,
        "size": size,
        "property_type": entity.get("objectType"),
        "tags": tags,
        "days_on_booli": f"{entity.get('daysActive')} dagar på Booli" if entity.get("daysActive") is not None else None,
        "published": entity.get("published"),
        "lat": entity.get("latitude"),
        "lon": entity.get("longitude"),
    }


def project_from_entity(entity: dict[str, Any], source_url: str) -> dict[str, Any]:
    datapoints = data_point_values(entity)
    rooms = next((v for v in datapoints if "rum" in v), None)
    size = next((v for v in datapoints if "m²" in v), None)
    rel_url = entity.get("booliUrl") or ""
    listing_url = rel_url if rel_url.startswith("http") else f"https://www.booli.se{rel_url}"
    return {
        "source": "booli",
        "source_id": f"project:{entity.get('booliId') or entity.get('id')}",
        "source_url": source_url,
        "listing_url": listing_url,
        "title": entity.get("name"),
        "location": entity.get("subtitle"),
        "price": entity.get("listPriceRange"),
        "rooms": rooms,
        "size": size,
        "property_type": "Nyproduktionsprojekt",
        "tags": [tag for tag in ["Nyproduktionsprojekt", entity.get("phase")] if tag],
        "lat": entity.get("latitude"),
        "lon": entity.get("longitude"),
    }


def collect(area_id: str, max_price: str, min_rooms: str, out_path: Path, max_pages: int, max_distance_m: int) -> None:
    started = time.perf_counter()
    source_url = search_url(area_id, max_price, min_rooms)
    stops = load_stop_points()
    all_items: dict[str, dict[str, Any]] = {}
    raw_items = 0
    pages_seen = 0
    total_pages: int | None = None
    total_count: int | None = None

    for page in range(1, max_pages + 1):
        page_started = time.perf_counter()
        result = graphql_search(area_id, max_price, min_rooms, page, source_url)
        total_pages = int(result.get("pages") or 0) or total_pages
        total_count = int(result.get("totalCount") or 0) or total_count
        entities = result.get("result") or []
        raw_items += len(entities)
        kept = 0
        for entity in entities:
            item = listing_from_entity(entity, source_url)
            if not item:
                continue
            near, proximity = is_near_sl_stop(item.get("lat"), item.get("lon"), stops, max_distance_m)
            if not near:
                continue
            kept += 1
            if proximity:
                item.update(proximity)
            key = str(item.get("source_id") or item.get("listing_url") or item.get("title"))
            all_items[key] = {**all_items.get(key, {}), **item}
        pages_seen += 1
        print(f"page {page}: {len(entities)} raw, {kept} within {max_distance_m}m SL ({len(all_items)} total) in {time.perf_counter() - page_started:.1f}s")
        if not entities or (total_pages and page >= total_pages):
            break
        time.sleep(0.1)

    elapsed = time.perf_counter() - started
    payload = {
        "source": "booli",
        "method": "graphql",
        "graphql_url": GRAPHQL_URL,
        "search_hash": SEARCH_HASH,
        "search_url": source_url,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "max_sl_stop_distance_m": max_distance_m,
        "pages_seen": pages_seen,
        "total_pages": total_pages,
        "total_count": total_count,
        "raw_items_seen": raw_items,
        "elapsed_seconds": round(elapsed, 3),
        "items": list(all_items.values()),
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {out_path} with {len(all_items)} items in {elapsed:.1f}s")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("out", nargs="?", default="sources/booli_graphql_area.json")
    parser.add_argument("--area-id", default=DEFAULT_AREA_ID)
    parser.add_argument("--max-price", default="3000000")
    parser.add_argument("--min-rooms", default="4")
    parser.add_argument("--max-pages", type=int, default=100)
    parser.add_argument("--max-distance-m", type=int, default=DEFAULT_MAX_DISTANCE_M)
    args = parser.parse_args()
    collect(args.area_id, args.max_price, args.min_rooms, Path(args.out), args.max_pages, args.max_distance_m)


if __name__ == "__main__":
    main()
