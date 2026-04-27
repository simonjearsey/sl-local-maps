#!/usr/bin/env python3
"""Collect Hemnet listings from Next.js/Apollo state and filter near SL stops."""
from __future__ import annotations

import argparse
import html
import json
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sl_proximity import DEFAULT_MAX_DISTANCE_M, is_near_sl_stop, load_stop_points

DEFAULT_URL = "https://www.hemnet.se/bostader?location_ids%5B%5D=17744&location_ids%5B%5D=17745&location_ids%5B%5D=17746&price_max=3000000&rooms_min=4"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"


def with_page(url: str, page_no: int) -> str:
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    if page_no <= 1:
        qs.pop("page", None)
    else:
        qs["page"] = [str(page_no)]
    return urllib.parse.urlunparse(parsed._replace(query=urllib.parse.urlencode(qs, doseq=True)))


def fetch_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "text/html"})
    with urllib.request.urlopen(req, timeout=45) as response:
        return response.read().decode("utf-8", "ignore")


def extract_next_data(html_text: str) -> dict[str, Any] | None:
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html_text, re.S)
    if not m:
        return None
    return json.loads(html.unescape(m.group(1)))


def parse_published_age(published_at: str | None) -> str | None:
    if not published_at:
        return None
    try:
        ts = float(published_at)
    except ValueError:
        return None
    days = max(0, round((datetime.now(timezone.utc).timestamp() - ts) / 86400))
    return "idag på Hemnet" if days == 0 else f"{days} dagar på Hemnet"


def listing_from_card(card: dict[str, Any], search_url: str) -> dict[str, Any] | None:
    coords = card.get("coordinates") or {}
    lat = coords.get("lat")
    lon = coords.get("long")
    title = card.get("streetAddress") or card.get("heading") or card.get("address")
    # Hemnet card headings live in the slug reliably; convert it into a readable fallback.
    slug = card.get("slug") or ""
    if not title and slug:
        title = re.sub(r"-\d+$", "", slug.rsplit("/", 1)[-1]).replace("-", " ").title()
    location = card.get("locationDescription")
    rel_url = f"/bostad/{slug}" if slug and not slug.startswith("/") else slug
    listing_url = rel_url if str(rel_url).startswith("http") else f"https://www.hemnet.se{rel_url}"
    labels = [label.get("text") for label in card.get("labels") or [] if label.get("text")]
    tags = [card.get("activePackage") or "", *labels]
    housing_form = card.get("housingForm") or {}
    return {
        "source": "hemnet",
        "source_id": str(card.get("id")),
        "source_url": search_url,
        "listing_url": listing_url,
        "title": title,
        "location": location,
        "price": card.get("askingPrice"),
        "rooms": card.get("rooms"),
        "size": card.get("livingAndSupplementalAreas"),
        "property_type": housing_form.get("name"),
        "floor": card.get("floor"),
        "fee": card.get("fee"),
        "blurb": card.get("description"),
        "tags": [tag for tag in tags if tag],
        "days_on_market": parse_published_age(card.get("publishedAt")),
        "lat": lat,
        "lon": lon,
    }


def extract_items(next_data: dict[str, Any], search_url: str) -> list[dict[str, Any]]:
    state = (((next_data.get("props") or {}).get("pageProps") or {}).get("__APOLLO_STATE__") or {})
    items = []
    for key, entity in state.items():
        if not isinstance(entity, dict):
            continue
        if key.startswith("ListingCard:") or entity.get("__typename") == "ListingCard":
            item = listing_from_card(entity, search_url)
            if item and item.get("title"):
                items.append(item)
    return items


def collect(url: str, out_path: Path, max_pages: int, max_distance_m: int) -> None:
    started = time.perf_counter()
    stops = load_stop_points()
    all_items: dict[str, dict[str, Any]] = {}
    raw_seen: set[str] = set()
    pages_seen = 0
    raw_items = 0
    for page_no in range(1, max_pages + 1):
        page_url = with_page(url, page_no)
        page_started = time.perf_counter()
        html_text = fetch_text(page_url)
        next_data = extract_next_data(html_text)
        if not next_data:
            if page_no == 1:
                raise RuntimeError("Hemnet page did not expose __NEXT_DATA__")
            break
        page_items = extract_items(next_data, url)
        raw_items += len(page_items)
        raw_before = len(raw_seen)
        kept = 0
        for item in page_items:
            raw_key = str(item.get("source_id") or item.get("listing_url") or item.get("title"))
            raw_seen.add(raw_key)
            near, proximity = is_near_sl_stop(item.get("lat"), item.get("lon"), stops, max_distance_m)
            if not near:
                continue
            kept += 1
            if proximity:
                item.update(proximity)
            all_items[raw_key] = {**all_items.get(raw_key, {}), **item}
        pages_seen += 1
        print(f"page {page_no}: {len(page_items)} raw, {kept} within {max_distance_m}m SL ({len(all_items)} total) in {time.perf_counter() - page_started:.1f}s")
        if not page_items:
            break
        if page_no > 2 and len(raw_seen) == raw_before:
            break
        time.sleep(0.25)
    elapsed = time.perf_counter() - started
    payload = {
        "source": "hemnet",
        "search_url": url,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "max_sl_stop_distance_m": max_distance_m,
        "pages_seen": pages_seen,
        "raw_items_seen": raw_items,
        "elapsed_seconds": round(elapsed, 3),
        "items": list(all_items.values()),
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {out_path} with {len(all_items)} items in {elapsed:.1f}s")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("url", nargs="?", default=DEFAULT_URL)
    parser.add_argument("out", nargs="?", default="sources/hemnet_sl_area.json")
    parser.add_argument("--max-pages", type=int, default=80)
    parser.add_argument("--max-distance-m", type=int, default=DEFAULT_MAX_DISTANCE_M)
    args = parser.parse_args()
    collect(args.url, Path(args.out), args.max_pages, args.max_distance_m)


if __name__ == "__main__":
    main()
