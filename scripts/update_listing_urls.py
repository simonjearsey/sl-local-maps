#!/usr/bin/env python3
"""Fetch source search pages and save per-listing URLs when they can be matched safely."""
from __future__ import annotations

import json
import re
import time
import unicodedata
import urllib.request
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SOURCES_DIR = ROOT / "sources"
DEFAULT_HEMNET_SEARCH_URL = "https://www.hemnet.se/bostader?location_ids%5B%5D=17744&price_max=3000000&rooms_min=4"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"


def normalize(text: str) -> str:
    text = (text or "").lower().replace("|", " ")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"\b(van|vån|tr|trappa)\b", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def slug_text(url: str) -> str:
    slug = urlparse(url).path.rsplit("/", 1)[-1]
    slug = re.sub(r"-\d+$", "", slug)
    return normalize(slug.replace("-", " "))


def fetch_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as response:
        return response.read().decode("utf-8", "ignore")


def collect_hemnet_urls(search_url: str, max_pages: int = 18) -> list[str]:
    urls: set[str] = set()
    for page in range(1, max_pages + 1):
        url = search_url if page == 1 else f"{search_url}&page={page}"
        html = fetch_text(url)
        found = {
            "https://www.hemnet.se" + href
            for href in re.findall(r'href=["\'](/bostad/[^"\']+)["\']', html)
        }
        if not found:
            break
        before = len(urls)
        urls.update(found)
        if len(urls) == before and page > 2:
            break
        time.sleep(0.25)
    return sorted(urls)


def score_listing_to_url(listing: dict, url: str) -> float:
    title = normalize(listing.get("title", ""))
    location = normalize(listing.get("location", "").split(",")[0])
    slug = slug_text(url)
    if not title or not slug:
        return 0.0
    title_tokens = set(title.split())
    slug_tokens = set(slug.split())
    token_score = len(title_tokens & slug_tokens) / max(1, len(title_tokens))
    seq_score = SequenceMatcher(None, title, slug).ratio()
    location_bonus = 0.08 if location and any(tok in slug_tokens for tok in location.split()) else 0.0
    return max(token_score, seq_score) + location_bonus


def load_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def save_manual_overrides(updates: list[dict]) -> None:
    path = SOURCES_DIR / "hemnet_links_manual.json"
    existing = load_json(path, [])
    by_key = {(normalize(row.get("title", "")), normalize(row.get("location", ""))): row for row in existing}
    for update in updates:
        key = (normalize(update.get("title", "")), normalize(update.get("location", "")))
        by_key[key] = {**by_key.get(key, {}), **update}
    rows = sorted(by_key.values(), key=lambda row: (normalize(row.get("location", "")), normalize(row.get("title", ""))))
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    listings_payload = load_json(DATA_DIR / "listings.json", {"items": []})
    listings = [item for item in listings_payload.get("items", []) if item.get("source") == "hemnet"]
    search_payload = load_json(DATA_DIR / "search-parameters.json", {"items": []})
    hemnet_urls = [item.get("url") for item in search_payload.get("items", []) if item.get("source") == "hemnet" and item.get("url")]
    if not hemnet_urls:
        hemnet_urls = [DEFAULT_HEMNET_SEARCH_URL]

    source_urls: list[str] = []
    for search_url in hemnet_urls:
        source_urls.extend(collect_hemnet_urls(search_url))
    source_urls = sorted(set(source_urls))

    updates: list[dict] = []
    used: set[str] = set()
    for listing in listings:
        if listing.get("listing_url"):
            continue
        ranked = sorted(((score_listing_to_url(listing, url), url) for url in source_urls if url not in used), reverse=True)
        if not ranked:
            continue
        score, url = ranked[0]
        if score >= 0.92:
            used.add(url)
            updates.append({
                "title": listing.get("title"),
                "location": listing.get("location"),
                "listing_url": url,
            })

    save_manual_overrides(updates)
    report = {
        "searched_urls": len(source_urls),
        "hemnet_listings": len(listings),
        "new_listing_url_matches": len(updates),
        "updates": updates,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
