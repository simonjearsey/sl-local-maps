#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
SITE_DIR = ROOT / "sl-map-site"
TRANSPORT_DB_PATH = ROOT / "sl-db" / "sl_transport.sqlite"
HEMNET_PATH = ROOT / "hemnet-scrape" / "listings_final.json"
SITE_SOURCES_DIR = SITE_DIR / "sources"
DATA_DIR = SITE_DIR / "data"
LISTINGS_DB_PATH = DATA_DIR / "listings.sqlite"
LISTINGS_JSON_OUT = DATA_DIR / "listings.json"

DEFAULT_HEMNET_SEARCH_URL = "https://www.hemnet.se/bostader?location_ids%5B%5D=17744&price_max=3000000&rooms_min=4"

RENOVATED_RE = re.compile(r"\b(renoverad|renoverat|renoverade|renovering|nyrenoverad|nyrenoverat|totalrenoverad|totalrenoverat|topprenoverad|smakfullt renoverad|stambytt|helrenoverad|helrenoverat)\b", re.IGNORECASE)
NEW_RE = re.compile(r"\b(nybyggnadsprojekt|nyproduktion|nybyggd|nybyggt|svanenmärkt|nytt grannskap)\b", re.IGNORECASE)


@dataclass(frozen=True)
class Place:
    name: str
    lat: float
    lon: float
    kind: str
    norm: str


def normalize(text: str) -> str:
    text = (text or "").lower().strip()
    text = text.replace("stockholms kommun", "")
    text = text.replace("kommun", "")
    text = re.sub(r"\([^)]*\)", " ", text)
    text = text.replace("/", " ")
    text = re.sub(r"\s*[-–]\s*", " ", text)
    for word in ["centrala", "norra", "sodra", "vastra", "ostra"]:
        text = re.sub(rf"\b{word}\b", " ", text)
    text = " ".join(text.split())
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text


def normalize_title(text: str) -> str:
    return normalize((text or "").replace("|", " "))


def location_candidates(location: str) -> list[str]:
    base = (location or "").split(",")[0].strip()
    parts = [base]
    if " - " in base:
        parts.extend(p.strip() for p in base.split(" - ") if p.strip())
    if "/" in base:
        parts.extend(p.strip() for p in base.split("/") if p.strip())
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        candidate = normalize(part)
        if candidate and candidate not in seen:
            seen.add(candidate)
            out.append(candidate)
    return out


def classify_listing(record: dict[str, Any]) -> str:
    tags = " ".join(record.get("tags") or [])
    text = " ".join(
        str(part) for part in [record.get("title", ""), record.get("blurb", ""), record.get("property_type", ""), tags] if part
    )
    if NEW_RE.search(text):
        return "new"
    if RENOVATED_RE.search(text):
        return "renovated"
    return "old"


def category_label(category: str) -> str:
    return {
        "new": "New construction",
        "old": "Older stock",
        "renovated": "Renovated",
    }.get(category, category.title())


def parse_market_age_days(record: dict[str, Any]) -> int | None:
    """Parse explicit source-provided listing age; do not infer from local scrape date."""
    text = " ".join(
        str(part)
        for part in [record.get("days_on_market"), record.get("days_on_booli"), record.get("age_text")]
        if part
    ).lower()
    if not text:
        return None
    if re.search(r"\b(idag|today)\b", text):
        return 0
    m = re.search(r"(\d+)\s*(?:dag|day)", text)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s*(?:vecka|veckor|week|weeks)", text)
    if m:
        return int(m.group(1)) * 7
    return None


def market_age_text(record: dict[str, Any]) -> str | None:
    return record.get("days_on_market") or record.get("days_on_booli") or record.get("age_text")


def load_places() -> list[Place]:
    conn = sqlite3.connect(TRANSPORT_DB_PATH)
    cur = conn.cursor()
    places: list[Place] = []
    for row in cur.execute("SELECT name, lat, lon, type FROM stop_areas WHERE lat IS NOT NULL AND lon IS NOT NULL"):
        places.append(Place(name=row[0], lat=row[1], lon=row[2], kind=f"stop_area:{row[3]}", norm=normalize(row[0])))
    for row in cur.execute("SELECT name, lat, lon FROM sites WHERE lat IS NOT NULL AND lon IS NOT NULL"):
        places.append(Place(name=row[0], lat=row[1], lon=row[2], kind="site", norm=normalize(row[0])))
    conn.close()
    return places


def match_location(location: str, places: list[Place]) -> dict[str, Any] | None:
    if not location:
        return None
    common_noise = {"centrum", "kommun", "strandpark"}
    best: tuple[float, Place, str] | None = None
    for candidate in location_candidates(location):
        candidate_tokens = set(candidate.split()) - common_noise
        for place in places:
            score = 0.0
            if candidate == place.norm:
                score = 1.0
            else:
                place_tokens = set(place.norm.split()) - common_noise
                if place.norm.startswith(candidate) or candidate.startswith(place.norm):
                    score = max(score, 0.94)
                if candidate_tokens and candidate_tokens <= place_tokens:
                    score = max(score, 0.93)
                if place_tokens and place_tokens <= candidate_tokens:
                    score = max(score, 0.90)
                score = max(score, SequenceMatcher(None, candidate, place.norm).ratio() * 0.88)
            if best is None or score > best[0]:
                best = (score, place, candidate)
    if best is None or best[0] < 0.88:
        return None
    score, place, candidate = best
    return {
        "lat": place.lat,
        "lon": place.lon,
        "matched_name": place.name,
        "matched_kind": place.kind,
        "match_score": round(score, 3),
        "match_query": candidate,
    }


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text())


def load_manual_link_overrides() -> dict[tuple[str, str], dict[str, Any]]:
    path = SITE_SOURCES_DIR / "hemnet_links_manual.json"
    rows = load_json(path, [])
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (normalize_title(row.get("title", "")), normalize(row.get("location", "")))
        out[key] = row
    return out


def infer_source_id(source: str, record: dict[str, Any]) -> str:
    if record.get("source_id"):
        return str(record["source_id"])
    if record.get("listing_url"):
        m = re.search(r"-(\d+)(?:[#/?]|$)", str(record["listing_url"]))
        if m:
            return m.group(1)
    payload = "|".join(
        [source, normalize_title(record.get("title", "")), normalize(record.get("location", "")), str(record.get("price", ""))]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def dedupe_key(source: str, source_id: str, title: str, location: str, price: str) -> str:
    if source_id:
        return f"{source}:{source_id}"
    payload = "|".join([source, normalize_title(title), normalize(location), str(price or "")])
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def listing_identity_key(title: str, location: str, price: str = "") -> str:
    """Cross-portal identity key for deduplicating Hemnet/Booli rows.

    Source ids are portal-specific, so they cannot be used for the public listing set.
    Titles are normally street addresses or project names; combining normalized title
    and locality dedupes the same object across Hemnet and Booli while keeping
    distinct objects in different municipalities separate.
    """
    title_norm = normalize_title(title)
    location_norm = normalize(location).split(",")[0].strip()
    if title_norm and location_norm:
        return hashlib.sha1(f"listing:{title_norm}|{location_norm}".encode("utf-8")).hexdigest()
    return dedupe_key("listing", "", title, location, price)


def merge_row(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    sources = set(merged.get("sources") or ([merged.get("source")] if merged.get("source") else []))
    sources.update(incoming.get("sources") or ([incoming.get("source")] if incoming.get("source") else []))
    if sources:
        merged["sources"] = sorted(sources)
        merged["source"] = "+".join(sorted(sources))

    source_urls = {item for item in (merged.get("source_urls") or []) if item}
    if merged.get("source_url"):
        source_urls.add(merged["source_url"])
    if incoming.get("source_url"):
        source_urls.add(incoming["source_url"])
    source_urls.update(item for item in (incoming.get("source_urls") or []) if item)
    if source_urls:
        merged["source_urls"] = sorted(source_urls)

    listing_urls = {item for item in (merged.get("listing_urls") or []) if item}
    if merged.get("listing_url"):
        listing_urls.add(merged["listing_url"])
    if incoming.get("listing_url"):
        listing_urls.add(incoming["listing_url"])
    listing_urls.update(item for item in (incoming.get("listing_urls") or []) if item)
    if listing_urls:
        merged["listing_urls"] = sorted(listing_urls)
    aggregated_keys = {"source", "sources", "source_url", "source_urls", "listing_url", "listing_urls"}
    for key, value in incoming.items():
        if key in aggregated_keys:
            continue
        if value not in (None, "", [], {}):
            merged[key] = value
    if source_urls and not merged.get("source_url"):
        merged["source_url"] = sorted(source_urls)[0]
    if listing_urls and not merged.get("listing_url"):
        merged["listing_url"] = sorted(listing_urls)[0]
    return merged


def load_raw_sources() -> list[dict[str, Any]]:
    raw_rows: list[dict[str, Any]] = []

    hemnet_source_files = sorted(path for path in SITE_SOURCES_DIR.glob("hemnet_*.json") if path.name != "hemnet_links_manual.json")
    if hemnet_source_files:
        for path in hemnet_source_files:
            payload = load_json(path, {})
            items = payload.get("items", []) if isinstance(payload, dict) else payload
            search_url = payload.get("search_url") if isinstance(payload, dict) else None
            for item in items:
                raw_rows.append({**item, "source": "hemnet", "source_url": item.get("source_url") or search_url})
    else:
        for item in load_json(HEMNET_PATH, []):
            raw_rows.append({**item, "source": "hemnet", "source_url": DEFAULT_HEMNET_SEARCH_URL})

    # The supplemental file predates the coordinate-based Hemnet/Booli captures.
    # Only use it as a fallback when no fresh portal source files exist.
    if not hemnet_source_files and not (SITE_SOURCES_DIR / "booli_sl_area.json").exists():
        for item in load_json(SITE_SOURCES_DIR / "listings_supplemental.json", []):
            raw_rows.append({**item, "source": item.get("source_portal", "hemnet") or "hemnet"})

    if (SITE_SOURCES_DIR / "booli_graphql_area.json").exists():
        booli_source_files = [SITE_SOURCES_DIR / "booli_graphql_area.json"]
    elif (SITE_SOURCES_DIR / "booli_sl_area.json").exists():
        booli_source_files = [SITE_SOURCES_DIR / "booli_sl_area.json"]
    else:
        booli_source_files = sorted(SITE_SOURCES_DIR.glob("booli_*.json"))
    for path in booli_source_files:
        payload = load_json(path, {})
        items = payload.get("items", []) if isinstance(payload, dict) else payload
        search_url = payload.get("search_url") if isinstance(payload, dict) else None
        for item in items:
            raw_rows.append({**item, "source": "booli", "source_url": item.get("source_url") or search_url})

    return raw_rows


def canonical_rows() -> list[dict[str, Any]]:
    places = load_places()
    overrides = load_manual_link_overrides()
    now = datetime.now(timezone.utc).isoformat()
    rows_by_key: dict[str, dict[str, Any]] = {}

    for raw in load_raw_sources():
        source = raw.get("source", "unknown")
        title = (raw.get("title") or "").strip()
        location = (raw.get("location") or "").strip()
        key_override = overrides.get((normalize_title(title), normalize(location)), {})
        record = merge_row(raw, key_override)
        source_id = infer_source_id(source, record)
        category = classify_listing(record)
        match = None
        if record.get("lat") is not None and record.get("lon") is not None:
            match = {
                "lat": record.get("lat"),
                "lon": record.get("lon"),
                "matched_name": record.get("location") or record.get("title"),
                "matched_kind": "direct",
                "match_score": 1.0,
                "match_query": normalize(record.get("location") or record.get("title") or ""),
            }
        else:
            match = match_location(location, places)

        row = {
            "source": source,
            "sources": [source],
            "source_id": source_id,
            "source_url": record.get("source_url"),
            "source_urls": [record.get("source_url")] if record.get("source_url") else [],
            "listing_url": record.get("listing_url"),
            "listing_urls": [record.get("listing_url")] if record.get("listing_url") else [],
            "title": title,
            "location": location,
            "address": record.get("address") or title.rstrip(" |"),
            "price": record.get("price"),
            "rooms": record.get("rooms"),
            "size": record.get("size"),
            "property_type": record.get("property_type"),
            "tags": record.get("tags") or [],
            "category": category,
            "category_label": category_label(category),
            "status_text": record.get("status_text") or record.get("days_on_booli"),
            "market_age_text": market_age_text(record),
            "market_age_days": parse_market_age_days(record),
            "nearest_sl_stop": record.get("nearest_sl_stop"),
            "nearest_sl_stop_point": record.get("nearest_sl_stop_point"),
            "nearest_sl_stop_type": record.get("nearest_sl_stop_type"),
            "nearest_sl_stop_distance_m": record.get("nearest_sl_stop_distance_m"),
            "raw_json": json.dumps(record, ensure_ascii=False, sort_keys=True),
            "first_seen_at": record.get("first_seen_at") or now,
            "last_seen_at": now,
        }
        if match:
            row.update(match)
        row_key = listing_identity_key(title, location, row.get("price") or "")
        rows_by_key[row_key] = merge_row(rows_by_key.get(row_key, {}), row)

    return list(rows_by_key.values())


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        DROP TABLE IF EXISTS listings;
        CREATE TABLE listings (
            id INTEGER PRIMARY KEY,
            dedupe_key TEXT UNIQUE NOT NULL,
            source TEXT NOT NULL,
            source_id TEXT,
            source_url TEXT,
            listing_url TEXT,
            title TEXT,
            location TEXT,
            address TEXT,
            lat REAL,
            lon REAL,
            matched_name TEXT,
            matched_kind TEXT,
            match_score REAL,
            match_query TEXT,
            price TEXT,
            rooms TEXT,
            size TEXT,
            property_type TEXT,
            tags_json TEXT,
            category TEXT,
            category_label TEXT,
            status_text TEXT,
            market_age_text TEXT,
            market_age_days INTEGER,
            raw_json TEXT NOT NULL,
            first_seen_at TEXT,
            last_seen_at TEXT
        );
        """
    )


def build_sqlite(rows: list[dict[str, Any]]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(LISTINGS_DB_PATH)
    init_db(conn)
    conn.executemany(
        """
        INSERT INTO listings (
            dedupe_key, source, source_id, source_url, listing_url, title, location, address,
            lat, lon, matched_name, matched_kind, match_score, match_query,
            price, rooms, size, property_type, tags_json, category, category_label,
            status_text, market_age_text, market_age_days, raw_json, first_seen_at, last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                dedupe_key(row["source"], row.get("source_id", ""), row.get("title", ""), row.get("location", ""), row.get("price", "")),
                row.get("source"),
                row.get("source_id"),
                row.get("source_url"),
                row.get("listing_url"),
                row.get("title"),
                row.get("location"),
                row.get("address"),
                row.get("lat"),
                row.get("lon"),
                row.get("matched_name"),
                row.get("matched_kind"),
                row.get("match_score"),
                row.get("match_query"),
                row.get("price"),
                row.get("rooms"),
                row.get("size"),
                row.get("property_type"),
                json.dumps(row.get("tags") or [], ensure_ascii=False),
                row.get("category"),
                row.get("category_label"),
                row.get("status_text"),
                row.get("market_age_text"),
                row.get("market_age_days"),
                row.get("raw_json"),
                row.get("first_seen_at"),
                row.get("last_seen_at"),
            )
            for row in rows
        ],
    )
    conn.commit()
    conn.close()


def export_listings_json(rows: list[dict[str, Any]]) -> None:
    export_rows = [row for row in rows if row.get("lat") is not None and row.get("lon") is not None]
    LISTINGS_JSON_OUT.write_text(
        json.dumps(
            {
                "source_count": len(rows),
                "mapped_count": len(export_rows),
                "items": export_rows,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )


def build_listing_artifacts() -> list[dict[str, Any]]:
    rows = canonical_rows()
    build_sqlite(rows)
    export_listings_json(rows)
    return rows
