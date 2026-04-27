#!/usr/bin/env python3
"""Collect Booli listings from the page's Next.js/Apollo state via Chrome DevTools.

Booli's public HTML is Cloudflare-protected for plain HTTP clients, while the
loaded page contains normalized Apollo listing objects. This collector launches a
separate Chrome profile with remote debugging, navigates search result pages, and
extracts Listing/Project entities from __NEXT_DATA__.
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import websocket

CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
DEFAULT_URL = "https://www.booli.se/sok/till-salu?areaIds=33&maxPrice=3000000&minRooms=4"


def start_chrome(url: str, port: int, profile: Path) -> subprocess.Popen:
    return subprocess.Popen(
        [
            CHROME,
            f"--user-data-dir={profile}",
            f"--remote-debugging-port={port}",
            "--remote-allow-origins=*",
            "--no-first-run",
            "--no-default-browser-check",
            url,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def json_get(url: str) -> Any:
    return json.loads(urllib.request.urlopen(url, timeout=5).read().decode("utf-8"))


def wait_for_page(port: int) -> dict[str, Any]:
    deadline = time.time() + 30
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            pages = json_get(f"http://127.0.0.1:{port}/json")
            for page in pages:
                if page.get("type") == "page":
                    return page
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        time.sleep(0.5)
    raise RuntimeError(f"Chrome debug page did not become available: {last_error}")


class CDP:
    def __init__(self, ws_url: str):
        self.ws = websocket.create_connection(ws_url, timeout=10)
        self.seq = 0

    def call(self, method: str, params: dict[str, Any] | None = None, timeout: float = 30) -> dict[str, Any]:
        self.seq += 1
        msg_id = self.seq
        self.ws.send(json.dumps({"id": msg_id, "method": method, "params": params or {}}))
        deadline = time.time() + timeout
        while time.time() < deadline:
            msg = json.loads(self.ws.recv())
            if msg.get("id") == msg_id:
                if "error" in msg:
                    raise RuntimeError(msg["error"])
                return msg.get("result", {})
        raise TimeoutError(method)

    def close(self) -> None:
        self.ws.close()


def with_page(url: str, page_no: int) -> str:
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    if page_no <= 1:
        qs.pop("page", None)
    else:
        qs["page"] = [str(page_no)]
    query = urllib.parse.urlencode(qs, doseq=True)
    return urllib.parse.urlunparse(parsed._replace(query=query))


def extract_next_data(html: str) -> dict[str, Any] | None:
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if not m:
        return None
    return json.loads(m.group(1))


def data_point_values(entity: dict[str, Any]) -> list[str]:
    attrs = entity.get("displayAttributes") or {}
    out = []
    for point in attrs.get("dataPoints") or []:
        value = (point.get("value") or {}).get("plainText")
        if value:
            out.append(value)
    return out


def municipality(entity: dict[str, Any]) -> str | None:
    return (((entity.get("location") or {}).get("region") or {}).get("municipalityName"))


def listing_from_entity(entity: dict[str, Any], search_url: str) -> dict[str, Any]:
    datapoints = data_point_values(entity)
    rooms = next((v for v in datapoints if "rum" in v), None)
    size = next((v for v in datapoints if "m²" in v and "tomt" not in v), None)
    tags = []
    if entity.get("isNewConstruction"):
        tags.append("Nyproduktionsprojekt")
    location = ", ".join(part for part in [entity.get("descriptiveAreaName"), municipality(entity)] if part)
    rel_url = entity.get("url") or ""
    listing_url = rel_url if rel_url.startswith("http") else f"https://www.booli.se{rel_url}"
    return {
        "source": "booli",
        "source_id": str(entity.get("booliId") or entity.get("id")),
        "source_url": search_url,
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


def project_from_entity(entity: dict[str, Any], search_url: str) -> dict[str, Any]:
    datapoints = data_point_values(entity)
    rooms = next((v for v in datapoints if "rum" in v), None)
    size = next((v for v in datapoints if "m²" in v), None)
    rel_url = entity.get("booliUrl") or ""
    listing_url = rel_url if rel_url.startswith("http") else f"https://www.booli.se{rel_url}"
    return {
        "source": "booli",
        "source_id": f"project:{entity.get('booliId') or entity.get('id')}",
        "source_url": search_url,
        "listing_url": listing_url,
        "title": entity.get("name"),
        "location": entity.get("subtitle"),
        "price": entity.get("listPriceRange"),
        "rooms": rooms,
        "size": size,
        "property_type": "Nyproduktionsprojekt",
        "tags": ["Nyproduktionsprojekt", entity.get("phase") or ""],
        "lat": entity.get("latitude"),
        "lon": entity.get("longitude"),
    }


def extract_items(next_data: dict[str, Any], search_url: str) -> list[dict[str, Any]]:
    state = (((next_data.get("props") or {}).get("pageProps") or {}).get("__APOLLO_STATE__") or {})
    items: list[dict[str, Any]] = []
    for key, entity in state.items():
        if not isinstance(entity, dict):
            continue
        if key.startswith("Listing:") or entity.get("__typename") == "Listing":
            items.append(listing_from_entity(entity, search_url))
        elif key.startswith("Project:") or entity.get("__typename") == "Project":
            items.append(project_from_entity(entity, search_url))
    return [item for item in items if item.get("title")]


def collect(url: str, out_path: Path, max_pages: int, port: int) -> None:
    profile = Path(tempfile.mkdtemp(prefix="booli-cdp-"))
    proc = start_chrome(with_page(url, 1), port, profile)
    all_items: dict[str, dict[str, Any]] = {}
    try:
        page = wait_for_page(port)
        cdp = CDP(page["webSocketDebuggerUrl"])
        cdp.call("Runtime.enable")
        cdp.call("Page.enable")
        for page_no in range(1, max_pages + 1):
            page_url = with_page(url, page_no)
            cdp.call("Page.navigate", {"url": page_url})
            time.sleep(6 if page_no == 1 else 3)
            result = cdp.call("Runtime.evaluate", {"expression": "document.documentElement.outerHTML", "returnByValue": True})
            html = ((result.get("result") or {}).get("value") or "")
            next_data = extract_next_data(html)
            if not next_data:
                if page_no == 1:
                    raise RuntimeError("Booli page did not expose __NEXT_DATA__")
                break
            page_items = extract_items(next_data, url)
            before = len(all_items)
            for item in page_items:
                key = str(item.get("source_id") or item.get("listing_url") or item.get("title"))
                all_items[key] = {**all_items.get(key, {}), **item}
            print(f"page {page_no}: {len(page_items)} items ({len(all_items)} total)")
            if page_no > 1 and len(all_items) == before:
                break
        cdp.close()
    finally:
        proc.terminate()
        shutil.rmtree(profile, ignore_errors=True)

    payload = {
        "source": "booli",
        "search_url": url,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "items": list(all_items.values()),
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {out_path} with {len(all_items)} items")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("url", nargs="?", default=DEFAULT_URL)
    parser.add_argument("out", nargs="?", default="sources/booli_upplands_bro.json")
    parser.add_argument("--max-pages", type=int, default=8)
    parser.add_argument("--port", type=int, default=9233)
    args = parser.parse_args()
    collect(args.url, Path(args.out), args.max_pages, args.port)


if __name__ == "__main__":
    main()
