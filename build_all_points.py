#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import sqlite3
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SITE_DIR = ROOT / "sl-map-site"
DB_PATH = ROOT / "sl-db" / "sl_transport.sqlite"
HEMNET_PATH = ROOT / "hemnet-scrape" / "listings_final.json"
HEMNET_SUPPLEMENTAL_PATH = SITE_DIR / "sources" / "listings_supplemental.json"
DATA_DIR = SITE_DIR / "data"

STOPS_OUT = DATA_DIR / "sl-stop-points.json"
HEMNET_OUT = DATA_DIR / "hemnet-listings.json"


RENOVATED_RE = re.compile(r"\b(renoverad|renoverat|renoverade|renovering|nyrenoverad|nyrenoverat|totalrenoverad|totalrenoverat|topprenoverad|smakfullt renoverad|stambytt|helrenoverad|helrenoverat)\b", re.IGNORECASE)
NEW_RE = re.compile(r"\b(nybyggnadsprojekt|nyproduktion|nybyggd|nybyggt|svanenmärkt|nytt grannskap)\b", re.IGNORECASE)


def normalize(text: str) -> str:
    text = (text or "").lower().strip()
    text = text.replace("stockholms kommun", "")
    text = text.replace("kommun", "")
    text = re.sub(r"\([^)]*\)", " ", text)
    text = text.replace("/", " ")
    text = re.sub(r"\s*-\s*", " ", text)
    for word in ["centrala", "norra", "sodra", "vastra", "ostra"]:
        text = re.sub(rf"\b{word}\b", " ", text)
    text = " ".join(text.split())
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text


@dataclass(frozen=True)
class Place:
    name: str
    lat: float
    lon: float
    kind: str
    norm: str


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


def classify_listing(listing: dict) -> str:
    tags = " ".join(listing.get("tags") or [])
    text = " ".join(part for part in [listing.get("title", ""), listing.get("blurb", ""), tags] if part)
    if NEW_RE.search(text):
        return "new"
    if RENOVATED_RE.search(text):
        return "renovated"
    return "old"


def match_location(location: str, places: list[Place]) -> dict | None:
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


HTML = """<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"UTF-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <title>SL all stop points</title>
  <link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\" />
  <style>
    :root {
      --bg: #0b1020;
      --panel: #11182d;
      --card: #161f39;
      --line: #283556;
      --muted: #a9b5d1;
      --text: #e8edf7;
      --stop: #ff6b6b;
      --stop-fill: #ff9b9b;
      --hemnet-old: #111111;
      --hemnet-old-ring: rgba(255,255,255,0.18);
      --hemnet-renovated: #22c55e;
      --hemnet-renovated-ring: rgba(34,197,94,0.24);
      --hemnet-new: #ef4444;
      --hemnet-new-ring: rgba(239,68,68,0.24);
    }
    * { box-sizing: border-box; }
    body { margin: 0; font: 14px/1.45 -apple-system, BlinkMacSystemFont, \"Segoe UI\", sans-serif; background: var(--bg); color: var(--text); }
    .wrap { display: grid; grid-template-columns: 360px 1fr; height: 100vh; }
    .side { padding: 16px; overflow: auto; background: var(--panel); border-right: 1px solid var(--line); }
    #map { height: 100vh; width: 100%; }
    .card { background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 12px; margin-bottom: 12px; }
    .stats { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
    .stat { background: #0f1730; border-radius: 10px; padding: 10px; }
    .stat b { display: block; font-size: 20px; }
    input, select, button { width: 100%; border-radius: 10px; border: 1px solid #314266; background: #0d1430; color: var(--text); padding: 10px 12px; }
    input::placeholder { color: #7d8bb0; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
    .subtle { color: var(--muted); }
    h1, h2 { margin: 0 0 10px; }
    .legend { display: grid; gap: 8px; }
    .legend-item { display: flex; align-items: center; gap: 8px; }
    .swatch { width: 12px; height: 12px; border-radius: 999px; display: inline-block; }
    .swatch.stop { background: var(--stop); box-shadow: 0 0 0 3px rgba(255,107,107,0.18); }
    .swatch.hemnet { background: var(--hemnet-renovated); box-shadow: 0 0 0 3px var(--hemnet-renovated-ring); }
    .pill { display: inline-flex; align-items: center; gap: 6px; padding: 5px 8px; border-radius: 999px; border: 1px solid var(--line); background: #0f1730; color: var(--muted); margin: 6px 6px 0 0; }
    .small { font-size: 12px; }
    a { color: #9bc3ff; }
    @media (max-width: 960px) { .wrap { grid-template-columns: 1fr; grid-template-rows: auto 60vh; } #map { height: 60vh; } }
  </style>
</head>
<body>
  <div class=\"wrap\">
    <div class=\"side\">
      <h1>SL stop points + Hemnet</h1>
      <p class=\"subtle\">Point map from the local SQLite DB. Stops are red dots. Hemnet listings are color-coded, red for new construction, black for older stock, green for renovated.</p>

      <div class=\"card stats\" id=\"stats\"></div>

      <div class=\"card\">
        <h2>Find</h2>
        <div style=\"margin-bottom:8px;\"><input id=\"search\" placeholder=\"Search stop, area, listing, or location\" /></div>
        <div class=\"row\">
          <select id=\"type\">
            <option value=\"ALL\">All stop types</option>
            <option value=\"BUSSTOP\">Bus stops</option>
            <option value=\"PLATFORM\">Platforms</option>
            <option value=\"PIER\">Piers</option>
          </select>
          <select id=\"hemnetFilter\">
            <option value=\"ALL\">Hemnet + SL</option>
            <option value=\"STOPS_ONLY\">SL only</option>
            <option value=\"HEMNET_ONLY\">Hemnet only</option>
          </select>
        </div>
        <div style=\"margin-top:8px;\"><button id=\"reset\">Reset view</button></div>
      </div>

      <div class=\"card legend\">
        <div class=\"legend-item\"><span class=\"swatch stop\"></span><span>SL stop points (red)</span></div>
        <div class=\"legend-item\"><span class=\"swatch hemnet\" style=\"background:var(--hemnet-new); box-shadow:0 0 0 3px var(--hemnet-new-ring);\"></span><span>Hemnet new construction (red)</span></div>
        <div class=\"legend-item\"><span class=\"swatch hemnet\" style=\"background:var(--hemnet-old); box-shadow:0 0 0 3px var(--hemnet-old-ring);\"></span><span>Hemnet older stock (black)</span></div>
        <div class=\"legend-item\"><span class=\"swatch hemnet\" style=\"background:var(--hemnet-renovated); box-shadow:0 0 0 3px var(--hemnet-renovated-ring);\"></span><span>Hemnet renovated (green)</span></div>
        <div class=\"small subtle\">Basemap toggle is in the top-right corner. Satellite uses Esri World Imagery.</div>
      </div>

      <div class=\"card subtle small\" id=\"matchNote\"></div>
    </div>
    <div id=\"map\"></div>
  </div>

  <script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\"></script>
  <script>
    const STOP_STYLE = {
      BUSSTOP: { radius: 2.4, color: '#ff6b6b', fillColor: '#ff9b9b', fillOpacity: 0.55, weight: 0.45 },
      PLATFORM: { radius: 2.8, color: '#ff8787', fillColor: '#ffc2c2', fillOpacity: 0.65, weight: 0.55 },
      PIER: { radius: 3.2, color: '#ffa8a8', fillColor: '#ffe3e3', fillOpacity: 0.8, weight: 0.7 },
      DEFAULT: { radius: 2.6, color: '#ff6b6b', fillColor: '#ffb3b3', fillOpacity: 0.55, weight: 0.45 },
    };

    const HEMNET_STYLE = {
      new: { bg: '#ef4444', border: '#7f1d1d', ring: 'rgba(239,68,68,0.24)' },
      old: { bg: '#111111', border: '#d1d5db', ring: 'rgba(255,255,255,0.18)' },
      renovated: { bg: '#22c55e', border: '#14532d', ring: 'rgba(34,197,94,0.24)' },
    };

    function hemnetIcon(kind) {
      const style = HEMNET_STYLE[kind] || HEMNET_STYLE.old;
      return L.divIcon({
        className: '',
        html: `<div style="width:16px;height:16px;border-radius:999px;background:${style.bg};border:2px solid ${style.border};color:${style.border};font-size:10px;display:flex;align-items:center;justify-content:center;box-shadow:0 0 0 3px ${style.ring}">⌂</div>`,
        iconSize: [16, 16],
        iconAnchor: [8, 8],
        popupAnchor: [0, -8],
      });
    }

    const map = L.map('map', { preferCanvas: true, zoomControl: true }).setView([59.3293, 18.0686], 10);
    const osm = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { attribution: '&copy; OpenStreetMap contributors' });
    const esri = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', { attribution: 'Tiles &copy; Esri' });
    osm.addTo(map);
    L.control.layers({ 'Street map': osm, 'Satellite': esri }, null, { position: 'topright' }).addTo(map);

    const stopLayer = L.layerGroup().addTo(map);
    const hemnetLayer = L.layerGroup().addTo(map);

    const stats = document.getElementById('stats');
    const searchEl = document.getElementById('search');
    const typeEl = document.getElementById('type');
    const hemnetFilterEl = document.getElementById('hemnetFilter');
    const matchNoteEl = document.getElementById('matchNote');

    stats.innerHTML = `
      <div class="stat"><span class="subtle">Stops</span><b id="countStops"></b></div>
      <div class="stat"><span class="subtle">Visible stops</span><b id="countVisibleStops"></b></div>
      <div class="stat"><span class="subtle">Hemnet matched</span><b id="countHemnet"></b></div>
      <div class="stat"><span class="subtle">Visible Hemnet</span><b id="countVisibleHemnet"></b></div>
    `;

    const state = { stops: [], hemnet: [] };

    function popupHtmlStop(point) {
      return `<b>${point.name}</b><br>${point.stop_area_name ? `Area: ${point.stop_area_name}<br>` : ''}Type: ${point.type}<br>${point.designation ? `Designation: ${point.designation}<br>` : ''}ID: ${point.id}`;
    }

    function popupHtmlHemnet(item) {
      const location = item.location ? `<br>Location: ${item.location}` : '';
      const price = item.price ? `<br>Price: ${item.price}` : '';
      const size = item.size ? `<br>Size: ${item.size}` : '';
      const rooms = item.rooms ? `<br>Rooms: ${item.rooms}` : '';
      const category = item.category ? `<br>Status: ${item.category_label}` : '';
      const source = item.source ? `<br>Source: ${item.source}` : '';
      const matched = item.matched_name ? `<br>Matched via: ${item.matched_name} (${item.matched_kind}, ${item.match_score})` : '';
      return `<b>${item.title || 'Hemnet listing'}</b>${location}${price}${size}${rooms}${category}${source}${matched}`;
    }

    function render() {
      const query = searchEl.value.trim().toLowerCase();
      const type = typeEl.value;
      const hemnetMode = hemnetFilterEl.value;

      const visibleStops = state.stops.filter((point) => {
        if (type !== 'ALL' && point.type !== type) return false;
        if (!query) return hemnetMode !== 'HEMNET_ONLY';
        const haystack = `${point.name || ''} ${point.stop_area_name || ''} ${point.designation || ''} ${point.id || ''}`.toLowerCase();
        return hemnetMode !== 'HEMNET_ONLY' && haystack.includes(query);
      });

      const visibleHemnet = state.hemnet.filter((item) => {
        if (hemnetMode === 'STOPS_ONLY') return false;
        if (!query) return true;
        const haystack = `${item.title || ''} ${item.location || ''} ${item.matched_name || ''}`.toLowerCase();
        return haystack.includes(query);
      });

      stopLayer.clearLayers();
      hemnetLayer.clearLayers();

      visibleStops.forEach((point) => {
        const style = STOP_STYLE[point.type] || STOP_STYLE.DEFAULT;
        L.circleMarker([point.lat, point.lon], style)
          .bindPopup(popupHtmlStop(point))
          .addTo(stopLayer);
      });

      visibleHemnet.forEach((item) => {
        L.marker([item.lat, item.lon], { icon: hemnetIcon(item.category) })
          .bindPopup(popupHtmlHemnet(item))
          .addTo(hemnetLayer);
      });

      document.getElementById('countVisibleStops').textContent = visibleStops.length.toLocaleString();
      document.getElementById('countVisibleHemnet').textContent = visibleHemnet.length.toLocaleString();
    }

    function resetView() {
      searchEl.value = '';
      typeEl.value = 'ALL';
      hemnetFilterEl.value = 'ALL';
      map.setView([59.3293, 18.0686], 10);
      render();
    }

    document.getElementById('reset').addEventListener('click', resetView);
    searchEl.addEventListener('input', render);
    typeEl.addEventListener('change', render);
    hemnetFilterEl.addEventListener('change', render);

    Promise.all([
      fetch('./data/sl-stop-points.json').then((r) => r.json()),
      fetch('./data/hemnet-listings.json').then((r) => r.json()),
    ]).then(([stops, hemnet]) => {
      state.stops = stops;
      state.hemnet = hemnet.items;

      document.getElementById('countStops').textContent = stops.length.toLocaleString();
      document.getElementById('countHemnet').textContent = hemnet.items.length.toLocaleString();
      matchNoteEl.innerHTML = `Matched <b>${hemnet.items.length}</b> of <b>${hemnet.source_count}</b> Hemnet listings from local data using local SL place names only. Unmatched listings: <b>${hemnet.unmatched_count}</b>.`;

      render();
    }).catch((error) => {
      matchNoteEl.textContent = `Failed to load local JSON data: ${error.message}`;
    });
  </script>
</body>
</html>
"""


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    stop_rows = cur.execute(
        """
        SELECT id, name, designation, type, lat, lon, stop_area_name
        FROM stop_points
        WHERE lat IS NOT NULL AND lon IS NOT NULL
        ORDER BY id
        """
    ).fetchall()

    stops = [
        {
            "id": row["id"],
            "name": row["name"],
            "designation": row["designation"],
            "type": row["type"],
            "lat": row["lat"],
            "lon": row["lon"],
            "stop_area_name": row["stop_area_name"],
        }
        for row in stop_rows
    ]

    places: list[Place] = []
    for row in cur.execute("SELECT name, lat, lon, type FROM stop_areas WHERE lat IS NOT NULL AND lon IS NOT NULL"):
        places.append(Place(name=row[0], lat=row[1], lon=row[2], kind=f"stop_area:{row[3]}", norm=normalize(row[0])))
    for row in cur.execute("SELECT name, lat, lon FROM sites WHERE lat IS NOT NULL AND lon IS NOT NULL"):
        places.append(Place(name=row[0], lat=row[1], lon=row[2], kind="site", norm=normalize(row[0])))

    listings = json.loads(HEMNET_PATH.read_text())
    if HEMNET_SUPPLEMENTAL_PATH.exists():
        seen = {
            (item.get("title", "").strip(), item.get("location", "").strip(), item.get("price", "").strip())
            for item in listings
        }
        for item in json.loads(HEMNET_SUPPLEMENTAL_PATH.read_text()):
            key = (item.get("title", "").strip(), item.get("location", "").strip(), item.get("price", "").strip())
            if key not in seen:
                listings.append(item)
                seen.add(key)
    hemnet_items = []
    unmatched = 0
    for listing in listings:
        match = match_location(listing.get("location", ""), places)
        if not match:
            unmatched += 1
            continue
        category = classify_listing(listing)
        hemnet_items.append(
            {
                "title": listing.get("title"),
                "location": listing.get("location"),
                "price": listing.get("price"),
                "size": listing.get("size"),
                "rooms": listing.get("rooms"),
                "category": category,
                "category_label": {
                    "new": "New construction",
                    "old": "Older stock",
                    "renovated": "Renovated",
                }[category],
                "source": listing.get("source", "local_scrape"),
                **match,
            }
        )

    STOPS_OUT.write_text(json.dumps(stops, ensure_ascii=False, separators=(",", ":")))
    HEMNET_OUT.write_text(
        json.dumps(
            {
                "source": str(HEMNET_PATH.relative_to(ROOT)),
                "source_count": len(listings),
                "unmatched_count": unmatched,
                "items": hemnet_items,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )
    (SITE_DIR / "all-points.html").write_text(HTML)

    print(f"Wrote {STOPS_OUT}")
    print(f"Wrote {HEMNET_OUT}")
    print(f"Wrote {SITE_DIR / 'all-points.html'}")
    print(f"Hemnet matches: {len(hemnet_items)}/{len(listings)}")


if __name__ == "__main__":
    main()
