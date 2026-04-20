#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
import textwrap
import time
from datetime import datetime, timezone
from pathlib import Path


def osa(script: str, timeout: int = 40):
    res = subprocess.run(["osascript"], input=script, text=True, capture_output=True, timeout=timeout)
    return res.returncode, res.stdout, res.stderr


def chrome_open(url: str) -> None:
    script = textwrap.dedent(
        f'''
        tell application "Google Chrome"
          activate
          if (count of windows) = 0 then make new window
          tell window 1
            set URL of active tab to "{url}"
          end tell
        end tell
        '''
    )
    rc, out, err = osa(script, 20)
    if rc != 0:
        raise RuntimeError(err.strip() or out.strip())


def page_down(times: int = 3) -> None:
    script = textwrap.dedent(
        f'''
        tell application "System Events"
          tell process "Google Chrome"
            repeat {times} times
              key code 121
              delay 0.35
            end repeat
          end tell
        end tell
        '''
    )
    rc, out, err = osa(script, 20)
    if rc != 0:
        raise RuntimeError(err.strip() or out.strip())


def dump_ax(max_count: int = 1200) -> str:
    script = textwrap.dedent(
        f'''
        tell application "System Events"
          tell process "Google Chrome"
            set frontmost to true
            tell group 1 of window 1
              set uiElems to entire contents
              set outText to ""
              set maxCount to {max_count}
              if (count of uiElems) < maxCount then set maxCount to (count of uiElems)
              repeat with i from 1 to maxCount
                set e to item i of uiElems
                try
                  set r to role of e as text
                  set n to ""
                  try
                    set n to name of e as text
                  end try
                  if r is "AXStaticText" or r is "AXHeading" or r is "AXButton" or r is "AXLink" then
                    set outText to outText & i & ": " & r & " | " & n & linefeed
                  end if
                end try
              end repeat
              return outText
            end tell
          end tell
        end tell
        '''
    )
    rc, out, err = osa(script, 60)
    if rc != 0:
        raise RuntimeError(err.strip() or out.strip())
    return out


def parse_entries(text: str) -> list[dict]:
    entries: list[dict] = []
    current: dict | None = None
    for raw in text.splitlines():
        parts = [part.strip() for part in raw.split("|")]
        if len(parts) < 2:
            continue
        role = parts[0]
        value = parts[1]
        if role.endswith("AXHeading"):
            heading = value
            skip = {
                "Bostäder till salu i Upplands-Bro kommun",
                "Bostäder till salu i Hallstavik",
                "Nyproduktionsprojekt",
                "Till salu",
                "Slutpris",
                "Upplands-Bro kommun",
                "Hallstavik",
            }
            if heading in skip:
                continue
            if current and (current.get("price") or current.get("project") or current.get("property_type")):
                entries.append(current)
            current = {"title": heading}
            continue
        if current is None:
            continue
        txt = value
        if not txt or txt == "missing value" or txt == current.get("title"):
            continue
        if " · " in txt and not current.get("property_type"):
            bits = [bit.strip() for bit in txt.split("·")]
            current["property_type"] = bits[0]
            if len(bits) > 1:
                current["location"] = ", ".join(bits[1:])
        elif re.search(r"\d[\d\s\u00a0]*kr$", txt) or "–" in txt and "kr" in txt:
            current.setdefault("price", txt)
        elif re.match(r"\d+[\.,]?\d*\s*m²", txt) or txt.endswith("m² tomt"):
            current.setdefault("size", txt)
        elif re.match(r"\d+(?:-\d+)?\s*rum$", txt):
            current.setdefault("rooms", txt)
        elif txt.startswith("vån") or txt.endswith("tomt") or "/mån" in txt:
            current.setdefault("status_text", txt)
        elif txt in {"Balkong", "Hiss", "Uteplats", "Eldstad", "Nyproduktionsprojekt", "Inflyttningsklart", "Under försäljning"}:
            current.setdefault("tags", []).append(txt)
            if txt == "Nyproduktionsprojekt":
                current["project"] = True
        elif txt.endswith("på Booli"):
            current["days_on_booli"] = txt
    if current and (current.get("price") or current.get("project") or current.get("property_type")):
        entries.append(current)
    cleaned = []
    seen = set()
    for entry in entries:
        key = (entry.get("title"), entry.get("location"), entry.get("price"))
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(entry)
    return cleaned


def collect(url: str, out_path: Path, steps: int, delay: float) -> None:
    chrome_open(url)
    time.sleep(8)
    entries: dict[tuple, dict] = {}
    for step in range(steps):
        snap = dump_ax(1400)
        for item in parse_entries(snap):
            key = (item.get("title"), item.get("location"), item.get("price"))
            item["source_url"] = url
            item["source_id"] = re.sub(r"\W+", "-", "|".join(str(part or "") for part in key).lower()).strip("-")[:120]
            entries[key] = {**entries.get(key, {}), **item}
        page_down(4)
        time.sleep(delay)
    payload = {
        "source": "booli",
        "search_url": url,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "items": list(entries.values()),
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect Booli listings via Chrome accessibility tree")
    parser.add_argument("url")
    parser.add_argument("out")
    parser.add_argument("--steps", type=int, default=18)
    parser.add_argument("--delay", type=float, default=1.2)
    args = parser.parse_args()
    collect(args.url, Path(args.out), args.steps, args.delay)
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
