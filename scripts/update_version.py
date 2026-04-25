#!/usr/bin/env python3
"""Update public project version metadata before each deploy/push."""
from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION_PATH = ROOT / "data" / "version.json"


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def main() -> None:
    # This file is committed with the release, so the next commit count is the visible version.
    next_count = int(git("rev-list", "--count", "HEAD")) + 1
    now_utc = datetime.now(timezone.utc)
    local_tz = os.environ.get("PROJECT_TZ", "Europe/Stockholm")

    try:
        from zoneinfo import ZoneInfo

        local_dt = now_utc.astimezone(ZoneInfo(local_tz))
    except Exception:
        local_dt = now_utc
        local_tz = "UTC"

    payload = {
        "version": f"v0.0.{next_count}",
        "release_number": next_count,
        "published_at": local_dt.isoformat(timespec="seconds"),
        "published_at_utc": now_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "timezone": local_tz,
        "source_commit_before_release": git("rev-parse", "--short", "HEAD"),
        "source_branch": git("branch", "--show-current"),
    }
    VERSION_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Updated {VERSION_PATH.relative_to(ROOT)} -> {payload['version']} ({payload['published_at']})")


if __name__ == "__main__":
    main()
