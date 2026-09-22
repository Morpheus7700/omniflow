#!/usr/bin/env python3
"""Re-download the vendored latin-subset fonts into frontend/src/app/fonts.

Run this ONLY when a family is added or changed — a routine refresh churns binary files for no
benefit. The files are committed on purpose: see that directory's README. `next/font/google`
fetches at BUILD time, which made every image build depend on reaching fonts.gstatic.com and failed
a CI boot job when that fetch did not succeed, minutes after the same `npm run build` had passed on
the runner.

    python scripts/fetch-fonts.py
"""

import os
import re
import sys
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.normpath(os.path.join(HERE, os.pardir, "frontend", "src", "app", "fonts"))

# Google serves different files by User-Agent; a modern one gets woff2.
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Newsreader and Public Sans are VARIABLE fonts: Google returns byte-identical files for weight 400
# and 500, so one file per style covers the range (declared as `weight: "400 500"` in layout.tsx).
# IBM Plex Mono ships static weights and needs one file each.
JOBS = [
    (
        "Newsreader:ital,wght@0,400;0,500;1,400;1,500",
        [
            ("normal", "400", "newsreader-variable.woff2"),
            ("italic", "400", "newsreader-variable-italic.woff2"),
        ],
    ),
    ("Public+Sans:wght@400;500", [("normal", "400", "public-sans-variable.woff2")]),
    (
        "IBM+Plex+Mono:wght@400;500",
        [
            ("normal", "400", "ibm-plex-mono-400.woff2"),
            ("normal", "500", "ibm-plex-mono-500.woff2"),
        ],
    ),
]


def fetch(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as r:  # noqa: S310 - fixed https host
        return r.read()


def main() -> int:
    os.makedirs(OUT, exist_ok=True)
    failed = False
    for query, faces in JOBS:
        css = fetch(f"https://fonts.googleapis.com/css2?family={query}&display=swap").decode("utf-8")
        # Only the `latin` subset: the UI is English, and shipping every subset would multiply the
        # bytes for glyphs nothing renders.
        blocks = re.findall(r"/\*\s*latin\s*\*/\s*@font-face\s*\{(.*?)\}", css, re.S)
        for style, weight, name in faces:
            for block in blocks:
                st = re.search(r"font-style:\s*(\w+)", block)
                wt = re.search(r"font-weight:\s*(\d+)", block)
                if not st or not wt or st.group(1) != style or wt.group(1) != weight:
                    continue
                url = re.search(r"url\((https://[^)]+\.woff2)\)", block).group(1)
                data = fetch(url)
                with open(os.path.join(OUT, name), "wb") as fh:
                    fh.write(data)
                print(f"ok   {name:34s} {len(data):>7,} bytes")
                break
            else:
                print(f"MISS {name} ({style} {weight}) — the family's CSS no longer offers that face")
                failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
