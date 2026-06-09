#!/usr/bin/env python3
"""
Sync the canonical pincodes.json snapshot to every consumer in the workspace.

Why this exists
---------------
Five copies of pincodes.json live across these repos, and they drifted apart
over time (different entry counts, 1249-3276 pincode deltas between copies,
465 conflicting zone assignments in the stalest copy). That drift silently
corrupted UTSF generation in two ways:
  - pincodes the vendor served got dropped as "unknown" at generation time
    because the generator's snapshot lacked them (see knowledge/geo_overrides.py
    / pinOverrides)
  - "phantom served" pincodes appeared at reconstruction time because the
    frontend's snapshot placed pincodes in a zone that the generator's snapshot
    didn't (see fc4_schema.determine_coverage_mode's docstring)

THIS FILE (utsf-generator/data/pincodes.json) is the canonical snapshot — the
generator is the most data-integrity-critical consumer, so its copy is the one
that must never silently fall behind. Whenever it is updated (new pincodes,
corrected zones), run this script to propagate the change everywhere else.

Usage:  python sync_pincodes.py
"""

import json
import os
import shutil

_HERE = os.path.dirname(os.path.abspath(__file__))
CANONICAL = os.path.join(_HERE, "pincodes.json")

# Every other location that must stay byte-identical to CANONICAL.
TARGETS = [
    os.path.join(_HERE, "..", "..", "freight-compare-frontend", "public", "pincodes.json"),
    os.path.join(_HERE, "..", "..", "freight-compare-frontend", "dist", "pincodes.json"),
    os.path.join(_HERE, "..", "..", "freight-compare-backend", "data", "pincodes.json"),
    os.path.join(_HERE, "..", "..", "freight-compare-tester", "data", "pincodes.json"),
]


def main():
    with open(CANONICAL, encoding="utf-8") as f:
        canonical_data = json.load(f)
    print(f"[sync_pincodes] Canonical: {CANONICAL} ({len(canonical_data):,} entries)")

    for target in TARGETS:
        target = os.path.normpath(target)
        if not os.path.exists(os.path.dirname(target)):
            print(f"[sync_pincodes]   SKIP (parent dir missing): {target}")
            continue
        shutil.copyfile(CANONICAL, target)
        print(f"[sync_pincodes]   synced -> {target}")

    print("[sync_pincodes] Done. All copies are now byte-identical to canonical.")


if __name__ == "__main__":
    main()
