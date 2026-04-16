"""
Zone Mapper
===========
Core intelligence for mapping pincodes to zones and detecting
cross-zone reclassifications by transporters.

The source of truth is pincodes.json — every pincode has one canonical zone.
Transporters may deviate from this:
  - A pincode canonically in N1 might be priced as N2 by some transporters
  - Entire clusters of pincodes may be reclassified

This module:
1. Loads master pincode → zone mapping
2. Groups any list of served pincodes by their canonical zones
3. Detects cross-zone pincodes (served under a different zone than canonical)
4. Chooses optimal coverage mode (FULL_ZONE / EXCLUDING / INCLUDING)
5. Builds the FC4 serviceability block
"""

import json
import os
from typing import Dict, List, Set, Optional, Tuple
from collections import defaultdict

from fc4_schema import (
    ALL_ZONES, REGIONS, ZONE_TO_REGION,
    compress_to_ranges, expand_ranges,
    determine_coverage_mode, empty_zone_entry,
    MODE_NORMALIZE,
    MODE_FULL_ZONE, MODE_FULL_MINUS_EXCEPT, MODE_ONLY_SERVED, MODE_NOT_SERVED,
)

# Common simplified zone labels used by Indian transporters → candidate UTSF zones
# When a transporter uses "North" for a pincode, we need to know which UTSF zone it maps to.
# We use the canonical master zone to disambiguate when there are multiple candidates.
SIMPLIFIED_ZONE_MAP: Dict[str, List[str]] = {
    # Regional labels
    "NORTH":      ["N1", "N2", "N3", "N4"],
    "N":          ["N1", "N2", "N3", "N4"],
    "SOUTH":      ["S1", "S2", "S3", "S4"],
    "S":          ["S1", "S2", "S3", "S4"],
    "EAST":       ["E1", "E2"],
    "E":          ["E1", "E2"],
    "WEST":       ["W1", "W2"],
    "W":          ["W1", "W2"],
    "CENTRAL":    ["C1", "C2"],
    "C":          ["C1", "C2"],
    "NORTHEAST":  ["NE1", "NE2"],
    "NORTH EAST": ["NE1", "NE2"],
    "NE":         ["NE1", "NE2"],
    "NE/JK":      ["NE1", "NE2", "X3"],
    "J&K":        ["X3"],
    "JK":         ["X3"],
    "LADAKH":     ["X3"],
    "ANDAMAN":    ["X1"],
    "LAKSHADWEEP":["X2"],
    # 9-zone simplified schemes (common in B2B rate cards)
    "ZONE A":     ["N1", "N2"],
    "ZONE B":     ["N3", "N4"],
    "ZONE C":     ["E1", "E2"],
    "ZONE D":     ["W1", "W2"],
    "ZONE E":     ["S1", "S2", "S3", "S4"],
    "ZONE F":     ["C1", "C2"],
    "ZONE G":     ["NE1", "NE2"],
    # Special zones (single-map)
    "X1":         ["X1"],
    "X2":         ["X2"],
    "X3":         ["X3"],
}


class ZoneMapper:
    """
    Maps pincodes to canonical zones and builds FC4 serviceability blocks.
    Now uses GeoValidator for geographic impossibility checks.
    """

    def __init__(self, pincodes_path: str, zones_data_path: str = None):
        self.pincode_to_zone: Dict[int, str] = {}
        self.zone_to_pincodes: Dict[str, Set[int]] = defaultdict(set)
        self._pincodes_path = pincodes_path
        self._geo_validator = None
        self._load_pincodes(pincodes_path)

    def _load_pincodes(self, path: str):
        """Load master pincodes.json and build lookup tables."""
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        for entry in data:
            try:
                pin = int(entry["pincode"])
                zone = entry["zone"].upper().strip()
                self.pincode_to_zone[pin] = zone
                self.zone_to_pincodes[zone].add(pin)
            except (KeyError, ValueError):
                continue

        print(f"[ZoneMapper] Loaded {len(self.pincode_to_zone):,} pincodes across {len(self.zone_to_pincodes)} zones")
        # Lazy-load GeoValidator for impossibility checks
        try:
            from knowledge.geo_validator import GeoValidator
            self._geo_validator = GeoValidator(self._pincodes_path)
        except Exception:
            self._geo_validator = None

    def get_zone(self, pincode: int) -> Optional[str]:
        """Get canonical zone for a pincode."""
        return self.pincode_to_zone.get(pincode)

    def get_zone_pincodes(self, zone: str) -> Set[int]:
        """Get all pincodes in a canonical zone."""
        return self.zone_to_pincodes.get(zone.upper(), set())

    def resolve_transporter_zone(self, t_zone: str, pin: int) -> Optional[str]:
        """
        Resolve a transporter's zone label (possibly simplified) to a canonical UTSF zone.

        Priority:
        1. Already a UTSF zone (N1, W2, etc.) → return as-is (this IS the zone override)
        2. Simplified label (North, West, etc.) → expand via SIMPLIFIED_ZONE_MAP,
           use master pincode zone to pick the right one
        3. Unknown → return None (fall back to canonical)
        """
        t_upper = t_zone.upper().strip()

        # Already a canonical UTSF zone → direct override
        if t_upper in ALL_ZONES:
            return t_upper

        # Simplified zone label → resolve via SIMPLIFIED_ZONE_MAP
        candidates = SIMPLIFIED_ZONE_MAP.get(t_upper, [])

        # If still not found, try SmartMatcher for fuzzy/geo lookup
        if not candidates:
            try:
                from knowledge.smart_matcher import SmartMatcher
                _sm = SmartMatcher()
                r = _sm.match_zone(t_zone, min_confidence=0.65)
                if r.value:
                    candidates = r.value
            except Exception:
                pass

        if candidates:
            canonical = self.pincode_to_zone.get(pin, "")
            if canonical in candidates:
                # Canonical agrees → no deviation
                return canonical
            # Canonical disagrees → transporter is pricing it differently
            return candidates[0]

        return None  # Unknown zone label

    def all_zone_pincodes(self) -> Dict[str, Set[int]]:
        """Return full zone → pincodes mapping."""
        return dict(self.zone_to_pincodes)

    def group_by_canonical_zone(self, pincodes: List[int]) -> Dict[str, List[int]]:
        """
        Group a list of pincodes by their canonical zone.
        Pincodes not in master pincodes.json are placed in '_unknown'.
        """
        groups: Dict[str, List[int]] = defaultdict(list)
        for pin in pincodes:
            zone = self.pincode_to_zone.get(pin, "_unknown")
            groups[zone].append(pin)
        return dict(groups)

    def build_serviceability(
        self,
        served_pincodes: List[int],
        oda_pincodes: List[int] = None,
        transporter_zone_override: Dict[int, str] = None,
        coverage_threshold: float = 0.5
    ) -> Dict:
        """
        Build the v2.1 UTSF serviceability block from a list of served pincodes.

        Returns:
            serviceability dict {zone: coverage_entry} in v2.1 format:
              mode           FULL_ZONE | FULL_MINUS_EXCEPT | ONLY_SERVED | NOT_SERVED
              exceptRanges   [{s,e}]  — for FULL_MINUS_EXCEPT
              exceptSingles  [int]
              servedRanges   [{s,e}]  — for ONLY_SERVED
              servedSingles  [int]
              crossZoneRanges / crossZoneSingles
              odaRanges / odaSingles / odaCount  (inline ODA; also in oda block)
              totalInZone, servedCount, coveragePercent
        """
        if oda_pincodes is None:
            oda_pincodes = []
        if transporter_zone_override is None:
            transporter_zone_override = {}

        # ── Geo validation: reject impossible pincodes before processing ──────
        gv = self._geo_validator
        if gv:
            orig_count = len(served_pincodes)
            valid_pins = []
            invalid_pins = []
            for p in served_pincodes:
                if gv.is_valid_format(p):
                    valid_pins.append(int(p))
                else:
                    invalid_pins.append(p)
            if invalid_pins:
                print(f"[ZoneMapper] Removed {len(invalid_pins)} invalid-format pincodes "
                      f"(e.g. {invalid_pins[:3]})")
            served_pincodes = valid_pins

            valid_oda = [int(p) for p in oda_pincodes if gv.is_valid_format(p)]
            if len(valid_oda) < len(oda_pincodes):
                print(f"[ZoneMapper] Removed {len(oda_pincodes)-len(valid_oda)} "
                      f"invalid-format ODA pincodes")
            oda_pincodes = valid_oda

        served_set = set(served_pincodes)
        oda_set    = set(oda_pincodes)

        print(f"[ZoneMapper.build_serviceability] Input: "
              f"{len(served_set):,} served, {len(oda_set):,} ODA, "
              f"{len(transporter_zone_override):,} zone overrides, "
              f"threshold={coverage_threshold}")

        # Step 1: Effective zone per served pincode
        effective_zone: Dict[int, str] = {}
        unknown_pincodes = 0
        for pin in served_set:
            if pin in transporter_zone_override:
                effective_zone[pin] = transporter_zone_override[pin].upper()
            else:
                canonical = self.pincode_to_zone.get(pin)
                if canonical:
                    effective_zone[pin] = canonical
                else:
                    unknown_pincodes += 1

        if unknown_pincodes:
            print(f"[ZoneMapper.build_serviceability] "
                  f"{unknown_pincodes:,} served pincodes not in master (skipped)")

        # Step 2: Group by effective zone
        served_by_effective_zone: Dict[str, Set[int]] = defaultdict(set)
        for pin, zone in effective_zone.items():
            served_by_effective_zone[zone].add(pin)

        # Step 3: Cross-zone pincodes
        cross_zone_by_target: Dict[str, Set[int]] = defaultdict(set)
        cross_zone_total = 0
        for pin in served_set:
            canonical = self.pincode_to_zone.get(pin)
            if not canonical:
                continue
            eff = effective_zone.get(pin, canonical)
            if eff != canonical:
                cross_zone_by_target[eff].add(pin)
                cross_zone_total += 1

        if cross_zone_total:
            print(f"[ZoneMapper.build_serviceability] "
                  f"{cross_zone_total:,} cross-zone pincodes detected")

        # Step 4: Build per-zone entries (v2.1 field names)
        serviceability: Dict = {}
        zones_built: List[str] = []
        mode_counts: Dict[str, int] = {}

        for zone in ALL_ZONES:
            canonical_pins  = self.zone_to_pincodes.get(zone, set())
            served_in_zone  = served_by_effective_zone.get(zone, set())
            cross_zone_pins = cross_zone_by_target.get(zone, set())

            # Pincodes from canonical zone only (no cross-zone arrivals)
            canonical_served = served_in_zone - cross_zone_pins

            mode = determine_coverage_mode(
                canonical_served, canonical_pins, coverage_threshold
            )

            if mode == MODE_NOT_SERVED and not cross_zone_pins:
                continue

            entry = empty_zone_entry()
            entry["mode"] = mode
            entry["totalInZone"] = len(canonical_pins)
            mode_counts[mode] = mode_counts.get(mode, 0) + 1

            if mode == MODE_FULL_MINUS_EXCEPT:
                exceptions = canonical_pins - canonical_served
                compressed = compress_to_ranges(sorted(exceptions))
                entry["exceptRanges"]  = compressed["ranges"]
                entry["exceptSingles"] = compressed["singles"]

            elif mode == MODE_ONLY_SERVED:
                compressed = compress_to_ranges(sorted(canonical_served))
                entry["servedRanges"]  = compressed["ranges"]
                entry["servedSingles"] = compressed["singles"]

            # Cross-zone
            if cross_zone_pins:
                compressed = compress_to_ranges(sorted(cross_zone_pins))
                entry["crossZoneRanges"]  = compressed["ranges"]
                entry["crossZoneSingles"] = compressed["singles"]

            # ODA inline
            oda_in_zone = (served_in_zone | cross_zone_pins) & oda_set
            if oda_in_zone:
                compressed = compress_to_ranges(sorted(oda_in_zone))
                entry["odaRanges"]  = compressed["ranges"]
                entry["odaSingles"] = compressed["singles"]
                entry["odaCount"]   = len(oda_in_zone)

            # Stats
            total_served = len(canonical_served) + len(cross_zone_pins)
            entry["servedCount"] = total_served
            entry["coveragePercent"] = (
                round(len(canonical_served) / len(canonical_pins) * 100, 2)
                if canonical_pins else 0.0
            )

            cov_str   = f"{entry['coveragePercent']:.1f}%"
            cross_str = f"  +{len(cross_zone_pins)} cross-zone" if cross_zone_pins else ""
            oda_str   = f"  ODA:{len(oda_in_zone)}" if oda_in_zone else ""
            print(f"[ZoneMapper.build_serviceability]   {zone:>4}: "
                  f"{len(canonical_served):>6,}/{len(canonical_pins):>6,} "
                  f"({cov_str:>6}) -> {mode:<18}{cross_str}{oda_str}")

            serviceability[zone] = entry
            zones_built.append(zone)

        print(f"[ZoneMapper.build_serviceability] Done: "
              f"{len(zones_built)} zones built  mode_counts={mode_counts}")
        return serviceability

    def build_oda_block(self, serviceability: Dict) -> Dict:
        """
        Build the separate v2.1 oda block from serviceability inline ODA data.
        Returns: {zone: {odaCount, odaRanges, odaSingles}}
        """
        oda_block: Dict = {}
        for zone, entry in serviceability.items():
            oda_ranges  = entry.get("odaRanges", [])
            oda_singles = entry.get("odaSingles", [])
            oda_count   = entry.get("odaCount", 0)
            if oda_count > 0 or oda_ranges or oda_singles:
                oda_block[zone] = {
                    "odaCount":   oda_count,
                    "odaRanges":  oda_ranges,
                    "odaSingles": oda_singles,
                }
        return oda_block

    def detect_transporter_zone_overrides(
        self,
        transporter_zone_to_pincodes: Dict[str, List[int]]
    ) -> Dict[int, str]:
        """
        Given a transporter's own zone→pincodes mapping, detect which pincodes
        are priced in a different zone than our canonical system.
        Handles both UTSF zone labels (N1, W2) and simplified labels (North, West).

        Returns:
            {pincode: resolved_utsf_zone} for pincodes that differ from canonical
        """
        overrides = {}
        for t_zone, pins in transporter_zone_to_pincodes.items():
            for pin in pins:
                pin = int(pin)
                resolved = self.resolve_transporter_zone(t_zone, pin)
                if resolved is None:
                    continue  # Unknown zone — skip, let canonical handle it
                canonical = self.pincode_to_zone.get(pin)
                if canonical and resolved != canonical:
                    overrides[pin] = resolved
        return overrides

    def build_index(
        self,
        serviceability: Dict
    ) -> Tuple[Set[int], Set[int], Dict[int, str]]:
        """
        Build O(1) lookup indexes from v2.1 serviceability block.
        Handles both v2.1 (exceptRanges/servedRanges) and FC4 (excludedRanges/includedRanges).
        Returns: (served_pincodes, oda_pincodes, pincode_to_zone)
        """
        served_pincodes: Set[int] = set()
        oda_pincodes: Set[int] = set()
        pincode_to_zone: Dict[int, str] = {}

        for zone, data in serviceability.items():
            raw_mode = data.get("mode", "NOT_SERVED")
            mode = MODE_NORMALIZE.get(raw_mode, "NOT_SERVED")
            canonical_pins = self.zone_to_pincodes.get(zone, set())

            if mode == "FULL_ZONE":
                zone_served = set(canonical_pins)
            elif mode in ("FULL_MINUS_EXCEPT", "EXCLUDING"):
                # v2.1: exceptRanges; FC4: excludedRanges
                excluded = set(expand_ranges(
                    data.get("exceptRanges", data.get("excludedRanges", [])),
                    data.get("exceptSingles", data.get("excludedSingles", []))
                ))
                zone_served = canonical_pins - excluded
            elif mode in ("ONLY_SERVED", "INCLUDING"):
                # v2.1: servedRanges; FC4: includedRanges
                zone_served = set(expand_ranges(
                    data.get("servedRanges", data.get("includedRanges", [])),
                    data.get("servedSingles", data.get("includedSingles", []))
                ))
            else:
                zone_served = set()

            # Cross-zone pincodes
            cross = set(expand_ranges(
                data.get("crossZoneRanges", []),
                data.get("crossZoneSingles", [])
            ))
            zone_served |= cross

            served_pincodes |= zone_served
            for pin in zone_served:
                pincode_to_zone[pin] = zone

            # ODA (inline)
            oda = set(expand_ranges(
                data.get("odaRanges", []),
                data.get("odaSingles", [])
            ))
            oda_pincodes |= (oda & zone_served)

        return served_pincodes, oda_pincodes, pincode_to_zone

    def compute_stats(
        self,
        serviceability: Dict
    ) -> Dict:
        """Compute FC4 stats block from serviceability data."""
        served, oda, pz = self.build_index(serviceability)
        active_zones = [z for z, d in serviceability.items()
                        if d.get("mode") != "NOT_SERVED" or
                        d.get("crossZoneRanges") or d.get("crossZoneSingles")]

        coverage_by_region = {}
        for region, zones in REGIONS.items():
            region_total = sum(len(self.zone_to_pincodes.get(z, set())) for z in zones)
            region_served = sum(
                serviceability.get(z, {}).get("servedCount", 0)
                for z in zones
            )
            coverage_by_region[region] = {
                "served": region_served,
                "total": region_total,
                "percent": round(region_served / region_total * 100, 2) if region_total else 0.0
            }

        avg_coverage = 0.0
        active_entries = [d for d in serviceability.values() if d.get("mode") != "NOT_SERVED"]
        if active_entries:
            avg_coverage = round(
                sum(d.get("coveragePercent", 0) for d in active_entries) / len(active_entries), 2
            )

        return {
            "totalServedPincodes": len(served),
            "totalOdaPincodes": len(oda),
            "zonesServed": len(active_zones),
            "activeZones": sorted(active_zones),
            "coverageByRegion": coverage_by_region,
            "avgCoveragePercent": avg_coverage,
        }
