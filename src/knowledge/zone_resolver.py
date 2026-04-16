"""
Zone Resolver
=============
Resolves vague or transporter-specific zone names to canonical zones
by analysing which pincodes belong to each zone.

The core insight: if a transporter labels their zone "WEST" and gives
a list of pincodes [400001, 380001, 395001, ...], we can look up those
pincodes in pincodes.json and confirm they're in W1/W2.

Key behaviours:
  - Accepts any dict of {transporter_zone_label: [pincodes]}
  - For each transporter zone, computes canonical zone distribution
  - Maps each transporter label -> best canonical zone(s)
  - Rejects impossible cross-zone assignments
  - Validates zone matrix keys if pincodes are available

Usage:
    from knowledge.zone_resolver import ZoneResolver
    from knowledge.geo_validator import GeoValidator

    gv = GeoValidator("data/pincodes.json")
    zr = ZoneResolver(gv)

    # Resolve from pincode data
    mapping = zr.resolve_zone_labels({"WEST": [400001, 380001, 395003]})
    # -> {"WEST": ["W1", "W2"]}

    # Validate a zone matrix
    clean_matrix, issues = zr.validate_zone_matrix(raw_matrix, served_pincodes)
"""

from typing import Dict, List, Optional, Tuple, Set
from collections import defaultdict, Counter

from knowledge.geo_validator import GeoValidator
from knowledge.smart_matcher import SmartMatcher
from knowledge.dictionary import ALL_ZONES, ZONE_SET, ZONE_SYNONYMS


class ZoneResolver:
    """
    Resolves vague zone labels and validates zone-pincode assignments.
    Requires a loaded GeoValidator instance.
    """

    def __init__(self, geo_validator: GeoValidator, smart_matcher: SmartMatcher = None):
        self.gv = geo_validator
        self.sm = smart_matcher or SmartMatcher()

    # ─── Label resolution ─────────────────────────────────────────────────────

    def resolve_zone_labels(
        self,
        zone_pincodes: Dict[str, List[int]],
        min_coverage: float = 40.0,
    ) -> Dict[str, List[str]]:
        """
        Given a dict of {transporter_zone_label: [pincodes]},
        return {transporter_zone_label: [canonical_zones]}.

        Strategy per label:
          1. If label is already a canonical zone -> done
          2. Try SmartMatcher dictionary lookup
          3. If not found or ambiguous -> use pincodes to infer (pincode majority vote)
          4. Zones that cover >= min_coverage% of the pincodes are included

        Returns mapping from transporter label -> canonical zone list.
        Raises no exceptions — unknown labels map to [] with a warning printed.
        """
        mapping: Dict[str, List[str]] = {}

        for label, pins in zone_pincodes.items():
            label_upper = str(label).upper().strip()

            # L1: already canonical
            if label_upper in ZONE_SET:
                mapping[label] = [label_upper]
                continue

            # L2: dictionary / fuzzy match
            sm_result = self.sm.match_zone(label, min_confidence=0.7)
            if sm_result.value and sm_result.confidence >= 0.7:
                mapping[label] = sm_result.value
                print(f"[ZoneResolver] '{label}' -> {sm_result.value} "
                      f"(method={sm_result.method}, conf={sm_result.confidence:.2f})")
                continue

            # L3: pincode-based inference
            if pins:
                dist = self.gv.get_zone_distribution([int(p) for p in pins if self.gv.is_valid_format(p)])
                if dist:
                    inferred = [z for z, pct in sorted(dist.items(), key=lambda x: -x[1])
                                if pct >= min_coverage]
                    if inferred:
                        mapping[label] = inferred
                        top_items = sorted(dist.items(), key=lambda x: -x[1])[:4]
                        print(f"[ZoneResolver] '{label}' -> {inferred} "
                              f"(pincode inference: {top_items})")
                        continue

            # Fallback: use SmartMatcher with lower threshold
            if sm_result.value:
                mapping[label] = sm_result.value
                print(f"[ZoneResolver] '{label}' -> {sm_result.value} "
                      f"(low-conf fallback, method={sm_result.method}, "
                      f"conf={sm_result.confidence:.2f})")
            else:
                mapping[label] = []
                print(f"[ZoneResolver] WARNING: '{label}' could not be resolved to any canonical zone")

        return mapping

    def resolve_flat_pincodes(
        self, pincodes: List[int]
    ) -> Dict[str, List[int]]:
        """
        Resolve a flat list of pincodes -> grouped by canonical zone.
        Uses pincodes.json for exact lookups; prefix rules as fallback.
        Returns {canonical_zone: [pincodes]}.
        """
        by_zone: Dict[str, List[int]] = defaultdict(list)
        unknown = []

        for pin in pincodes:
            pin_int = self.gv.to_int(pin)
            if pin_int is None:
                continue
            zone = self.gv.lookup_zone(pin_int)
            if zone:
                by_zone[zone].append(pin_int)
            else:
                # Use prefix to guess zone
                likely = self.gv.get_likely_zones(pin_int)
                if len(likely) == 1:
                    by_zone[likely[0]].append(pin_int)
                else:
                    unknown.append(pin_int)

        if unknown:
            print(f"[ZoneResolver] {len(unknown)} pincodes not in database and ambiguous prefix — "
                  f"samples: {unknown[:5]}")

        return dict(by_zone)

    # ─── Zone matrix validation ───────────────────────────────────────────────

    def validate_zone_matrix(
        self,
        raw_matrix: Dict[str, Dict[str, float]],
        served_pincodes: Optional[Dict[str, List[int]]] = None,
    ) -> Tuple[Dict[str, Dict[str, float]], List[str]]:
        """
        Validate and canonicalise a zone matrix.

        1. Expand non-canonical zone keys -> canonical zones
        2. Sanity-check rates (must be > 0, < 1000 Rs/kg)
        3. If served_pincodes provided, warn if an active zone has no rates
        4. Remove origin zones with all-zero / all-None rates

        Returns (canonical_matrix, issue_list).
        """
        canonical: Dict[str, Dict[str, float]] = {}
        issues: List[str] = []

        for orig_label, dest_map in raw_matrix.items():
            if not isinstance(dest_map, dict):
                issues.append(f"Zone matrix row '{orig_label}': not a dict")
                continue

            # Expand origin label
            orig_zones = self._expand_zone_label(orig_label)
            if not orig_zones:
                issues.append(f"Cannot resolve origin zone label: '{orig_label}'")
                continue

            for orig_zone in orig_zones:
                if orig_zone not in canonical:
                    canonical[orig_zone] = {}

                for dest_label, rate in dest_map.items():
                    if rate is None:
                        continue

                    dest_zones = self._expand_zone_label(dest_label)
                    if not dest_zones:
                        issues.append(f"Cannot resolve dest zone label: '{dest_label}'")
                        continue

                    try:
                        rate_f = float(rate)
                    except (ValueError, TypeError):
                        issues.append(f"Non-numeric rate [{orig_zone}][{dest_label}]={rate!r}")
                        continue

                    if rate_f <= 0:
                        issues.append(f"Non-positive rate [{orig_zone}][{dest_label}]={rate_f}")
                        continue
                    if rate_f > 1000:
                        issues.append(f"Unusually high rate [{orig_zone}][{dest_label}]={rate_f} Rs/kg")

                    for dest_zone in dest_zones:
                        canonical[orig_zone][dest_zone] = rate_f

        # Remove origin zones with no rates
        empty_origins = [z for z, dests in canonical.items() if not dests]
        for z in empty_origins:
            del canonical[z]
            issues.append(f"Origin zone {z} had no valid rates — removed")

        # Cross-check with served pincodes if provided
        if served_pincodes:
            active_zones = set(served_pincodes.keys()) & ZONE_SET
            for z in active_zones:
                if z not in canonical:
                    issues.append(f"Zone {z} is served but missing from zone matrix")

        return canonical, issues

    def validate_and_clean_pincode_assignments(
        self,
        zone_pincodes: Dict[str, List[int]],
        strict: bool = False,
    ) -> Tuple[Dict[str, List[int]], List[str]]:
        """
        For each zone, filter out geographically impossible pincodes.

        strict=True: also reject database mismatches
        strict=False: only reject hard prefix-rule violations

        Returns (cleaned_zone_pincodes, issues_list).
        """
        cleaned: Dict[str, List[int]] = {}
        all_issues: List[str] = []

        for zone, pins in zone_pincodes.items():
            zone_upper = zone.upper().strip()
            if zone_upper not in ZONE_SET:
                all_issues.append(f"Unknown zone key: '{zone}'")
                continue

            # Validate format first
            valid_format = []
            for p in pins:
                if self.gv.is_valid_format(p):
                    valid_format.append(self.gv.to_int(p))
                else:
                    all_issues.append(f"Invalid pincode format: {p!r} in zone {zone}")

            accepted, rejected = self.gv.filter_impossible_pincodes(
                zone_upper, valid_format, strict=strict
            )
            cleaned[zone_upper] = accepted

            for pin, reason in rejected:
                all_issues.append(f"[{zone_upper}] PIN {pin}: {reason}")

            if rejected:
                print(f"[ZoneResolver] {zone_upper}: rejected {len(rejected)} impossible pincodes "
                      f"({len(accepted)} kept)")

        return cleaned, all_issues

    # ─── Matrix symmetry helper ───────────────────────────────────────────────

    def fill_symmetric_rates(
        self, matrix: Dict[str, Dict[str, float]]
    ) -> Dict[str, Dict[str, float]]:
        """
        Fill missing reverse rates from existing rates.
        If [N1][S1] = 9.0 but [S1][N1] is missing -> set [S1][N1] = 9.0.
        Many transporters only list one direction.
        """
        filled = {z: dict(dests) for z, dests in matrix.items()}
        zones = list(filled.keys())

        for orig in zones:
            for dest, rate in list(filled[orig].items()):
                if dest not in filled:
                    filled[dest] = {}
                if orig not in filled[dest]:
                    filled[dest][orig] = rate   # mirror

        return filled

    # ─── Internal helpers ─────────────────────────────────────────────────────

    def _expand_zone_label(self, label: str) -> List[str]:
        """Expand a zone label -> list of canonical zones (may be empty)."""
        label_upper = str(label).upper().strip()

        # Already canonical
        if label_upper in ZONE_SET:
            return [label_upper]

        # Direct dictionary lookup
        if label_upper in ZONE_SYNONYMS:
            return ZONE_SYNONYMS[label_upper]

        # SmartMatcher
        r = self.sm.match_zone(label, min_confidence=0.65)
        if r.value:
            return r.value

        return []

    def get_coverage_report(
        self, zone_pincodes: Dict[str, List[int]]
    ) -> Dict[str, Dict]:
        """
        For each zone in zone_pincodes, report:
          - pincode count
          - zone distribution (% breakdown)
          - dominant canonical zone
          - mismatch rate (% assigned to "wrong" canonical zone)
        """
        report = {}
        for label, pins in zone_pincodes.items():
            valid_pins = [self.gv.to_int(p) for p in pins if self.gv.is_valid_format(p)]
            dist = self.gv.get_zone_distribution(valid_pins)
            dominant = self.gv.infer_canonical_zone(valid_pins)

            label_canonical = self._expand_zone_label(label)
            correct_pct = sum(dist.get(z, 0) for z in label_canonical) if label_canonical else 0

            report[label] = {
                "count":            len(valid_pins),
                "distribution":     dict(sorted(dist.items(), key=lambda x: -x[1])),
                "dominant":         dominant,
                "claimed_zones":    label_canonical,
                "match_pct":        round(correct_pct, 1),
                "mismatch_pct":     round(100 - correct_pct, 1),
            }

        return report
