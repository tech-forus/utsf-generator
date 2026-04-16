"""
Geographic Validator
=====================
Validates pincodes and zone assignments using hard geographic rules.

Rules enforced:
  1. Pincode must be exactly 6 digits (100000–999999)
  2. Pincode's first digit constrains which zones it can be in
  3. Pincode's first two digits give finer zone constraints
  4. If the pincode exists in pincodes.json → its zone is known exactly
  5. A state's pincodes can never be in impossible zones (e.g., AP never in N1)

Usage:
    from knowledge.geo_validator import GeoValidator
    gv = GeoValidator("data/pincodes.json")
    ok, issue = gv.validate_pincode_in_zone(110001, "S1")  # False, "Delhi PIN in S1"
    zone = gv.lookup_zone(110001)  # "N1"
"""

import json
import os
from typing import Dict, List, Optional, Tuple, Set

from knowledge.dictionary import (
    PINCODE_PREFIX_ZONES,
    IMPOSSIBLE_ZONES_FOR_PREFIX,
    PINCODE_2PREFIX_ZONES,
    STATE_TO_ZONES,
    ZONE_IMPOSSIBLE_STATES,
    ALL_ZONES,
    ZONE_SET,
)


class GeoValidator:
    """
    Geographic validation engine.
    Loads pincodes.json once; all lookups are O(1) via dict.
    """

    def __init__(self, pincodes_path: str):
        self._pin_to_data: Dict[str, Dict] = {}
        self._pin_to_zone: Dict[int, str] = {}
        self._pin_to_state: Dict[int, str] = {}
        self._loaded = False
        self._load(pincodes_path)

    def _load(self, pincodes_path: str):
        if not os.path.exists(pincodes_path):
            print(f"[GeoValidator] WARNING: pincodes.json not found at {pincodes_path}")
            return
        try:
            with open(pincodes_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for entry in data:
                pin_str = str(entry.get("pincode", "")).strip()
                zone    = str(entry.get("zone", "")).strip().upper()
                state   = str(entry.get("state", "")).strip().upper()
                if len(pin_str) == 6 and pin_str.isdigit():
                    pin_int = int(pin_str)
                    self._pin_to_zone[pin_int]  = zone
                    self._pin_to_state[pin_int] = state
                    self._pin_to_data[pin_str]  = entry
            self._loaded = True
            print(f"[GeoValidator] Loaded {len(self._pin_to_zone):,} pincodes")
        except Exception as e:
            print(f"[GeoValidator] ERROR loading pincodes: {e}")

    # ── Pincode format validation ─────────────────────────────────────────────

    @staticmethod
    def is_valid_format(pin) -> bool:
        """Return True if pin is a valid 6-digit Indian pincode."""
        try:
            n = int(float(str(pin).replace(",", "").strip()))
            return 100000 <= n <= 999999
        except (ValueError, TypeError):
            return False

    @staticmethod
    def to_int(pin) -> Optional[int]:
        """Parse any pincode representation to int, or None if invalid."""
        try:
            n = int(float(str(pin).replace(",", "").strip()))
            if 100000 <= n <= 999999:
                return n
            return None
        except (ValueError, TypeError):
            return None

    # ── Zone lookup ───────────────────────────────────────────────────────────

    def lookup_zone(self, pin: int) -> Optional[str]:
        """Return canonical zone for a pincode, or None if not in database."""
        return self._pin_to_zone.get(pin)

    def lookup_state(self, pin: int) -> Optional[str]:
        """Return state for a pincode, or None if not in database."""
        return self._pin_to_state.get(pin)

    def is_known(self, pin: int) -> bool:
        """Return True if pincode exists in the master database."""
        return pin in self._pin_to_zone

    # ── Zone validation ───────────────────────────────────────────────────────

    def validate_pincode_in_zone(
        self, pin: int, zone: str
    ) -> Tuple[bool, str]:
        """
        Check whether a pincode can possibly belong to a given zone.
        Returns (valid, reason).

        Validation layers (stop at first failure):
          L1: pincode format (6 digits)
          L2: zone is a known canonical zone
          L3: if pincode is in database → check exact match
          L4: if not in database → check first-digit prefix rules
          L5: two-digit prefix rules (finer grained)
        """
        if not self.is_valid_format(pin):
            return False, f"Invalid pincode format: {pin!r} (must be 6 digits)"

        zone_upper = zone.upper().strip()
        if zone_upper not in ZONE_SET:
            return False, f"Unknown zone: {zone!r}"

        # L3: exact lookup
        known_zone = self._pin_to_zone.get(int(pin))
        if known_zone is not None:
            if known_zone == zone_upper:
                return True, "exact match in database"
            # Allow if it's in a cross-zone scenario (same region)
            # but flag as mismatch
            return False, (
                f"PIN {pin} is in zone {known_zone} "
                f"(not {zone_upper}) per master database"
            )

        # L4: first-digit prefix constraint
        prefix1 = str(pin)[0]
        impossible = IMPOSSIBLE_ZONES_FOR_PREFIX.get(prefix1, set())
        if zone_upper in impossible:
            possible = PINCODE_PREFIX_ZONES.get(prefix1, [])
            return False, (
                f"PIN {pin} (prefix {prefix1}x) cannot be in {zone_upper}. "
                f"Pincodes starting with {prefix1} belong to: {possible}"
            )

        # L5: two-digit prefix constraint
        prefix2 = str(pin)[:2]
        possible2 = PINCODE_2PREFIX_ZONES.get(prefix2)
        if possible2 is not None and zone_upper not in possible2:
            return False, (
                f"PIN {pin} (prefix {prefix2}x) is unlikely in {zone_upper}. "
                f"Expected zones for {prefix2}x: {possible2}"
            )

        return True, "prefix rules passed (not in database)"

    def get_likely_zones(self, pin: int) -> List[str]:
        """
        Return the most likely canonical zones for a pincode.
        Priority: database lookup → two-digit prefix → one-digit prefix.
        """
        # L1: exact
        known = self._pin_to_zone.get(pin)
        if known:
            return [known]

        # L2: two-digit prefix
        prefix2 = str(pin)[:2]
        if prefix2 in PINCODE_2PREFIX_ZONES:
            return PINCODE_2PREFIX_ZONES[prefix2]

        # L3: one-digit prefix
        prefix1 = str(pin)[0]
        return PINCODE_PREFIX_ZONES.get(prefix1, ALL_ZONES)

    # ── Bulk validation ───────────────────────────────────────────────────────

    def validate_zone_pincodes(
        self, zone: str, pincodes: List[int]
    ) -> Dict:
        """
        Validate a list of pincodes claimed to belong to a zone.

        Returns:
            {
                "valid":    [int, ...],      # accepted
                "invalid":  [(pin, reason)], # rejected
                "warnings": [(pin, reason)], # accepted but suspicious
                "database_mismatches": [(pin, actual_zone)],
            }
        """
        valid    = []
        invalid  = []
        warnings = []
        db_mismatches = []

        for pin in pincodes:
            if not self.is_valid_format(pin):
                invalid.append((pin, f"Not a valid 6-digit pincode: {pin!r}"))
                continue

            pin_int = int(pin)
            ok, reason = self.validate_pincode_in_zone(pin_int, zone)

            if ok:
                valid.append(pin_int)
                if "not in database" in reason:
                    warnings.append((pin_int, reason))
            else:
                if "master database" in reason:
                    # Could be cross-zone but still flagged
                    db_mismatches.append((pin_int, reason))
                    # Still add to valid since transporter may intentionally
                    # price this pincode under a different zone (cross-zone)
                    valid.append(pin_int)
                    warnings.append((pin_int, f"Cross-zone? {reason}"))
                else:
                    invalid.append((pin_int, reason))

        return {
            "valid":              valid,
            "invalid":            invalid,
            "warnings":           warnings,
            "database_mismatches": db_mismatches,
        }

    def filter_impossible_pincodes(
        self, zone: str, pincodes: List[int], strict: bool = False
    ) -> Tuple[List[int], List[Tuple[int, str]]]:
        """
        Remove pincodes that are geographically impossible for the given zone.
        Returns (accepted_pincodes, rejected_list).

        strict=True: also reject database mismatches (not just prefix violations)
        strict=False: only reject hard prefix-rule violations
        """
        accepted = []
        rejected = []

        zone_upper = zone.upper()

        for pin in pincodes:
            pin_int = self.to_int(pin)
            if pin_int is None:
                rejected.append((pin, "Invalid format"))
                continue

            prefix1 = str(pin_int)[0]
            impossible = IMPOSSIBLE_ZONES_FOR_PREFIX.get(prefix1, set())

            if zone_upper in impossible:
                rejected.append((pin_int,
                    f"Prefix {prefix1}x cannot be in {zone_upper}"))
                continue

            if strict:
                known_zone = self._pin_to_zone.get(pin_int)
                if known_zone and known_zone != zone_upper:
                    rejected.append((pin_int,
                        f"DB says zone={known_zone}, not {zone_upper}"))
                    continue

            accepted.append(pin_int)

        return accepted, rejected

    # ── Summary helpers ───────────────────────────────────────────────────────

    def summarize_pincodes_by_zone(
        self, pincodes: List[int]
    ) -> Dict[str, List[int]]:
        """
        Group a flat list of pincodes by their canonical zone.
        Unknown pincodes go under 'UNKNOWN'.
        """
        result: Dict[str, List[int]] = {}
        for pin in pincodes:
            pin_int = self.to_int(pin)
            if pin_int is None:
                continue
            zone = self._pin_to_zone.get(pin_int, "UNKNOWN")
            result.setdefault(zone, []).append(pin_int)
        return result

    def get_zone_distribution(
        self, pincodes: List[int]
    ) -> Dict[str, float]:
        """
        Return percentage distribution of pincodes across canonical zones.
        Useful for inferring what canonical zone a vague transporter zone is.
        """
        by_zone = self.summarize_pincodes_by_zone(pincodes)
        known = {z: v for z, v in by_zone.items() if z != "UNKNOWN"}
        total = sum(len(v) for v in known.values())
        if total == 0:
            return {}
        return {z: round(len(v) / total * 100, 1) for z, v in known.items()}

    def infer_canonical_zone(self, pincodes: List[int]) -> Optional[str]:
        """
        Given a list of pincodes, infer the single most likely canonical zone.
        Returns zone with highest coverage, or None if ambiguous (< 60% dominant).
        """
        dist = self.get_zone_distribution(pincodes)
        if not dist:
            return None
        top_zone, top_pct = max(dist.items(), key=lambda x: x[1])
        if top_pct >= 60.0:
            return top_zone
        return None   # ambiguous

    def infer_canonical_zones(
        self, pincodes: List[int], threshold: float = 10.0
    ) -> List[str]:
        """
        Return all canonical zones that cover >= threshold% of the pincodes.
        More lenient than infer_canonical_zone — returns multiple zones.
        """
        dist = self.get_zone_distribution(pincodes)
        return sorted(
            [z for z, pct in dist.items() if pct >= threshold],
            key=lambda z: -dist[z]
        )
