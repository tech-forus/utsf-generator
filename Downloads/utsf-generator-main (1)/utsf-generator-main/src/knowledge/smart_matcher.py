"""
Smart Matcher
=============
Resolves any raw label (from an Excel header, PDF text, etc.) to a canonical
UTSF field name or zone list, using a layered lookup strategy:

  Layer 1: Exact match (lowercased / uppercased)
  Layer 2: Normalised match (strip punctuation, collapse spaces)
  Layer 3: Substring / token match
  Layer 4: Fuzzy match (difflib SequenceMatcher)
  Layer 5: Geographic fallback (state/city names in label)

Each method returns a MatchResult with:
  - value     : canonical field name or zone list
  - confidence: 0.0–1.0
  - method    : "exact" | "normalised" | "substring" | "token" | "fuzzy" | "geo"

Usage:
    from knowledge.smart_matcher import SmartMatcher
    sm = SmartMatcher()
    r = sm.match_charge("Fuel Surcharge %")     # MatchResult(value="fuel", confidence=1.0, method="exact")
    r = sm.match_zone("Western India")           # MatchResult(value=["W1","W2"], confidence=0.95, method="normalised")
    r = sm.match_charge("Petrol Surcharge")      # MatchResult(value="fuel", confidence=0.82, method="fuzzy")
"""

import os
import re
import difflib
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple

from knowledge.dictionary import (
    CHARGE_SYNONYMS,
    COMPANY_SYNONYMS,
    ZONE_SYNONYMS,
    ALL_ZONES,
    ZONE_SET,
    STATE_TO_ZONES,
    PINCODE_PREFIX_ZONES,
)

# Optional learned corrections (saved by user feedback — see learned_dict.py)
try:
    from knowledge.learned_dict import LEARNED_CHARGES, LEARNED_ZONES
except ImportError:
    LEARNED_CHARGES: Dict[str, str] = {}
    LEARNED_ZONES:   Dict[str, List[str]] = {}


def _load_runtime_corrections():
    """
    Load user corrections from the writable directory.

    Works in both modes:
      - Dev mode:   reads from src/knowledge/learned_dict.py (same as static import above)
      - Frozen EXE: reads from UTSF_ROOT/knowledge/ (writable, next to EXE)

    Also reads high-confidence entries directly from learning_data.json so
    corrections are available even without the Python file.

    Returns: (charges_dict, zones_dict)
    """
    import json as _json

    rt_charges: Dict[str, str]       = {}
    rt_zones:   Dict[str, List[str]] = {}

    root = os.environ.get("UTSF_ROOT")
    if not root:
        return rt_charges, rt_zones

    kdir = os.path.join(root, "knowledge")

    # ── 1. learned_dict.py in writable dir ────────────────────────────────────
    ld_py = os.path.join(kdir, "learned_dict.py")
    if os.path.isfile(ld_py):
        try:
            ns: Dict = {}
            with open(ld_py, "r", encoding="utf-8") as _f:
                exec(compile(_f.read(), ld_py, "exec"), ns)  # noqa: S102
            rt_charges.update(ns.get("LEARNED_CHARGES", {}))
            rt_zones.update(ns.get("LEARNED_ZONES", {}))
        except Exception:
            pass

    # ── 2. learning_data.json — promoted entries ───────────────────────────────
    ld_json = os.path.join(kdir, "learning_data.json")
    if os.path.isfile(ld_json):
        try:
            with open(ld_json, "r", encoding="utf-8") as _f:
                data = _json.load(_f)
            for entry in data.get("entries", {}).values():
                if not (entry.get("auto_promoted") or entry.get("confidence", 0) >= 0.85):
                    continue
                t     = entry.get("type", "")
                raw_e = entry.get("raw", "").lower().strip()
                canon = entry.get("canonical")
                if not raw_e or canon is None:
                    continue
                if t == "charge":
                    rt_charges[raw_e] = canon
                elif t == "zone":
                    rt_zones[raw_e.upper()] = canon if isinstance(canon, list) else [canon]
        except Exception:
            pass

    return rt_charges, rt_zones


@dataclass
class MatchResult:
    value: Any          # canonical field name (str) or zone list ([str,...])
    confidence: float   # 0.0 – 1.0
    method: str         # "exact" | "normalised" | "substring" | "token" | "fuzzy" | "geo" | "learned"
    raw: str = ""       # original input string


NO_MATCH = MatchResult(value=None, confidence=0.0, method="none")


def _normalise(s: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    s = s.lower()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _tokens(s: str) -> set:
    """Return set of meaningful tokens (words ≥ 2 chars)."""
    return {w for w in s.split() if len(w) >= 2}


def _strip_noise(s: str) -> str:
    """Strip common suffix noise words used in zone column labels."""
    noise = [
        "india", "region", "area", "zone", "zones", "state", "states",
        "part", "belt", "corridor", "hub", "sector", "circle",
    ]
    words = s.split()
    filtered = [w for w in words if w.lower() not in noise]
    return " ".join(filtered) if filtered else s


class SmartMatcher:
    """
    Stateless (after init) smart lookup for charge fields and zone names.
    Build once, reuse everywhere.
    """

    def __init__(self):
        # Pre-build normalised lookup tables for O(1) after normalisation
        self._charge_norm: Dict[str, Tuple[str, str]] = {}
        self._zone_norm:   Dict[str, Tuple[List[str], str]] = {}
        self._company_norm: Dict[str, Tuple[str, str]] = {}

        # Load corrections from writable dir (survives EXE sessions)
        rt_charges, rt_zones = _load_runtime_corrections()
        # rt_ overrides static learned_dict (which may be the bundled read-only copy)
        merged_charges = {**CHARGE_SYNONYMS, **LEARNED_CHARGES, **rt_charges}
        merged_zones   = {**ZONE_SYNONYMS,   **LEARNED_ZONES,   **rt_zones}

        for raw, canonical in merged_charges.items():
            self._charge_norm[_normalise(raw)] = (canonical, raw)

        for raw, zones in merged_zones.items():
            self._zone_norm[_normalise(raw)] = (zones, raw)

        # Store merged learned for fast direct lookup in _lookup()
        self._all_learned_charges = {**LEARNED_CHARGES, **rt_charges}
        self._all_learned_zones   = {**LEARNED_ZONES,   **rt_zones}

        for raw, canonical in COMPANY_SYNONYMS.items():
            n = _normalise(raw)
            self._company_norm[n] = (canonical, raw)

        # Keys for fuzzy matching
        self._charge_keys     = list(self._charge_norm.keys())
        self._zone_keys       = list(self._zone_norm.keys())
        self._company_keys    = list(self._company_norm.keys())

    # ─── Public API ───────────────────────────────────────────────────────────

    def match_charge(self, raw: str, min_confidence: float = 0.6) -> MatchResult:
        """
        Match a raw label to a canonical charge field.
        Returns MatchResult. value=None means skip; value is a string otherwise.
        """
        r = self._lookup(raw, self._charge_norm, self._charge_keys, min_confidence)
        r.raw = raw
        return r

    def match_zone(self, raw: str, min_confidence: float = 0.55) -> MatchResult:
        """
        Match a raw label to a list of canonical zone(s).
        Returns MatchResult where value is List[str] or None.
        """
        # First check if it's already a canonical zone (case-insensitive)
        upper = raw.upper().strip()
        if upper in ZONE_SET:
            return MatchResult(value=[upper], confidence=1.0, method="exact", raw=raw)

        # Try full zone synonym table
        r = self._lookup_zone(raw, min_confidence)
        r.raw = raw

        # Geographic fallback: look for state names embedded in label
        if r.confidence < min_confidence:
            geo_r = self._geo_zone_match(raw)
            if geo_r.confidence > r.confidence:
                return geo_r

        return r

    def match_company_field(self, raw: str, min_confidence: float = 0.65) -> MatchResult:
        """Match a raw label to a canonical company field name."""
        r = self._lookup(raw, self._company_norm, self._company_keys, min_confidence)
        r.raw = raw
        return r

    def expand_zones(self, raw: str) -> List[str]:
        """Convenience: return list of canonical zones from a zone label. Empty = unknown."""
        r = self.match_zone(raw)
        if r.value:
            return r.value
        return []

    # ─── Internal lookup helpers ──────────────────────────────────────────────

    def _lookup(
        self,
        raw: str,
        norm_table: Dict[str, Tuple],
        key_list: List[str],
        min_confidence: float,
    ) -> MatchResult:
        if not raw or not raw.strip():
            return NO_MATCH

        # L1: exact (lowercased)
        exact_low = raw.lower().strip()
        n = _normalise(raw)

        # Check learned first (includes runtime corrections from writable dir)
        if exact_low in self._all_learned_charges:
            return MatchResult(value=self._all_learned_charges[exact_low], confidence=1.0, method="learned")

        # Exact normalised
        if n in norm_table:
            val, _ = norm_table[n]
            return MatchResult(value=val, confidence=1.0, method="exact")

        # L2: strip noise words and try again
        stripped_n = _normalise(_strip_noise(n))
        if stripped_n and stripped_n in norm_table:
            val, _ = norm_table[stripped_n]
            return MatchResult(value=val, confidence=0.95, method="normalised")

        # L3: substring match — does any known key contain/is-contained-by raw?
        for key, (val, _) in norm_table.items():
            if n in key or key in n:
                overlap = len(min(n, key, key=len)) / len(max(n, key, key=len))
                if overlap >= 0.7:
                    return MatchResult(value=val, confidence=0.85, method="substring")

        # L4: token overlap — do enough tokens match?
        raw_tokens = _tokens(n)
        best_overlap = 0.0
        best_val = None
        for key, (val, _) in norm_table.items():
            key_tokens = _tokens(key)
            if not key_tokens:
                continue
            common = raw_tokens & key_tokens
            overlap = len(common) / max(len(raw_tokens), len(key_tokens))
            if overlap > best_overlap:
                best_overlap = overlap
                best_val = val
        if best_overlap >= 0.7:
            return MatchResult(value=best_val, confidence=0.75, method="token")

        # L5: fuzzy string match
        matches = difflib.get_close_matches(n, key_list, n=1, cutoff=max(0.6, min_confidence))
        if matches:
            val, _ = norm_table[matches[0]]
            sim = difflib.SequenceMatcher(None, n, matches[0]).ratio()
            return MatchResult(value=val, confidence=round(sim * 0.9, 3), method="fuzzy")

        return NO_MATCH

    def _lookup_zone(self, raw: str, min_confidence: float) -> MatchResult:
        n = _normalise(raw)
        n_upper = raw.upper().strip()

        # Check learned (includes runtime corrections from writable dir)
        if n_upper in self._all_learned_zones:
            return MatchResult(value=self._all_learned_zones[n_upper], confidence=1.0, method="learned")

        # Exact normalised
        if n in self._zone_norm:
            val, _ = self._zone_norm[n]
            return MatchResult(value=val, confidence=1.0, method="exact")

        # Strip noise
        stripped = _normalise(_strip_noise(n))
        if stripped and stripped in self._zone_norm:
            val, _ = self._zone_norm[stripped]
            return MatchResult(value=val, confidence=0.95, method="normalised")

        # Substring
        for key, (val, _) in self._zone_norm.items():
            if n in key or key in n:
                overlap = len(min(n, key, key=len)) / len(max(n, key, key=len))
                if overlap >= 0.65:
                    return MatchResult(value=val, confidence=0.85, method="substring")

        # Token overlap
        raw_tokens = _tokens(n)
        best_overlap = 0.0
        best_val = None
        for key, (val, _) in self._zone_norm.items():
            common = raw_tokens & _tokens(key)
            overlap = len(common) / max(len(raw_tokens), len(_tokens(key)), 1)
            if overlap > best_overlap:
                best_overlap = overlap
                best_val = val
        if best_overlap >= 0.65 and best_val is not None:
            return MatchResult(value=best_val, confidence=0.78, method="token")

        # Fuzzy
        matches = difflib.get_close_matches(n, self._zone_keys, n=1, cutoff=max(0.58, min_confidence))
        if matches:
            val, _ = self._zone_norm[matches[0]]
            sim = difflib.SequenceMatcher(None, n, matches[0]).ratio()
            return MatchResult(value=val, confidence=round(sim * 0.88, 3), method="fuzzy")

        return NO_MATCH

    def _geo_zone_match(self, raw: str) -> MatchResult:
        """
        Look for state/city names embedded in the label.
        E.g. "Andhra Pradesh route" → ["E2","S2","S3"]
        """
        n = _normalise(raw)
        best_zones = None
        best_len = 0

        for state, zones in STATE_TO_ZONES.items():
            state_n = _normalise(state)
            if state_n in n:
                if len(state_n) > best_len:
                    best_len = len(state_n)
                    best_zones = zones

        if best_zones:
            confidence = min(0.90, 0.60 + best_len * 0.02)
            return MatchResult(value=best_zones, confidence=confidence, method="geo", raw=raw)

        return NO_MATCH

    # ─── Batch helpers ────────────────────────────────────────────────────────

    def classify_header_row(
        self, cells: List[str], min_zone_matches: int = 3
    ) -> Dict:
        """
        Classify all cells in a header row.
        Returns:
            {
                "is_zone_matrix_header": bool,
                "zone_cols": {col_idx: [canonical_zones]},
                "charge_cols": {col_idx: canonical_field},
                "pincode_col": int | None,
            }
        """
        zone_cols: Dict[int, List[str]] = {}
        charge_cols: Dict[int, str] = {}
        pincode_col = None

        from knowledge.dictionary import PINCODE_COL_NAMES

        for idx, cell in enumerate(cells):
            if not cell or not cell.strip():
                continue
            cell_lower = cell.lower().strip()

            # Pincode column?
            if cell_lower in PINCODE_COL_NAMES or "pincode" in cell_lower or "pin code" in cell_lower:
                if pincode_col is None:
                    pincode_col = idx
                continue

            # Zone?
            zone_r = self.match_zone(cell, min_confidence=0.65)
            if zone_r.value:
                zone_cols[idx] = zone_r.value
                continue

            # Charge?
            charge_r = self.match_charge(cell, min_confidence=0.65)
            if charge_r.value is not None:
                charge_cols[idx] = charge_r.value

        return {
            "is_zone_matrix_header": len(zone_cols) >= min_zone_matches,
            "zone_cols": zone_cols,
            "charge_cols": charge_cols,
            "pincode_col": pincode_col,
        }
