"""
Content Classifier
==================
Sniffs what a block of text or table rows is about, assigning a UTSF data
category before any field-level extraction starts.  Works like spam detection:
keyword density + pattern hits → confidence scores → best category.

Categories (returned as strings):
  ZONE_MATRIX    zone-to-zone rate matrix
  PINCODE_LIST   serviceability / pincode coverage list
  CHARGES        surcharges, fees, tariff lines
  COMPANY_INFO   company name, GST, PAN, contact
  ODA_LIST       out-of-delivery-area pincode data
  WEIGHT_SLAB    weight-band pricing table
  MIXED          multiple types detected (process all)
  UNKNOWN        not enough signal

Usage:
    from knowledge.content_classifier import ContentClassifier
    cc = ContentClassifier()
    result = cc.classify_text(raw_text)
    # result = {"category": "CHARGES", "confidence": 0.87, "signals": {...}}

    result = cc.classify_rows(list_of_rows)
    # same shape
"""

import re
from typing import Dict, List, Tuple, Any

# ---------------------------------------------------------------------------
# Signal keyword sets for each category
# ---------------------------------------------------------------------------

_ZONE_MATRIX_SIGNALS = {
    # Zone code patterns
    "zone_codes":   re.compile(r'\b(N[1-4]|S[1-4]|E[12]|W[12]|C[12]|NE[12]|X[123])\b'),
    # Zone label words
    "zone_words":   re.compile(
        r'\b(zone|north|south|east|west|central|northeast|metro|roi|rest of india|'
        r'regional|corridor|route|lane)\b', re.I),
    # Rate-type words
    "rate_words":   re.compile(
        r'\b(rate|per\s*kg|freight|tariff|price|charge|rs\.?|inr|₹)\b', re.I),
    # Matrix structure: numbers in a grid
    "numeric_grid": re.compile(r'(\d+\.?\d*\s+){3,}'),
}

_PINCODE_SIGNALS = {
    # Dense 6-digit numbers
    "pin_numbers":  re.compile(r'\b([1-9]\d{5})\b'),
    # Serviceability words
    "svc_words":    re.compile(
        r'\b(pincode|serviceable|serviceability|served|coverage|delivery|'
        r'network|area|reachable|pin\s*code|postal)\b', re.I),
    # Y/N flags
    "yn_flags":     re.compile(r'\b(yes|no|y|n|true|false|oda|edl)\b', re.I),
}

_CHARGES_SIGNALS = {
    "charge_words": re.compile(
        r'\b(fuel|docket|minimum|surcharge|charges?|fee|rov|fov|oda|cod|'
        r'handling|insurance|divisor|green\s*tax|topay|prepaid|dacc|'
        r'bilty|lr\b|dsc|fsc|hsd|levy|cess|misc|octroi)\b', re.I),
    "charge_values": re.compile(
        r'\b(\d+(?:\.\d+)?)\s*(%|per\s*kg|rs\.?|inr|₹|/-)\b', re.I),
    "key_value":    re.compile(r'[:\-=]\s*(\d+(?:\.\d+)?)\s*(%|rs)?', re.I),
}

_COMPANY_SIGNALS = {
    "gst_pattern":  re.compile(r'\b\d{2}[A-Z]{5}\d{4}[A-Z]\d[ZYX]\d\b'),
    "pan_pattern":  re.compile(r'\b[A-Z]{5}\d{4}[A-Z]\b'),
    "phone_pattern":re.compile(r'\b(\+?91[-\s]?)?\d{10}\b'),
    "email_pattern":re.compile(r'\b[\w.+-]+@[\w-]+\.\w+\b'),
    "company_words":re.compile(
        r'\b(pvt\.?\s*ltd|limited|llp|incorporated|company|transporter|'
        r'logistics|courier|cargo|express|freight|gst\s*no|cin\b|pan\b|'
        r'registered|head\s*office|branch)\b', re.I),
}

_ODA_SIGNALS = {
    "oda_words":    re.compile(
        r'\b(oda|out\s*of\s*delivery|edl|extended\s*delivery|remote\s*area|'
        r'special\s*area|restricted|non.?serviceable)\b', re.I),
    "pin_numbers":  re.compile(r'\b([1-9]\d{5})\b'),
}

_WEIGHT_SLAB_SIGNALS = {
    "weight_ranges":re.compile(
        r'\b(\d+\s*[-–]\s*\d+\s*kg|\d+\s*kg\s*(?:to|[-–])\s*\d+\s*kg|'
        r'up\s*to\s*\d+\s*kg|above\s*\d+\s*kg|\d+\+\s*kg)\b', re.I),
    "weight_words": re.compile(
        r'\b(weight|wt\.?|slab|kg\b|kgs\b|per\s*kg|minimum\s*weight|'
        r'chargeable\s*weight)\b', re.I),
    "per_kg_rates": re.compile(r'\b\d+(?:\.\d+)?\s*/?\s*kg\b', re.I),
}

# ---------------------------------------------------------------------------
# Scoring weights: how much each signal type contributes
# ---------------------------------------------------------------------------

_WEIGHTS = {
    "ZONE_MATRIX": {
        "zone_codes":    3.0,   # strongest signal
        "zone_words":    1.0,
        "rate_words":    0.8,
        "numeric_grid":  0.5,
    },
    "PINCODE_LIST": {
        "pin_numbers":   2.0,   # count of 6-digit numbers matters
        "svc_words":     1.5,
        "yn_flags":      0.5,
    },
    "CHARGES": {
        "charge_words":  2.5,
        "charge_values": 1.5,
        "key_value":     0.8,
    },
    "COMPANY_INFO": {
        "gst_pattern":   4.0,   # regex hit = very strong
        "pan_pattern":   3.5,
        "phone_pattern": 2.0,
        "email_pattern": 2.0,
        "company_words": 1.0,
    },
    "ODA_LIST": {
        "oda_words":     3.5,
        "pin_numbers":   1.5,
    },
    "WEIGHT_SLAB": {
        "weight_ranges": 3.5,
        "weight_words":  1.5,
        "per_kg_rates":  2.0,
    },
}

# Minimum raw score for a category to be considered
_MIN_SCORE = 1.5
# Gap between top-2 categories to call it MIXED
_MIXED_GAP_THRESHOLD = 0.35


# ---------------------------------------------------------------------------
# Classifier class
# ---------------------------------------------------------------------------

class ContentClassifier:
    """
    Fast, stateless content sniffer.  Build once, call many times.
    """

    def classify_text(self, text: str) -> Dict[str, Any]:
        """
        Classify a raw text string.

        Returns:
            {
                "category":   str,          # e.g. "CHARGES"
                "confidence": float,        # 0.0 – 1.0
                "scores":     dict,         # raw score per category
                "signals":    dict,         # what triggered each category
            }
        """
        if not text or not text.strip():
            return {"category": "UNKNOWN", "confidence": 0.0, "scores": {}, "signals": {}}

        return self._score(text, mode="text")

    def classify_rows(self, rows: List[List[str]]) -> Dict[str, Any]:
        """
        Classify a table (list of rows, each row is list of cell strings).
        """
        if not rows:
            return {"category": "UNKNOWN", "confidence": 0.0, "scores": {}, "signals": {}}

        flat = " ".join(
            " ".join(str(c) for c in row if c)
            for row in rows
        )
        return self._score(flat, mode="table", rows=rows)

    def classify_file_hint(self, filename: str) -> str:
        """
        Quick category hint from filename alone.
        Returns category string or "UNKNOWN".
        """
        name = filename.lower()
        if any(k in name for k in ("rate", "matrix", "zone", "tariff", "price")):
            return "ZONE_MATRIX"
        if any(k in name for k in ("pincode", "serviceable", "coverage", "network", "area")):
            return "PINCODE_LIST"
        if any(k in name for k in ("charge", "fee", "surcharge", "oda", "misc")):
            return "CHARGES"
        if any(k in name for k in ("company", "vendor", "profile", "gst", "pan", "kyc", "info")):
            return "COMPANY_INFO"
        return "UNKNOWN"

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _score(self, text: str, mode: str = "text", rows: List = None) -> Dict[str, Any]:
        scores: Dict[str, float] = {}
        signals: Dict[str, Dict] = {}

        for category, pattern_weights in _WEIGHTS.items():
            cat_score = 0.0
            cat_signals: Dict[str, int] = {}

            for signal_name, weight in pattern_weights.items():
                pattern = self._get_pattern(category, signal_name)
                if pattern is None:
                    continue
                hits = pattern.findall(text)
                n = len(hits)
                if n == 0:
                    continue

                # For count-based signals, use sqrt to dampen large lists
                import math
                if signal_name in ("pin_numbers", "zone_codes"):
                    # Count unique matches
                    unique = len(set(h if isinstance(h, str) else h[0] for h in hits))
                    # Strong bonus for density: ≥20 = very likely this category
                    density_bonus = min(3.0, math.log10(unique + 1) * 2.0)
                    cat_score += weight * density_bonus
                    cat_signals[signal_name] = unique
                else:
                    cat_score += weight * min(n, 5)   # cap at 5 hits
                    cat_signals[signal_name] = n

            scores[category] = round(cat_score, 3)
            if cat_signals:
                signals[category] = cat_signals

        # Extra: if rows contain a zone matrix structure, boost ZONE_MATRIX
        if rows:
            zm_bonus = self._zone_matrix_structure_bonus(rows)
            scores["ZONE_MATRIX"] = scores.get("ZONE_MATRIX", 0) + zm_bonus

        # Determine winner
        if not scores or max(scores.values()) < _MIN_SCORE:
            return {"category": "UNKNOWN", "confidence": 0.0, "scores": scores, "signals": signals}

        sorted_cats = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        top_cat, top_score = sorted_cats[0]
        second_score = sorted_cats[1][1] if len(sorted_cats) > 1 else 0

        total = sum(max(s, 0) for s in scores.values())
        confidence = round(top_score / total, 3) if total > 0 else 0.0

        # MIXED: top two categories within 35% of each other and both strong
        if (second_score >= _MIN_SCORE and
                top_score > 0 and
                (top_score - second_score) / top_score < _MIXED_GAP_THRESHOLD):
            category = "MIXED"
        else:
            category = top_cat

        return {
            "category":   category,
            "confidence": confidence,
            "scores":     scores,
            "signals":    signals,
        }

    def _get_pattern(self, category: str, signal_name: str):
        mapping = {
            "ZONE_MATRIX":  _ZONE_MATRIX_SIGNALS,
            "PINCODE_LIST": _PINCODE_SIGNALS,
            "CHARGES":      _CHARGES_SIGNALS,
            "COMPANY_INFO": _COMPANY_SIGNALS,
            "ODA_LIST":     _ODA_SIGNALS,
            "WEIGHT_SLAB":  _WEIGHT_SLAB_SIGNALS,
        }
        return mapping.get(category, {}).get(signal_name)

    def _zone_matrix_structure_bonus(self, rows: List[List[str]]) -> float:
        """
        Give a structural bonus if rows look like a numeric grid with zone headers.
        """
        if len(rows) < 3:
            return 0.0
        # Check if any of the first 10 rows has 3+ zone-code-like headers
        zone_re = re.compile(r'^(N[1-4]|S[1-4]|E[12]|W[12]|C[12]|NE[12]|X[123])$', re.I)
        for row in rows[:10]:
            zone_hits = sum(1 for cell in row if zone_re.match(str(cell).strip()))
            if zone_hits >= 3:
                return 2.5  # strong structure bonus
        return 0.0
