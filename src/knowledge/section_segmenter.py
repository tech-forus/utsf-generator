"""
Section Segmenter
=================
Splits a PDF text blob or list of Excel rows into named semantic sections
BEFORE field-level extraction runs.

Why this exists
---------------
The original pipeline ran all charge regexes against the full concatenated
page text.  A "1 CFT = 1728 cubic inches" line on page 3 could bleed into
a docketCharges regex that started matching on page 1 — because the regex
engine sees one flat string with no section boundaries.

This module solves that by:
  1. Detecting section boundaries (headings, horizontal rules, blank clusters)
  2. Labelling each section with a ContentClassifier category
  3. Returning sections in a structured form so callers can gate which
     extraction patterns run against which text

It intentionally reuses ContentClassifier (existing) as the scorer
and DocumentContext (existing) as the running state carrier.
It does NOT replace them — it wraps them with positional awareness.

Section types emitted (same vocabulary as ContentClassifier):
  CHARGES       surcharge table, fee schedule, rate card appendix
  ZONE_MATRIX   zone-to-zone pricing grid
  PINCODE_LIST  serviceability / coverage / ODA pincode lists
  COMPANY_INFO  header block with GST / PAN / contact
  ODA_LIST      ODA rate table (distance/weight matrix)
  WEIGHT_SLAB   weight-band pricing
  VOLUMETRIC    dimensional weight explanation (divisor / CFT formula)
  LEGAL         T&C, disclaimer, limitation of liability
  UNKNOWN       cannot classify confidently

Usage (PDF)
-----------
    from knowledge.section_segmenter import SectionSegmenter
    seg = SectionSegmenter()
    sections = seg.segment_text(page_text)
    charges_text = seg.extract_category_text(sections, "CHARGES")

Usage (Excel rows)
------------------
    sections = seg.segment_rows(all_rows)
    charge_rows = seg.extract_category_rows(sections, "CHARGES")
"""

import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Tuple

from knowledge.content_classifier import ContentClassifier


# ── Hard heading patterns ─────────────────────────────────────────────────────
# These are strong boundary signals — a line matching one of these almost
# certainly starts a new section regardless of what follows.

_HEADING_PATTERNS: List[Tuple[str, str]] = [
    # Category                 Pattern (case-insensitive)
    # AIR_FREIGHT must come before CHARGES/ZONE_MATRIX so "Air Freight Rates" doesn't
    # accidentally match the ZONE_MATRIX "freight rate" pattern.
    ("AIR_FREIGHT",     r'(?i)^[\s\*\-=>#]*(?:air\s*(?:freight|cargo|express|rates?|tariff|'
                        r'charges?|price|mode|service|zone|only)|'
                        r'aviation\s*(?:freight|cargo|rates?)|'
                        r'airway?\s*bill|awb\s*(?:rates?|charges?)|'
                        r'by\s*air(?:\s*(?:rates?|charges?|tariff))?)[\s\*:\-]*$'),
    ("CHARGES",         r'(?i)^[\s\*\-=>#]*(?:surcharge|additional\s*charge|fee\s*schedule|'
                        r'charge\s*details?|tariff|accessorial|ancillary|levy|misc|'
                        r'docket|lr\s*charge|fuel\s*surcharge|oda\s*charge|'
                        r'handling\s*charge|cod\s*charge|freight\s*charges?)[\s\*:\-]*$'),
    ("ZONE_MATRIX",     r'(?i)^[\s\*\-=>#]*(?:zone(?:\s*(?:wise|to\s*zone|matrix|'
                        r'rate|price|tariff))?|rate\s*(?:card|matrix|chart)|'
                        r'price\s*(?:matrix|chart)|freight\s*rate)[\s\*:\-]*$'),
    # TRANSIT_DAYS must come before ZONE_MATRIX/RATE rules to win on ambiguous headings
    ("TRANSIT_DAYS",    r'(?i)^[\s\*\-=>#]*(?:transit\s*(?:time|days?)|'
                        r'delivery\s*(?:time|days?)|tat|eta|lead\s*time|'
                        r'turnaround\s*time|expected\s*(?:days?|delivery)|'
                        r'days?\s*(?:matrix|chart|schedule)|no\.?\s*of\s*days?)[\s\*:\-]*$'),
    ("ODA_LIST",        r'(?i)^[\s\*\-=>#]*(?:oda|out[\s-]*of[\s-]*delivery|edl|'
                        r'extended\s*delivery|remote\s*area|special\s*area|'
                        r'non[\s-]*serviceable)[\s\*:\-]*$'),
    ("PINCODE_LIST",    r'(?i)^[\s\*\-=>#]*(?:pincode|serviceable|coverage|network|'
                        r'served\s*area|delivery\s*network|service\s*area)[\s\*:\-]*$'),
    ("COMPANY_INFO",    r'(?i)^[\s\*\-=>#]*(?:company|vendor|transporter|about\s*us|'
                        r'company\s*profile|contact|registration|gst|kyc)[\s\*:\-]*$'),
    ("VOLUMETRIC",      r'(?i)^[\s\*\-=>#]*(?:volumetric|dimensional\s*weight|'
                        r'cft|cubic|k[\s-]*factor|divisor)[\s\*:\-]*$'),
    ("LEGAL",           r'(?i)^[\s\*\-=>#]*(?:terms?\s*(?:and|&)\s*conditions?|'
                        r'disclaimer|limitation|liability|t\s*&\s*c|'
                        r'general\s*conditions?)[\s\*:\-]*$'),
    ("WEIGHT_SLAB",     r'(?i)^[\s\*\-=>#]*(?:weight[\s-]*(?:slab|band|range|bracket)|'
                        r'slab[\s-]*rate|rate[\s-]*slab|per[\s-]*kg[\s-]*rate)[\s\*:\-]*$'),
]

_HEADING_RE = [(cat, re.compile(pat)) for cat, pat in _HEADING_PATTERNS]

# Inline section markers (occur mid-text, usually after a divider line)
_INLINE_MARKERS: List[Tuple[str, re.Pattern]] = [
    # TRANSIT_DAYS: only the compound phrases are strong enough for inline promotion.
    # Plain "N days" is too common (payment terms, credit days) so we require it to
    # be qualified: "N transit days", "N business days in transit", or TAT/ETA header.
    ("TRANSIT_DAYS", re.compile(
        r'(?i)(?:'
        r'\b\d+\s*transit\s*days?\b'               # "10 transit days"
        r'|\btransit\s*(?:time|days?)\b'           # "transit time", "transit days"
        r'|\bdelivery\s*(?:time|days?)\b'          # "delivery time", "delivery days"
        r'|\bdays?\s*in\s*transit\b'               # "days in transit"
        r'|(?<!\w)(?:tat|eta)(?!\w)'               # standalone TAT / ETA column header
        r'|\blead\s*time\b'                        # "lead time"
        r'|\bturnaround\s*time\b'                  # "turnaround time"
        r')',
    )),
    ("AIR_FREIGHT", re.compile(
        r'(?i)\b(?:air\s*(?:freight|cargo|way\s*bill|awb|mode)|awb|airway\s*bill|by\s*air\s*(?:rate|freight|tariff))',
    )),
    ("VOLUMETRIC",  re.compile(
        r'(?i)\b(?:1\s*cft\s*[=:]\s*\d+|k[\s-]*factor\s*[=:]\s*\d+|'
        r'volumetric\s*divisor\s*[=:]\s*\d+|dimensional\s*weight\s*formula)',
    )),
    ("LEGAL",       re.compile(
        r'(?i)\b(?:subject\s*to|governed\s*by|clause\s*\d+|notwithstanding|'
        r'indemnif|arbitration|jurisdiction)',
    )),
    ("CHARGES",     re.compile(
        r'(?i)\b(?:docket\s*charge|lr\s*charge|fuel\s*surcharge|oda\s*charge|'
        r'cod\s*charge|dacc|topay\s*charge|handling\s*charge|'
        r'green\s*tax|miscellaneous\s*charge)',
    )),
    ("COMPANY_INFO", re.compile(
        r'(?i)\b(?:gstin?|gst\s*no|pan\s*(?:no|number|card)|cin\s*no|'
        r'registered\s*office|head\s*office|contact\s*(?:us|no|number))',
    )),
]

# Lines that are almost certainly section dividers (blank, rule-only, page markers)
_DIVIDER_RE = re.compile(
    r'^[\s\-=_*#~.]{3,}$|'           # horizontal rules
    r'^---\s*Page\s+\d+\s*---$|'     # pdfplumber page markers
    r'^\s*$',                          # blank lines
    re.M
)


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class TextSection:
    """One named section of a PDF text document."""
    category:   str               # ContentClassifier category
    confidence: float             # 0.0–1.0
    text:       str               # raw text content
    start_line: int               # 0-based line index in original text
    end_line:   int
    heading:    str = ""          # detected heading line if any
    signals:    Dict = field(default_factory=dict)


@dataclass
class RowSection:
    """One named section of an Excel sheet (contiguous row block)."""
    category:   str
    confidence: float
    rows:       List[List[str]]
    start_row:  int
    end_row:    int
    heading:    str = ""


# ── Segmenter ─────────────────────────────────────────────────────────────────

class SectionSegmenter:
    """
    Splits raw text or Excel rows into typed sections.
    Uses ContentClassifier for scoring and heading regexes for boundary detection.

    Design principle: prefer to over-split (too many sections, some UNKNOWN)
    rather than under-split (merge a VOLUMETRIC section into CHARGES).
    Downstream extractors skip UNKNOWN/LEGAL/VOLUMETRIC sections automatically.
    """

    def __init__(self):
        self._cc = ContentClassifier()
        self._min_section_chars = 40   # ignore sections shorter than this
        self._min_section_rows  = 1

    # ── PDF text segmentation ──────────────────────────────────────────────────

    def segment_text(self, text: str) -> List[TextSection]:
        """
        Split a multi-page PDF text string into typed sections.

        Algorithm:
          1. Split into lines
          2. Identify boundary lines (blank clusters, headings, page markers)
          3. Group lines into candidate sections
          4. Classify each candidate using ContentClassifier + heading match
          5. Merge adjacent same-category sections (avoid over-fragmentation)
          6. Return list of TextSection objects
        """
        if not text:
            return []

        lines = text.splitlines()
        boundary_indices = self._find_boundaries(lines)

        # Build candidate text blocks between boundaries
        candidates: List[Tuple[int, int]] = []
        prev = 0
        for bi in sorted(set(boundary_indices)):
            if bi > prev:
                candidates.append((prev, bi))
            prev = bi + 1
        if prev < len(lines):
            candidates.append((prev, len(lines)))

        sections: List[TextSection] = []
        for start, end in candidates:
            block_lines = lines[start:end]
            block_text  = "\n".join(block_lines).strip()
            if not block_text or len(block_text) < self._min_section_chars:
                continue

            heading, forced_cat = self._detect_heading(block_lines)
            if forced_cat:
                cat, conf = forced_cat, 0.95
                signals   = {"heading_match": heading}
            else:
                # Try inline marker on first 300 chars of block
                inline_cat = self._detect_inline_marker(block_text[:300])
                if inline_cat and inline_cat in ("VOLUMETRIC", "LEGAL"):
                    cat, conf, signals = inline_cat, 0.88, {"inline_marker": True}
                else:
                    result  = self._cc.classify_text(block_text[:2000])
                    cat     = result["category"]
                    conf    = result["confidence"]
                    signals = result.get("signals", {})

                    # Inline marker can promote/override low-confidence classification
                    if inline_cat and conf < 0.55:
                        cat, conf = inline_cat, 0.75

            sections.append(TextSection(
                category=cat, confidence=conf,
                text=block_text,
                start_line=start, end_line=end,
                heading=heading, signals=signals,
            ))

        # Merge adjacent same-category sections (prevents micro-sections)
        return self._merge_text_sections(sections)

    def extract_category_text(
        self, sections: List[TextSection], category: str,
        min_confidence: float = 0.4
    ) -> str:
        """Return concatenated text for all sections of a given category."""
        parts = [
            s.text for s in sections
            if s.category == category and s.confidence >= min_confidence
        ]
        return "\n\n".join(parts)

    def get_sections_map(self, sections: List[TextSection]) -> Dict[str, List[TextSection]]:
        """Return {category: [sections]} for all categories present."""
        result: Dict[str, List[TextSection]] = {}
        for s in sections:
            result.setdefault(s.category, []).append(s)
        return result

    # ── Excel row segmentation ─────────────────────────────────────────────────

    def segment_rows(self, rows: List[List[str]], sheet_name: str = "") -> List[RowSection]:
        """
        Split a flat list of Excel rows into typed sections.

        Uses a sliding window: classify every W rows, emit a new section when
        the top category changes (with hysteresis to avoid flip-flopping on
        ambiguous rows).
        """
        if not rows:
            return []

        WINDOW     = 6     # rows to classify at once
        HYSTERESIS = 3     # consecutive windows needed to confirm category change

        sections: List[RowSection] = []
        current_cat  = "UNKNOWN"
        current_conf = 0.0
        current_start = 0
        pending_cat   = None
        pending_count = 0

        def _flush(end_row: int):
            nonlocal current_cat, current_start
            block = rows[current_start:end_row]
            if len(block) >= self._min_section_rows:
                heading = self._row_heading(block[:3])
                sections.append(RowSection(
                    category=current_cat, confidence=current_conf,
                    rows=block,
                    start_row=current_start, end_row=end_row,
                    heading=heading,
                ))
            current_start = end_row

        for i in range(0, len(rows), WINDOW // 2):
            window_rows = rows[i: i + WINDOW]
            flat = " ".join(
                " ".join(str(c) for c in row if c)
                for row in window_rows
            )
            result = self._cc.classify_text(flat[:1500])
            win_cat  = result["category"]
            win_conf = result["confidence"]

            # Forced heading detection on first row of window
            heading_row = window_rows[0] if window_rows else []
            heading_line = " ".join(str(c) for c in heading_row if c).strip()
            _, forced = self._detect_heading([heading_line])
            if forced:
                win_cat  = forced
                win_conf = 0.95

            if win_cat != current_cat and win_conf > 0.35:
                if win_cat == pending_cat:
                    pending_count += 1
                else:
                    pending_cat   = win_cat
                    pending_count = 1

                if pending_count >= HYSTERESIS:
                    _flush(i)
                    current_cat  = win_cat
                    current_conf = win_conf
                    pending_cat  = None
                    pending_count = 0
            else:
                pending_cat   = None
                pending_count = 0
                if win_conf > current_conf:
                    current_conf = win_conf  # update confidence upward

        _flush(len(rows))
        return sections

    def extract_category_rows(
        self, sections: List[RowSection], category: str,
        min_confidence: float = 0.35
    ) -> List[List[str]]:
        """Return all rows from sections of a given category."""
        result = []
        for s in sections:
            if s.category == category and s.confidence >= min_confidence:
                result.extend(s.rows)
        return result

    def is_dangerous_section(self, section: TextSection) -> bool:
        """
        Returns True if this section should be SKIPPED for charge extraction.
        Dangerous sections are where ghost values originate:
          VOLUMETRIC    — contains divisor constants (1728, 5000) that bleed into charge fields
          LEGAL         — contains number references that are clause numbers, not charges
          TRANSIT_DAYS  — contains day counts (1-30) that look identical to per-kg rates
          AIR_FREIGHT   — contains air-mode rates that must never be used as road/surface rates
        """
        return section.category in ("VOLUMETRIC", "LEGAL", "TRANSIT_DAYS", "AIR_FREIGHT")

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _find_boundaries(self, lines: List[str]) -> List[int]:
        """Find line indices that are likely section boundaries."""
        boundaries = []
        blank_run = 0

        for i, line in enumerate(lines):
            stripped = line.strip()

            # Page markers (pdfplumber inserts these)
            if re.match(r'^---\s*Page\s+\d+', stripped, re.I):
                boundaries.append(i)
                blank_run = 0
                continue

            # Horizontal rules
            if re.match(r'^[\-=_*~.]{4,}\s*$', stripped):
                boundaries.append(i)
                blank_run = 0
                continue

            # Blank line counting
            if not stripped:
                blank_run += 1
                if blank_run >= 2:
                    boundaries.append(i)
            else:
                blank_run = 0

            # Heading lines (ALL CAPS short lines, or known heading patterns)
            if (len(stripped) < 80 and stripped == stripped.upper()
                    and len(stripped.split()) >= 2
                    and not re.match(r'^[\d.,]+$', stripped)):
                boundaries.append(i)

        return boundaries

    def _detect_heading(self, lines: List[str]) -> Tuple[str, Optional[str]]:
        """
        Check if any of the first 3 lines is a known section heading.
        Returns (heading_text, category) or ("", None).
        """
        for line in lines[:3]:
            stripped = line.strip()
            if not stripped or len(stripped) > 120:
                continue
            for cat, pattern in _HEADING_RE:
                if pattern.match(stripped):
                    return stripped, cat
        return "", None

    def _detect_inline_marker(self, text: str) -> Optional[str]:
        """Check for inline section marker patterns in a text block."""
        for cat, pattern in _INLINE_MARKERS:
            if pattern.search(text):
                return cat
        return None

    def _row_heading(self, rows: List[List[str]]) -> str:
        """Extract a heading label from the first non-empty row in a block."""
        for row in rows:
            cells = [str(c).strip() for c in row if str(c).strip()]
            if cells:
                return " | ".join(cells[:3])
        return ""

    def _merge_text_sections(self, sections: List[TextSection]) -> List[TextSection]:
        """
        Merge adjacent sections with the same category to avoid
        micro-sections (e.g. two consecutive CHARGES paragraphs).
        UNKNOWN sections are kept separate (don't absorb into neighbours).
        """
        if not sections:
            return []
        merged = [sections[0]]
        for s in sections[1:]:
            last = merged[-1]
            if (s.category == last.category
                    and s.category != "UNKNOWN"
                    and s.confidence > 0.3
                    and last.confidence > 0.3):
                # Merge into last
                merged[-1] = TextSection(
                    category=last.category,
                    confidence=max(last.confidence, s.confidence),
                    text=last.text + "\n\n" + s.text,
                    start_line=last.start_line,
                    end_line=s.end_line,
                    heading=last.heading or s.heading,
                    signals={**last.signals, **s.signals},
                )
            else:
                merged.append(s)
        return merged
