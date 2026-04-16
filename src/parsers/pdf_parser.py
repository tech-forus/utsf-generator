"""
PDF Parser — Rich Data Extraction
===================================
Extracts text, tables, charges, company info, and zone data from PDF files.

Pipeline:
  1. pdfplumber (best for structured tables) — preferred
  2. PyPDF2 fallback for plain text extraction
  3. Per-page table extraction + cross-page merge for split tables
  4. Key-value charge extraction via regex patterns (handles proposal docs)
  5. Company info extraction (GST, phone, email, address) from text
  6. Zone matrix detection from extracted tables
  7. Pincode list extraction from dense number pages

Install:
  pip install pdfplumber PyPDF2
"""

import os
import re
from typing import Dict, List, Any, Optional

from parsers.base_parser import BaseParser


# ── Regex patterns for common logistics charge labels ────────────────────────

_CHARGE_PATTERNS = [
    # (canonical_field, regex_pattern)
    ("fuel",          r"fuel\s*(?:surcharge|%|percent)[^\d]*(\d+(?:\.\d+)?)"),
    ("docketCharges", r"(?:docket|doc(?:ument)?)\s*(?:charges?|fee)[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    ("minCharges",    r"min(?:imum)?\s*(?:charges?|freight)[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    ("minWeight",     r"min(?:imum)?\s*(?:chargeable\s*)?weight[^\d]*(\d+(?:\.\d+)?)\s*kg"),
    ("greenTax",      r"green\s*(?:tax|cess|levy)[^\d]*(\d+(?:\.\d+)?)"),
    ("rovCharges_v",  r"r\.?o\.?v\.?[^\d]*(\d+(?:\.\d+)?)\s*%"),
    ("rovCharges_f",  r"r\.?o\.?v\.?[^\d%]*(?:min(?:imum)?)?[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    ("odaCharges_f",  r"o\.?d\.?a\.?[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)\s*(?:per\s*(?:shipment|consignment|docket))?"),
    ("divisor",       r"(?:volumetric\s*divisor|kfactor|k\s*factor|cfactor)[^\d]*(\d+)"),
    ("insuranceCharges_v", r"insurance[^\d]*(\d+(?:\.\d+)?)\s*%"),
]

_COMPANY_PATTERNS = {
    "gstNo":       r'\b(\d{2}[A-Z]{5}\d{4}[A-Z]\d[ZYX]\d)\b',
    "panNo":       r'\bPAN\s*[:\-]?\s*([A-Z]{5}\d{4}[A-Z])\b',
    "contactPhone": r'\b((?:0\d{2,4}[-\s]?\d{6,8}|\+?91[-\s]?\d{10}|\d{10}))\b',
    "contactEmail": r'\b([\w.+-]+@[\w-]+\.[\w.]+)\b',
    "website":      r'\b((?:https?://)?(?:www\.)?[\w-]+\.(?:com|in|co\.in|net|org)(?:/[\w/.-]*)?)\b',
}


class PDFParser(BaseParser):
    SUPPORTED_EXTENSIONS = [".pdf"]

    def parse(self, file_path: str) -> Dict[str, Any]:
        text   = ""
        tables = []

        # ── Try pdfplumber (preferred — best table extraction) ────────────────
        try:
            import pdfplumber
            text, tables = self._parse_pdfplumber(file_path)
        except ImportError:
            # ── Fallback: PyPDF2 for text only ────────────────────────────────
            try:
                import PyPDF2
                text = self._parse_pypdf2(file_path)
            except ImportError:
                print("[PDFParser] Neither pdfplumber nor PyPDF2 available")
                print("  Install: pip install pdfplumber PyPDF2")
        except Exception as e:
            print(f"[PDFParser] pdfplumber error: {e}")
            # Try PyPDF2 as fallback
            try:
                import PyPDF2
                text = self._parse_pypdf2(file_path)
            except Exception:
                pass

        # ── Extract structured data ───────────────────────────────────────────
        data = self._extract_data(text, tables)

        return {
            "text":   text,
            "tables": tables,
            "data":   data,
        }

    # ── Parsers ───────────────────────────────────────────────────────────────

    def _parse_pdfplumber(self, file_path: str):
        """Extract text and tables from all pages using pdfplumber."""
        import pdfplumber

        all_text   = []
        all_tables = []
        prev_table_cols: Optional[int] = None  # for cross-page table merging
        carry_rows: List[List[str]] = []        # rows carried from previous page

        with pdfplumber.open(file_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                # ── Text ──────────────────────────────────────────────────────
                page_text = page.extract_text() or ""
                all_text.append(f"\n--- Page {page_num + 1} ---\n{page_text}")

                # ── Tables ────────────────────────────────────────────────────
                page_tables = page.extract_tables() or []
                for raw_table in page_tables:
                    if not raw_table or len(raw_table) < 2:
                        continue

                    # Normalise: replace None → ""
                    clean = [
                        [str(c).strip() if c is not None else "" for c in row]
                        for row in raw_table
                    ]

                    # Cross-page table merging: if previous page ended mid-table
                    # (same column count, no header row), prepend carry rows.
                    n_cols = max(len(r) for r in clean)
                    if carry_rows and prev_table_cols == n_cols:
                        clean = carry_rows + clean
                        carry_rows = []

                    all_tables.append(clean)
                    prev_table_cols = n_cols

                    # Carry last data rows to next page (may be continued)
                    carry_rows = clean[-2:] if len(clean) > 2 else []

        return "\n".join(all_text), all_tables

    def _parse_pypdf2(self, file_path: str) -> str:
        """Extract plain text from all pages using PyPDF2."""
        import PyPDF2
        parts = []
        with open(file_path, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            for i, page in enumerate(reader.pages):
                parts.append(f"\n--- Page {i+1} ---\n")
                parts.append(page.extract_text() or "")
        return "".join(parts)

    # ── Data extraction ───────────────────────────────────────────────────────

    def _extract_data(self, text: str, tables: List[List[List[str]]]) -> Dict:
        """Build structured data dict from extracted text and tables."""
        from parsers.excel_parser import ExcelParser
        ep = ExcelParser()

        data: Dict = {}

        # ── OICR engine: city-based zone matrix + smart charge extraction ────
        try:
            from parsers.oicr_engine import get_oicr_engine
            oicr = get_oicr_engine()

            # 1. Process PDF text for company info + charges
            oicr_pdf = oicr.process_pdf_text(text, tables)
            if oicr_pdf.get("company_details"):
                data.setdefault("company_details", {}).update(oicr_pdf["company_details"])
            if oicr_pdf.get("charges"):
                data.setdefault("charges", {})
                for k, v in oicr_pdf["charges"].items():
                    data["charges"].setdefault(k, v)
            if oicr_pdf.get("zone_matrix") and not data.get("zone_matrix"):
                data["zone_matrix"] = oicr_pdf["zone_matrix"]
                print(f"[PDFParser] OICR zone matrix: {len(data['zone_matrix'])} origins")

        except Exception as _oicr_err:
            print(f"[PDFParser] OICR pass failed: {_oicr_err}")

        # ── Tables → zone matrix + charges (fallback) ─────────────────────────
        for table in tables:
            if not data.get("zone_matrix"):
                zm = ep._try_parse_zone_matrix(table, "PDF_Table")
                if zm and len(zm) > len(data.get("zone_matrix", {})):
                    data["zone_matrix"] = zm

            ch = ep._try_parse_charges(table, "PDF_Table")
            if ch:
                data.setdefault("charges", {})
                for k, v in ch.items():
                    data["charges"].setdefault(k, v)  # OICR values take priority

            # Pincode list detection
            pincodes = self._extract_pincodes_from_table(table)
            if pincodes:
                data.setdefault("served_pincodes", [])
                data["served_pincodes"].extend(pincodes)

        # ── Text → charges (key-value regex patterns, fallback) ───────────────
        if text:
            text_charges = self._extract_charges_from_text(text)
            if text_charges:
                data.setdefault("charges", {})
                for k, v in text_charges.items():
                    data["charges"].setdefault(k, v)  # don't overwrite OICR

            cd = self._extract_company_from_text(text)
            if cd:
                data.setdefault("company_details", {})
                for k, v in cd.items():
                    data["company_details"].setdefault(k, v)

            # Zone matrix from text tables (tab/space-separated) if still missing
            if not data.get("zone_matrix"):
                text_rows = self._text_to_rows(text)
                zm = ep._try_parse_zone_matrix(text_rows, "PDF_Text")
                if zm:
                    data["zone_matrix"] = zm
                ch = ep._try_parse_charges(text_rows, "PDF_Text")
                if ch:
                    data.setdefault("charges", {})
                    for k, v in ch.items():
                        data["charges"].setdefault(k, v)

            # Pincode list pages
            pincodes = self._extract_pincodes_from_text(text)
            if pincodes:
                data.setdefault("served_pincodes", [])
                data["served_pincodes"].extend(pincodes)

        # Deduplicate pincodes
        if data.get("served_pincodes"):
            data["served_pincodes"] = list(dict.fromkeys(data["served_pincodes"]))

        return data

    # ── Charge extraction from text ───────────────────────────────────────────

    def _extract_charges_from_text(self, text: str) -> Dict:
        """
        Extract charge values from free text (proposal docs, emails, PDFs).
        Uses regex patterns for each canonical charge field.
        """
        lower = text.lower()
        charges: Dict = {}

        for field, pattern in _CHARGE_PATTERNS:
            m = re.search(pattern, lower)
            if not m:
                continue
            try:
                val = float(m.group(1))
            except (ValueError, IndexError):
                continue

            if field == "rovCharges_v":
                charges.setdefault("rovCharges", {})["v"] = val
            elif field == "rovCharges_f":
                charges.setdefault("rovCharges", {})["f"] = val
            elif field == "odaCharges_f":
                oda = charges.setdefault("odaCharges", {})
                oda["f"] = val
                oda.setdefault("type", "per_shipment")
            elif field == "insuranceCharges_v":
                charges["insuranceCharges"] = {"v": val, "f": 0.0}
            else:
                charges[field] = val

        # Also try extracting fuel % expressed differently
        # e.g. "14% Fuel Surcharge" or "Fuel: 14%"
        if "fuel" not in charges:
            fuel_m = re.search(r'(\d+(?:\.\d+)?)\s*%\s*fuel', lower)
            if fuel_m:
                charges["fuel"] = float(fuel_m.group(1))

        return charges

    # ── Company extraction from text ──────────────────────────────────────────

    def _extract_company_from_text(self, text: str) -> Dict:
        """Extract company info (GST, phone, email, PAN, website) via regex."""
        info: Dict = {}
        for field, pattern in _COMPANY_PATTERNS.items():
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                val = m.group(1).strip()
                if field == "contactPhone":
                    val = re.sub(r'[\s\-\+]', '', val)
                    if val.startswith("91") and len(val) == 12:
                        val = val[2:]
                elif field == "gstNo":
                    val = val.upper()
                elif field == "contactEmail":
                    val = val.lower()
                info[field] = val
        return info

    # ── Pincode extraction ────────────────────────────────────────────────────

    def _extract_pincodes_from_table(self, table: List[List[str]]) -> List[int]:
        """Extract 6-digit Indian pincodes from table cells."""
        pincodes: List[int] = []
        for row in table:
            for cell in row:
                for m in re.finditer(r'\b([1-9]\d{5})\b', cell):
                    pin = int(m.group(1))
                    if 100000 <= pin <= 999999:
                        pincodes.append(pin)
        return pincodes

    def _extract_pincodes_from_text(self, text: str) -> List[int]:
        """
        Extract 6-digit pincodes from text pages that are dense pincode lists.
        Only activates if the page has a high density of 6-digit numbers.
        """
        all_matches = re.findall(r'\b([1-9]\d{5})\b', text)
        pincodes = [int(p) for p in all_matches if 100000 <= int(p) <= 999999]

        # Only include if this looks like a pincode list (≥20 pincodes found)
        if len(pincodes) >= 20:
            return pincodes
        return []

    # ── Text to rows ──────────────────────────────────────────────────────────

    def _text_to_rows(self, text: str) -> List[List[str]]:
        """Convert multi-line text into row/cell structure for pattern matching."""
        rows: List[List[str]] = []
        for line in text.split("\n"):
            line = line.strip()
            if not line or line.startswith("---"):
                continue
            # Split on tab, pipe, or 2+ spaces
            parts = re.split(r"\t|\s{2,}|\|", line)
            parts = [p.strip() for p in parts if p.strip()]
            if parts:
                rows.append(parts)
        return rows
