"""
PDF Parser — Maximum Extraction with OCR Fallback
===================================================
Pipeline (in order, stops when text is found):
  1. pdfplumber        — best for structured/digital PDFs with tables
  2. pymupdf (fitz)    — handles more PDF variants, also does page rendering
  3. pypdfium2         — another robust fallback
  4. PyPDF2 / pypdf    — plain text, last text-layer attempt
  5. pytesseract OCR   — for scanned / image-based PDFs (prepare for the worst)
  6. easyocr           — second OCR engine if tesseract is unavailable

After text/tables are extracted:
  - ContentClassifier sniffs each page/section into a UTSF category
  - OICR engine handles city-based station rates
  - Excel parser methods handle structured tables
  - Regex patterns cover free-text charge/company mentions
  - Passive learning: successful extractions are auto-confirmed in learning_data.json
"""

import os
import re
import io
import json
import time
from typing import Dict, List, Any, Optional, Tuple

from parsers.base_parser import BaseParser


# ── Regex patterns for charge extraction from free text ──────────────────────
# Expanded from 10 to 60+ patterns matching all canonical UTSF charge fields

# Sanity caps for fixed charge fields — values above these are almost certainly
# parsing artefacts (e.g. volumetric constant 1728 = 1 CFT in cubic inches).
_CHARGE_SANITY_MAX: Dict[str, float] = {
    "docketCharges":      2000,   # per-LR fee; real world: ₹50–₹500
    "daccCharges":        10000,  # demurrage; real world: ₹100–₹2000
    "codCharges":         5000,   # COD fixed; real world: ₹0–₹500
    "handlingCharges":    50000,
    "greenTax":           2000,
    "minCharges":         50000,
    "odaCharges":         50000,
    "topayCharges":       5000,
    "appointmentCharges": 10000,
}

# All [^\d]* spans are capped at 60 chars to prevent cross-line/cross-section
# matching.  The pattern [^\d\n]{0,60} is the bounded version.
# Charges that legitimately appear on the same line as their label use _NL
# (no-newline boundary).  Charges that may be on the NEXT line use _ANY.
_NL  = r"[^\d\n]{0,60}"       # same line, up to 60 non-digit chars
_ANY = r"[^\d]{0,60}"         # allows one newline span

_CHARGE_PATTERNS = [
    # ── Fuel ─────────────────────────────────────────────────────────────────
    ("fuel",            rf"fuel\s*(?:surcharge|levy|%|percent|surcahrge){_NL}(\d+(?:\.\d+)?)"),
    ("fuel",            r"f\.?s\.?c\.?\s*[:\-=]?\s*(\d+(?:\.\d+)?)\s*%"),
    ("fuel",            r"(\d+(?:\.\d+)?)\s*%\s*fuel"),
    ("fuel",            rf"hsd\s*(?:surcharge)?{_NL}(\d+(?:\.\d+)?)\s*%"),
    ("fuel",            rf"diesel\s*(?:surcharge|levy){_NL}(\d+(?:\.\d+)?)\s*%"),
    ("fuel",            rf"diesel\s*escalation[^\d\n]{{0,20}}(\d+(?:\.\d+)?)"),  # TCI "Diesel Escalation" table

    # ── Docket / LR / DWB (TCI uses DWB = Docket Waybill) ────────────────────
    ("docketCharges",   rf"(?:docket|lr\b|bilty|dwb)\s*(?:charges?|fee)?{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    ("docketCharges",   rf"lorry\s*receipt{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    ("docketCharges",   rf"waybill\s*(?:charges?|fee)?{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    ("docketCharges",   rf"consignment\s*note{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),

    # ── GST % (priceRate.gst) ─────────────────────────────────────────────────
    # All patterns REQUIRE % sign to prevent matching the state-code prefix of
    # a GSTIN (e.g. "GSTIN: 29AAACR..." → "29" must NOT become gst=29)
    ("gst",             r"gst\s*[:\-=@]\s*(\d+(?:\.\d+)?)\s*%"),
    ("gst",             r"(\d+(?:\.\d+)?)\s*%\s*gst\b"),
    ("gst",             r"goods\s*and\s*services\s*tax[^\d\n]{0,30}(\d+(?:\.\d+)?)\s*%"),
    ("gst",             r"igst\s*[:\-=@]\s*(\d+(?:\.\d+)?)\s*%"),
    # "GST at Current rate is @ 18% for Surface..."
    ("gst",             rf"gst{_NL}@\s*(\d+(?:\.\d+)?)\s*%"),
    ("gst",             rf"gst\s+at\s+current\s+rate[^\d\n]{{0,30}}@?\s*(\d+(?:\.\d+)?)\s*%"),
    # "all charges subject to GST @ 18%" / "GST: 18% applicable"
    ("gst",             rf"gst\s*[:\-]?\s*(\d+(?:\.\d+)?)\s*%\s*(?:applicable|as\s+per|on)"),
    ("gst",             r"all\s*rates?\s*(?:are\s*)?subject\s*to\s*gst\s*@\s*(\d+(?:\.\d+)?)\s*%"),

    # ── Minimum charges — require currency context to avoid matching min weight
    ("minCharges",      rf"min(?:imum)?\s*(?:charg(?:es?|able)|freight){_NL}(?:rs\.?|₹|inr)\s*(\d+(?:\.\d+)?)"),
    ("minCharges",      rf"sfc\s*[-:]\s*rs[-\.]\s*(\d+(?:\.\d+)?)"),       # TCI "SFC-Rs-350"
    ("minCharges",      rf"min(?:imum)?\s*(?:billing|amount|rate){_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    ("minCharges",      rf"base\s*(?:freight|rate|charge){_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    ("minCharges",      rf"floor\s*(?:rate|charge){_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),

    # ── Min weight — must end with kg unit ────────────────────────────────────
    ("minWeight",       rf"min(?:imum)?\s*(?:chargeable\s*)?weight{_NL}(\d+(?:\.\d+)?)\s*kg"),
    # Flexible middle: "min. charg. weight: 30 kg", "min. wt. chargeable: 30 kg"
    ("minWeight",       rf"min[^\d\n]{{0,40}}weight[^\d\n]{{0,10}}(\d+(?:\.\d+)?)\s*kg"),
    ("minWeight",       rf"min(?:imum)?\s*wt\.?{_NL}(\d+(?:\.\d+)?)\s*kg"),
    ("minWeight",       rf"sfc\s*[-:]\s*(\d+(?:\.\d+)?)\s*kg"),

    # ── Divisor / kFactor ─────────────────────────────────────────────────────
    # "1CFT=Xkg" → HIGHEST PRIORITY: convert to cm³/kg → 28316/X
    # Must come FIRST so SFC divisor (from 1CFT=10kg → 2832) wins over
    # the AIR divisor (from L*B*H/5000) when both appear in the same table.
    ("_cft_kg",         r"1\s*cft\s*[=:]\s*(\d+(?:\.\d+)?)\s*kg"),
    # Standard explicit divisor label
    ("divisor",         r"(?:volumetric\s*divisor|k\s*factor|kfactor|cfactor|vol\s*divisor)[^\d\n]{0,30}(\d+)"),
    # "L*B*H(in CM)/5000" — lower priority than 1CFT formula
    ("divisor",         r"l\s*[x\*]\s*b\s*[x\*]\s*h\s*\(in\s*cm\)[^\d\n]{0,15}/\s*(\d+)"),

    # ── Green tax / NGT / Environmental ───────────────────────────────────────
    ("greenTax",        rf"green\s*(?:tax|cess|levy|surcharge){_NL}(\d+(?:\.\d+)?)"),
    ("greenTax",        rf"ngt\s*charge{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),  # TCI "NGT Charge Delhi/NCR"
    ("greenTax",        rf"environmental\s*(?:surcharge|charge|cess){_NL}(\d+(?:\.\d+)?)"),
    ("greenTax",        rf"ecology\s*(?:charge|cess){_NL}(\d+(?:\.\d+)?)"),
    ("greenTax",        rf"pollution\s*(?:charge|cess){_NL}(\d+(?:\.\d+)?)"),

    # ── ROV / FOV ─────────────────────────────────────────────────────────────
    ("rovCharges_v",    rf"(?:r\.?o\.?v\.?|f\.?o\.?v\.?|risk\s*(?:of\s*value)?|owner.?s?\s*risk){_NL}(\d+(?:\.\d+)?)\s*%"),
    ("rovCharges_f",    rf"(?:r\.?o\.?v\.?|f\.?o\.?v\.?)\s*(?:min(?:imum)?)?{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)(?!\s*%)"),

    # ── ODA ───────────────────────────────────────────────────────────────────
    # Dual-rate "higher of": "Rs. X/Kg or Rs. Y/consignment (higher)" → per_kg_minimum
    # Meaning: ODA = max(X × weight_in_kg, Y). Must match before the single-value patterns.
    ("odaCharges_dual",
     rf"o\.?d\.?a\.?\s*(?:charges?)?{_NL}"
     rf"(?:rs\.?\s*|₹\s*)?(\d+(?:\.\d+)?)\s*/?\s*kg"
     rf"[^\d\n]{{0,40}}"
     rf"(?:rs\.?\s*|₹\s*)?(\d+(?:\.\d+)?)\s*/?\s*(?:consignment|shipment|docket)"
     rf"[^\d\n]{{0,25}}(?:higher|max|whichever|greater)"),
    # Single flat charge (per shipment) — negative lookahead prevents matching "/kg" lines
    ("odaCharges_f",    rf"o\.?d\.?a\.?\s*(?:charges?)?{_NL}(?:rs\.?|₹)\s*(\d+(?:\.\d+)?)(?!\s*/?\s*kg)"),
    ("odaCharges_v",    rf"o\.?d\.?a\.?{_NL}(\d+(?:\.\d+)?)\s*%"),
    # Insurance
    ("insuranceCharges_v", rf"insurance{_NL}(\d+(?:\.\d+)?)\s*%"),
    ("insuranceCharges_f", rf"(?:cargo|transit|goods)\s*insurance{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    # COD — f pattern REQUIRES Rs/₹ so it doesn't steal the % value
    ("codCharges_v",    rf"c\.?o\.?d\.?\s*(?:charges?)?{_NL}(\d+(?:\.\d+)?)\s*%"),
    ("codCharges_f",    rf"c\.?o\.?d\.?\s*(?:charges?)?{_NL}(?:rs\.?|₹|inr)\s*(\d+(?:\.\d+)?)"),
    # Handling
    ("handlingCharges_f", rf"handling\s*(?:charges?){_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    # DACC — strict same-line
    ("daccCharges",     rf"dacc{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    ("daccCharges",     rf"demurrage{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    # Topay — require "charges/surcharge" OR ":" (label:value format like "To Pay: Rs.55")
    ("topayCharges_v",  rf"to\s*pay\s*(?:charges?|surcharge){_NL}(\d+(?:\.\d+)?)\s*%"),
    ("topayCharges_f",  rf"to\s*pay\s*(?:charges?|surcharge){_NL}(?:rs\.?|₹|inr)\s*(\d+(?:\.\d+)?)"),
    ("topayCharges_f",  rf"to\s*pay\s*[:\-]{_NL}(?:rs\.?|₹|inr)\s*(\d+(?:\.\d+)?)"),  # "To Pay: Rs.55"
    # Appointment
    ("appointmentCharges_f", rf"appointment{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
]

_COMPANY_PATTERNS = {
    # GST: accept with or without spaces between segments
    # Standard: 29AAACR5055K1ZB  |  Spaced: 29 AAACR 5055 K 1 Z B
    # Last two chars can be space-separated: "Z B"
    "gstNo":        r'\b(\d{2}\s*[A-Z]{5}\s*\d{4}\s*[A-Z]\s*\d\s*[A-Z0-9]\s*[A-Z0-9])\b',
    "panNo":        r'\b(?:PAN\s*[:\-]?\s*)?([A-Z]{5}\d{4}[A-Z])\b',
    "contactPhone": r'\b((?:0\d{2,4}[-\s]?\d{6,8}|\+?91[-\s]?\d{10}|\d{10}))\b',
    "contactEmail": r'\b([\w.+-]+@[\w-]+\.[\w.]+)\b',
    "website":      r'\b((?:https?://)?(?:www\.)?[\w-]+\.(?:com|in|co\.in|net|org)(?:/[\w/.-]*)?)\b',
    "cinNo":        r'\b([LUu]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6})\b',
    # Address — capture the rest of the line after common address labels (10–150 chars)
    "address": (
        r'(?:address|regd\.?\s*office|registered\s*office|head\s*office)'
        r'\s*[:\-]?\s*([A-Za-z0-9][A-Za-z0-9\s,/\-\.]{8,148}?)(?:\n|$)'
    ),
    # Pincode — explicit label preferred; fallback via address field extraction handles embedded pins
    "pincode": r'(?:pin(?:code)?|postal\s*code)\s*[:\-]?\s*([1-9]\d{5})\b',
}


# ── Minimum useful text threshold ─────────────────────────────────────────────
_MIN_TEXT_CHARS = 80   # fewer than this = treat as scanned/empty


class PDFParser(BaseParser):
    SUPPORTED_EXTENSIONS = [".pdf"]

    def parse(self, file_path: str, doc_context=None) -> Dict[str, Any]:
        fname = os.path.basename(file_path)
        print(f"[PDFParser] Parsing: {fname}")

        text   = ""
        tables: List[List[List[str]]] = []
        method = "none"

        # ── Stage 1: pdfplumber ───────────────────────────────────────────────
        # Always runs first — fast, no ML models, perfect for digital PDFs.
        # Docling is NEVER run on digital PDFs (it adds 3–10 min of ML inference
        # with zero benefit over pdfplumber for text-layer PDFs).
        try:
            import pdfplumber
            text, tables = self._parse_pdfplumber(file_path)
            method = "pdfplumber"
            print(f"[PDFParser] pdfplumber: {len(text)} chars, {len(tables)} tables")
        except ImportError:
            print("[PDFParser] pdfplumber not installed")
        except Exception as e:
            print(f"[PDFParser] pdfplumber error: {e}")

        # ── Stage 2: pymupdf (fitz) ───────────────────────────────────────────
        if len(text.strip()) < _MIN_TEXT_CHARS:
            try:
                import fitz
                fitz_text, fitz_tables = self._parse_pymupdf(file_path)
                if len(fitz_text.strip()) > len(text.strip()):
                    text   = fitz_text
                    tables = fitz_tables or tables
                    method = "pymupdf"
                    print(f"[PDFParser] pymupdf: {len(text)} chars, {len(tables)} tables")
            except ImportError:
                print("[PDFParser] pymupdf not installed")
            except Exception as e:
                print(f"[PDFParser] pymupdf error: {e}")

        # ── Stage 3: pypdfium2 ────────────────────────────────────────────────
        if len(text.strip()) < _MIN_TEXT_CHARS:
            try:
                import pypdfium2 as pdfium
                fium_text = self._parse_pypdfium2(file_path, pdfium)
                if len(fium_text.strip()) > len(text.strip()):
                    text   = fium_text
                    method = "pypdfium2"
                    print(f"[PDFParser] pypdfium2: {len(text)} chars")
            except ImportError:
                pass
            except Exception as e:
                print(f"[PDFParser] pypdfium2 error: {e}")

        # ── Stage 4: PyPDF2 / pypdf ───────────────────────────────────────────
        if len(text.strip()) < _MIN_TEXT_CHARS:
            try:
                pypdf_text = self._parse_pypdf2(file_path)
                if len(pypdf_text.strip()) > len(text.strip()):
                    text   = pypdf_text
                    method = "pypdf"
                    print(f"[PDFParser] pypdf: {len(text)} chars")
            except Exception as e:
                print(f"[PDFParser] pypdf error: {e}")

        # ── Stage 5: OCR (pytesseract) ────────────────────────────────────────
        if len(text.strip()) < _MIN_TEXT_CHARS:
            print(f"[PDFParser] Text layer empty/thin — attempting OCR")
            ocr_text = self._try_ocr_tesseract(file_path)
            if len(ocr_text.strip()) > len(text.strip()):
                text   = ocr_text
                method = "tesseract_ocr"
                print(f"[PDFParser] Tesseract OCR: {len(text)} chars")

        # ── Stage 6: EasyOCR fallback ─────────────────────────────────────────
        if len(text.strip()) < _MIN_TEXT_CHARS:
            ocr_text = self._try_ocr_easyocr(file_path)
            if len(ocr_text.strip()) > len(text.strip()):
                text   = ocr_text
                method = "easyocr"
                print(f"[PDFParser] EasyOCR: {len(text)} chars")

        # ── Stage 7: Docling — LAST RESORT for scanned PDFs only ─────────────
        # Docling loads ML layout models (~300MB) and runs full ML inference.
        # ONLY used when all other text-layer extractors produced < 80 chars
        # (= truly scanned/image-based PDF). Never run on digital PDFs.
        if len(text.strip()) < _MIN_TEXT_CHARS:
            print(f"[PDFParser] All text extractors failed — trying Docling (ML layout, slow)")
            try:
                import concurrent.futures, functools
                from docling.document_converter import DocumentConverter

                def _run_docling():
                    dc = DocumentConverter()
                    return dc.convert(file_path).document.export_to_markdown()

                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                    future = ex.submit(_run_docling)
                    try:
                        docling_text = future.result(timeout=120)  # 2-min hard cap
                        if len(docling_text.strip()) > len(text.strip()):
                            text   = docling_text
                            method = "docling"
                            print(f"[PDFParser] Docling: {len(text)} chars")
                    except concurrent.futures.TimeoutError:
                        print(f"[PDFParser] Docling timed out after 120s — skipping")
            except ImportError:
                pass
            except Exception as _dl_err:
                print(f"[PDFParser] Docling error: {_dl_err}")

        if len(text.strip()) < _MIN_TEXT_CHARS:
            print(f"[PDFParser] WARNING: All extraction methods exhausted. "
                  f"File may be encrypted or empty.")

        print(f"[PDFParser] Final extraction: {len(text)} chars via '{method}', {len(tables)} tables")

        # ── Extract structured data ───────────────────────────────────────────
        data = self._extract_data(text, tables, fname)

        print(f"[PDFParser] parse() done — zone_matrix={len(data.get('zone_matrix') or {})} origins, text_len={len(text)}")
        return {"text": text, "tables": tables, "data": data}

    # ─── Text/table extractors ────────────────────────────────────────────────

    def _parse_pdfplumber(self, file_path: str) -> Tuple[str, List]:
        import pdfplumber
        all_text   = []
        all_tables = []
        prev_cols: Optional[int] = None
        carry_header: Optional[List[str]] = None   # zone header row for multi-page carry
        carry_col_count: int = 0

        with pdfplumber.open(file_path) as pdf:
            for pnum, page in enumerate(pdf.pages):
                page_text = page.extract_text() or ""
                all_text.append(f"\n--- Page {pnum + 1} ---\n{page_text}")

                # RC-5: absorb page header/footer into DocumentContext if wired
                if getattr(self, '_doc_ctx', None):
                    lines = page_text.splitlines()
                    if lines:
                        self._doc_ctx.absorb(lines[0],  source=f"pdf_p{pnum+1}_header")
                        self._doc_ctx.absorb(lines[-1], source=f"pdf_p{pnum+1}_footer")

                # Primary: pdfplumber structured table extraction (best for bordered tables)
                page_tables = page.extract_tables() or []
                coord_tables = []

                # RC-2: For pages with no/few structured tables, try coordinate extraction
                if len(page_tables) == 0:
                    coord = self._extract_with_coordinates(page)
                    if len(coord) >= 2 and len(coord[0]) >= 2:
                        coord_tables.append(coord)

                for raw_table in page_tables:
                    if not raw_table or len(raw_table) < 2:
                        continue
                    clean = [
                        [str(c).strip() if c is not None else "" for c in row]
                        for row in raw_table
                    ]
                    n_cols = max(len(r) for r in clean)

                    # Multi-page table: if column count matches carry header, prepend it
                    if (carry_header is not None
                            and n_cols == carry_col_count
                            and not self._row_has_zone_header(clean[0])):
                        clean = [carry_header] + clean

                    all_tables.append(clean)

                    # Save zone header for next page carry
                    if self._row_has_zone_header(clean[0]):
                        carry_header    = clean[0]
                        carry_col_count = n_cols
                    prev_cols = n_cols

                # Add coordinate-extracted tables (from borderless pages)
                all_tables.extend(coord_tables)

        return "\n".join(all_text), all_tables

    # ── RC-2: Coordinate-aware table reconstruction ───────────────────────────

    def _extract_with_coordinates(self, page) -> List[List[str]]:
        """
        Reconstruct table structure from pdfplumber word-level bounding boxes.

        Unlike page.extract_text() which joins everything left→right / top→bottom,
        this groups words by Y-coordinate (rows) then X-gap (columns), preserving
        the horizontal relationship between "City" labels and their rate values.

        Works for: borderless tables, multi-column layouts, and tables where
        pdfplumber's grid detection finds no borders.

        Returns: List[row] where each row is List[cell_text].
        Empty list if fewer than 2 rows detected.
        """
        try:
            words = page.extract_words(
                x_tolerance=3,
                y_tolerance=3,
                keep_blank_chars=False,
            )
        except Exception:
            return []

        if not words:
            return []

        # Group words by quantised Y-position (4px buckets = same row)
        from collections import defaultdict
        rows_dict: dict = defaultdict(list)
        for w in words:
            # Floor-division into 8px buckets avoids Python 3 banker's-rounding
            # edge case where round(102/4)=26 (not 25) splits same-row words.
            row_key = int(w["top"] / 8) * 8
            rows_dict[row_key].append(w)

        table_rows: List[List[str]] = []
        for y_key in sorted(rows_dict.keys()):
            row_words = sorted(rows_dict[y_key], key=lambda w: w["x0"])
            cells = self._cluster_words_to_columns(row_words, gap=12)
            if cells:
                table_rows.append(cells)

        return table_rows

    def _cluster_words_to_columns(self, words: List, gap: int = 12) -> List[str]:
        """
        Group horizontally adjacent words into columns by detecting X-gaps > `gap` px.
        Preserves spatial relationship: words in the same column are joined with space.
        """
        if not words:
            return []
        columns: List[List] = [[words[0]]]
        for word in words[1:]:
            if word["x0"] - columns[-1][-1]["x1"] > gap:
                columns.append([])
            columns[-1].append(word)
        return [" ".join(w["text"] for w in col) for col in columns]

    def _row_has_zone_header(self, row: List[str]) -> bool:
        """True if this row contains >= 2 canonical zone codes (e.g. N1, S1)."""
        _ZONES = {"N1","N2","N3","N4","S1","S2","S3","S4",
                  "E1","E2","W1","W2","W3","C1","C2","NE1","NE2","X1","X2","X3"}
        hits = sum(1 for cell in row if str(cell).strip().upper() in _ZONES)
        return hits >= 2

    def _parse_pymupdf(self, file_path: str) -> Tuple[str, List]:
        import fitz
        doc    = fitz.open(file_path)
        parts  = []
        tables = []
        for pnum in range(len(doc)):
            page = doc[pnum]
            text = page.get_text("text")
            parts.append(f"\n--- Page {pnum + 1} ---\n{text}")
            # Extract tables via pymupdf's built-in finder (fitz 1.23+)
            try:
                for tab in page.find_tables().tables:
                    rows = tab.extract()
                    clean = [
                        [str(c).strip() if c is not None else "" for c in row]
                        for row in (rows or [])
                        if any(c for c in row)
                    ]
                    if len(clean) >= 2:
                        tables.append(clean)
            except Exception:
                pass
        return "\n".join(parts), tables

    def _parse_pypdfium2(self, file_path: str, pdfium) -> str:
        doc   = pdfium.PdfDocument(file_path)
        parts = []
        for pnum in range(len(doc)):
            page   = doc[pnum]
            textpage = page.get_textpage()
            parts.append(f"\n--- Page {pnum + 1} ---\n{textpage.get_text_range()}")
        return "\n".join(parts)

    def _parse_pypdf2(self, file_path: str) -> str:
        parts = []
        # Try modern pypdf first, fall back to PyPDF2
        try:
            import pypdf
            reader = pypdf.PdfReader(file_path)
            pages  = reader.pages
        except ImportError:
            import PyPDF2
            reader = PyPDF2.PdfReader(open(file_path, "rb"))
            pages  = reader.pages
        for i, page in enumerate(pages):
            parts.append(f"\n--- Page {i+1} ---\n{page.extract_text() or ''}")
        return "".join(parts)

    # ─── OCR engines ─────────────────────────────────────────────────────────

    @staticmethod
    def _preprocess_for_ocr(img):
        """
        Preprocess a PIL image for maximum OCR accuracy on scanned documents.
        Pipeline: grayscale → upscale if small → contrast boost → adaptive binarisation.
        Returns a preprocessed PIL Image.
        """
        try:
            from PIL import Image, ImageEnhance, ImageFilter
        except ImportError:
            return img  # PIL not available — return as-is

        # 1. Grayscale
        gray = img.convert("L")

        # 2. Upscale very small images (< 1200 px wide) so OCR has enough resolution
        if gray.width < 1200:
            scale = max(2, 1200 // gray.width)
            gray = gray.resize((gray.width * scale, gray.height * scale), Image.LANCZOS)

        # 3. Mild sharpening — helps with slightly blurry scans
        gray = gray.filter(ImageFilter.SHARPEN)

        # 4. Contrast enhancement
        gray = ImageEnhance.Contrast(gray).enhance(1.5)

        # 5. Adaptive binarisation via numpy/OpenCV (fall back to simple threshold)
        try:
            import numpy as np
            import cv2
            arr   = np.array(gray)
            arr   = cv2.adaptiveThreshold(
                arr, 255,
                cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY,
                blockSize=25, C=10,
            )
            # Mild dilation to reconnect broken character strokes
            kernel = np.ones((1, 1), np.uint8)
            arr    = cv2.dilate(arr, kernel, iterations=1)
            from PIL import Image as _PIL
            return _PIL.fromarray(arr)
        except Exception:
            pass  # OpenCV unavailable — return contrast-enhanced grayscale
        return gray

    def _try_ocr_tesseract(self, file_path: str) -> str:
        """
        Convert PDF pages to images and OCR with pytesseract.
        Tries pdf2image first (300 dpi), then pymupdf page rendering (3× scale).
        Images are preprocessed (grayscale, contrast, binarisation) before OCR.
        """
        images = []

        # Attempt 1: pdf2image — 300 dpi gives significantly better OCR on scanned docs
        try:
            from pdf2image import convert_from_path
            images = convert_from_path(file_path, dpi=300, fmt="png")
            print(f"[PDFParser] pdf2image: converted {len(images)} pages @ 300 dpi")
        except ImportError:
            print("[PDFParser] pdf2image not installed — trying pymupdf render")
        except Exception as e:
            print(f"[PDFParser] pdf2image error: {e}")

        # Attempt 2: pymupdf page rendering at 3× scale (≈ 216 dpi equivalent)
        if not images:
            try:
                import fitz
                from PIL import Image
                doc = fitz.open(file_path)
                for pnum in range(len(doc)):
                    page = doc[pnum]
                    mat  = fitz.Matrix(3.0, 3.0)   # 3× = ~216 dpi for better OCR
                    pix  = page.get_pixmap(matrix=mat, alpha=False)
                    img  = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                    images.append(img)
                print(f"[PDFParser] pymupdf render: {len(images)} pages @ 3× scale")
            except ImportError:
                print("[PDFParser] pymupdf not available for rendering")
            except Exception as e:
                print(f"[PDFParser] pymupdf render error: {e}")

        if not images:
            return ""

        # Run tesseract with preprocessing
        try:
            import pytesseract
            parts = []
            for i, img in enumerate(images):
                processed = self._preprocess_for_ocr(img)
                # --psm 3 = fully automatic page segmentation (better for mixed layouts)
                # --psm 6 is good for uniform blocks but misses tables; try 3 first then 6
                for psm in ("--psm 3 --oem 3", "--psm 6 --oem 3"):
                    try:
                        page_text = pytesseract.image_to_string(
                            processed,
                            lang="eng",
                            config=psm,
                        )
                        if len(page_text.strip()) > 50:
                            break  # good enough — use this result
                    except Exception:
                        page_text = ""
                parts.append(f"\n--- Page {i+1} ---\n{page_text}")
            result = "\n".join(parts)
            print(f"[PDFParser] Tesseract extracted {len(result)} chars from {len(images)} pages")
            return result
        except ImportError:
            print("[PDFParser] pytesseract not installed")
        except Exception as e:
            print(f"[PDFParser] tesseract error: {e}")

        return ""

    def _try_ocr_easyocr(self, file_path: str) -> str:
        """EasyOCR as second OCR engine — no poppler/tesseract dependency.
        Pages are preprocessed via _preprocess_for_ocr before reading."""
        images = []
        try:
            import fitz
            from PIL import Image
            doc = fitz.open(file_path)
            for pnum in range(len(doc)):
                page = doc[pnum]
                mat  = fitz.Matrix(3.0, 3.0)   # 3× for better OCR accuracy
                pix  = page.get_pixmap(matrix=mat, alpha=False)
                img  = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                images.append(img)
        except Exception:
            return ""

        try:
            import easyocr
            import numpy as np
            reader = easyocr.Reader(["en"], gpu=False, verbose=False)
            parts  = []
            for i, img in enumerate(images):
                processed = self._preprocess_for_ocr(img)
                arr       = np.array(processed.convert("RGB") if processed.mode != "RGB" else processed)
                results   = reader.readtext(arr, detail=0, paragraph=True)
                parts.append(f"\n--- Page {i+1} ---\n" + "\n".join(results))
            return "\n".join(parts)
        except ImportError:
            return ""
        except Exception as e:
            print(f"[PDFParser] easyocr error: {e}")
            return ""

    # ─── Data extraction ──────────────────────────────────────────────────────

    def _extract_data(
        self, text: str, tables: List[List[List[str]]], fname: str = ""
    ) -> Dict:
        from parsers.excel_parser import ExcelParser
        ep = ExcelParser()

        data: Dict = {}

        # ── Content classifier: sniff what this PDF is about ─────────────────
        try:
            from knowledge.content_classifier import ContentClassifier
            cc = ContentClassifier()
            file_hint = cc.classify_file_hint(fname)
            text_classification = cc.classify_text(text[:8000])
            print(f"[PDFParser] Content classifier: "
                  f"file_hint={file_hint}  "
                  f"text={text_classification['category']} "
                  f"(conf={text_classification['confidence']:.2f})")
        except Exception as _cc_err:
            print(f"[PDFParser] Content classifier unavailable: {_cc_err}")
            text_classification = {"category": "UNKNOWN"}

        # ── Section segmentation — must run BEFORE OICR and table extraction ───
        # This prevents the OICR engine and regex patterns from seeing VOLUMETRIC
        # section content (1728 CFT constant) as charge values.
        _sections_map: Dict = {}
        _charge_text_for_oicr = text  # default: full text
        try:
            from knowledge.section_segmenter import SectionSegmenter
            _seg = SectionSegmenter()
            _sections = _seg.segment_text(text)
            _sections_map = _seg.get_sections_map(_sections)
            # Give OICR only CHARGES + MIXED + COMPANY_INFO sections.
            # Blocked: VOLUMETRIC (divisor constants), LEGAL (clause numbers),
            # AIR_FREIGHT (air-mode rates that must never be used as road rates).
            _BLOCKED_CATS = frozenset(("VOLUMETRIC", "LEGAL", "AIR_FREIGHT"))
            _allowed = []
            for cat in ("CHARGES", "MIXED", "COMPANY_INFO"):
                for sec in _sections_map.get(cat, []):
                    _allowed.append(sec.text)
            if _allowed:
                _charge_text_for_oicr = "\n\n".join(_allowed)
                _blocked = sum(
                    len(secs) for cat, secs in _sections_map.items()
                    if cat in _BLOCKED_CATS
                )
                print(f"[PDFParser] Section pre-filter: {len(_allowed)} allowed sections, "
                      f"{_blocked} dangerous sections blocked from OICR")
        except Exception as _seg_err:
            print(f"[PDFParser] Section pre-filter unavailable: {_seg_err}")

        # ── OICR engine — runs on section-filtered text, not full text ────────
        # Pre-process tables BEFORE OICR sees them:
        #   1. Strip "By Air" sub-sections from mixed mode tables
        #   2. Pick SFC row from multi-service charge rows
        #
        # IMPORTANT: Save originals before mode-split for city-rate-card parsing.
        # When a table has [By Air header + air data] followed by [By Road data],
        # mode-split strips the header row (it's in the air section).  The original
        # tables still carry the header, so detect_city_rate_card can find it there.
        _original_tables = [list(t) for t in tables]   # shallow copy per table
        tables = [self._split_air_road_table(t) for t in tables]
        tables = [self._pick_surface_row(t) for t in tables]

        _OICR_SANITY = {
            "docketCharges": 2000, "daccCharges": 10000, "codCharges": 5000,
            "greenTax": 2000,      "minCharges": 50000,  "topayCharges": 5000,
            "handlingCharges": 50000, "odaCharges": 50000,
        }
        try:
            from parsers.oicr_engine import get_oicr_engine
            oicr = get_oicr_engine()
            oicr_pdf = oicr.process_pdf_text(_charge_text_for_oicr, tables)
            if oicr_pdf.get("company_details"):
                data.setdefault("company_details", {}).update(oicr_pdf["company_details"])
            # OICR charge extraction is intentionally skipped here.
            # OICR's row-by-row table parser conflicts with horizontal charge tables
            # (like TCI's DWB | FOD | Min | ... header/values format).
            # Charge extraction is handled by:
            #   1. ExcelParser._try_parse_charges() on each table (below)
            #   2. PDFParser._extract_charges_from_text() on section-filtered text
            # OICR contributes: zone matrix + company info only.
            if oicr_pdf.get("zone_matrix") and not data.get("zone_matrix"):
                data["zone_matrix"] = oicr_pdf["zone_matrix"]
                print(f"[PDFParser] OICR zone matrix: {len(data['zone_matrix'])} origins")
        except Exception as _oicr_err:
            print(f"[PDFParser] OICR pass failed: {_oicr_err}")

        # ── Table extraction ─────────────────────────────────────────────────
        # Tables were already pre-processed by _pick_surface_row before OICR.
        for ti, table in enumerate(tables):
            if not table:
                continue

            # Classify for logging/debugging only — does NOT gate extractors
            try:
                tc = cc.classify_rows(table) if 'cc' in dir() else {"category": "UNKNOWN"}
                table_cat = tc.get("category", "UNKNOWN")
                print(f"[PDFParser] Table {ti}: classifier={table_cat} "
                      f"(conf={tc.get('confidence', 0):.2f})")
            except Exception:
                table_cat = "UNKNOWN"

            # Zone matrix — always try if not yet found
            if not data.get("zone_matrix"):
                zm = ep._try_parse_zone_matrix(table, f"PDF_Table_{ti}")
                if zm and len(zm) > len(data.get("zone_matrix") or {}):
                    data["zone_matrix"] = zm
                    print(f"[PDFParser] Table {ti}: zone matrix {len(zm)} origins")

            # Charges — always try on every table
            ch = ep._try_parse_charges(table, f"PDF_Table_{ti}")
            if ch:
                data.setdefault("charges", {})
                for k, v in ch.items():
                    data["charges"].setdefault(k, v)

            # Pincode list — always try on every table
            pinlist = ep._try_parse_pincode_list(table, f"PDF_Table_{ti}")
            if pinlist:
                data.setdefault("served_pincodes", [])
                data.setdefault("oda_pincodes",    [])
                data["served_pincodes"].extend(pinlist.get("served", []))
                data["oda_pincodes"].extend(pinlist.get("oda", []))
                if pinlist.get("zone_pincodes"):
                    data.setdefault("zone_pincodes", {})
                    for z, pins in pinlist["zone_pincodes"].items():
                        data["zone_pincodes"].setdefault(z, []).extend(pins)
            else:
                # Raw pincode sweep on every table (old behaviour)
                pins = self._extract_pincodes_from_table(table)
                if pins:
                    data.setdefault("served_pincodes", [])
                    data["served_pincodes"].extend(pins)

        # ── Section-aware text-level extraction ─────────────────────────────
        if text:
            # Reuse sections already computed above (from pre-OICR segmentation)
            sections_map = _sections_map

            # Charges: extract from CHARGES, MIXED, UNKNOWN, and ZONE_MATRIX sections
            # ZONE_MATRIX is included because the GST clause is often on the same page
            # as zone rates ("GST @ 18% for Surface, Air and Rail") and gets
            # misclassified as ZONE_MATRIX by the keyword scorer.
            charge_text = ""
            for allowed_cat in ("CHARGES", "MIXED", "UNKNOWN", "ZONE_MATRIX"):
                for sec in sections_map.get(allowed_cat, []):
                    charge_text += "\n" + sec.text
            # Log if any AIR_FREIGHT sections were blocked from charge extraction
            _air_blocked = len(sections_map.get("AIR_FREIGHT", []))
            if _air_blocked:
                print(f"[PDFParser] Air-section filter: blocked {_air_blocked} "
                      f"AIR_FREIGHT section(s) from charge extraction")

            if not charge_text.strip():
                # Fallback: full text if segmenter found no CHARGES sections
                charge_text = text

            # Strip "By Air ..." blocks so air-mode minWeight / charges don't bleed in.
            # The section_segmenter may classify air blocks as TRANSIT_DAYS rather than
            # AIR_FREIGHT (TAT columns score high), so use direct text splitting as well.
            charge_text = self._strip_air_mode_from_text(
                sections_map.get("AIR_FREIGHT", []), charge_text
            )
            text_charges = self._extract_charges_from_text(charge_text)
            if text_charges:
                from knowledge.charge_richness import charge_richness as _cr
                data.setdefault("charges", {})
                for k, v in text_charges.items():
                    # Richer wins: a typed config from the text path (e.g.
                    # per_kg_minimum) must not be blocked by an untyped {v,f}
                    # the table path captured for the same field.
                    existing = data["charges"].get(k)
                    if existing is None or _cr(v) > _cr(existing):
                        if existing is not None:
                            print(f"[PDFParser] charges.{k}: text candidate richer "
                                  f"({_cr(v)} > {_cr(existing)}) — replacing table value")
                        data["charges"][k] = v

            # Company info: extract from COMPANY_INFO sections + full text
            # (company data is usually in header/cover — safe to search everywhere)
            cd = self._extract_company_from_text(text)
            if cd:
                data.setdefault("company_details", {})
                for k, v in cd.items():
                    data["company_details"].setdefault(k, v)

            # Zone matrix from text rows (if still missing)
            if not data.get("zone_matrix"):
                # Prefer ZONE_MATRIX sections; fall back to full text
                zm_text = ""
                for sec in sections_map.get("ZONE_MATRIX", []):
                    zm_text += "\n" + sec.text
                source_text = zm_text if zm_text.strip() else text

                text_rows = self._text_to_rows(source_text)
                zm = ep._try_parse_zone_matrix(text_rows, "PDF_Text")
                if zm:
                    data["zone_matrix"] = zm
                    print(f"[PDFParser] Text zone matrix: {len(zm)} origins")
                ch = ep._try_parse_charges(text_rows, "PDF_Text")
                if ch:
                    data.setdefault("charges", {})
                    for k, v in ch.items():
                        data["charges"].setdefault(k, v)

            # ── City-rate-card fallback (if still no zone matrix) ─────────────
            # Handles single-origin rate cards (Destination|State|Rate/kg rows).
            # Uses ORIGINAL tables (pre-mode-split) because mode-split may have
            # removed the "Destination|State|Rate|TAT" header row (it was in the
            # "By Air" sub-section of a mixed-mode table).
            # detect_city_rate_card has its own road-rate sanity filter (>60 Rs/kg
            # rows are rejected), so it's safe to pass it the full unsplit table.
            if not data.get("zone_matrix"):
                try:
                    from parsers.oicr_engine import get_oicr_engine
                    oicr = get_oicr_engine()
                    # Try original (pre-split) tables first — preserves headers
                    crc_sources = list(_original_tables)
                    # Also try road-text rows as last resort
                    road_text = self._strip_air_mode_from_text(
                        _sections_map.get("AIR_FREIGHT", []), text
                    )
                    road_rows = self._text_to_rows(road_text)
                    if road_rows:
                        crc_sources.append(road_rows)

                    for src in crc_sources:
                        if not src:
                            continue
                        zm = oicr.detect_city_rate_card(src, context_text=text)
                        if zm:
                            data["zone_matrix"] = zm
                            print(f"[PDFParser] City-rate-card zone matrix: "
                                  f"{len(zm)} origins")
                            break
                except Exception as _crc_err:
                    print(f"[PDFParser] City-rate-card fallback failed: {_crc_err}")

            # Pincodes: extract from PINCODE_LIST and ODA_LIST sections
            pin_text = ""
            for cat in ("PINCODE_LIST", "ODA_LIST"):
                for sec in sections_map.get(cat, []):
                    pin_text += "\n" + sec.text
            pins = self._extract_pincodes_from_text(pin_text if pin_text.strip() else text)
            if pins:
                data.setdefault("served_pincodes", [])
                data["served_pincodes"].extend(pins)

        # ── Dedup ────────────────────────────────────────────────────────────
        if data.get("served_pincodes"):
            data["served_pincodes"] = list(dict.fromkeys(data["served_pincodes"]))
        if data.get("oda_pincodes"):
            data["oda_pincodes"] = list(dict.fromkeys(data["oda_pincodes"]))

        # ── Zone matrix enrichment: validate, infer, and complete ─────────────
        data = self._enrich_zone_matrix(data)

        # ── Passive learning: auto-confirm high-confidence extractions ────────
        self._passive_learn(data)

        # ── Summary log ──────────────────────────────────────────────────────
        print(
            f"[PDFParser] _extract_data summary: "
            f"zone_matrix={len(data.get('zone_matrix') or {})} origins | "
            f"charges={list((data.get('charges') or {}).keys())} | "
            f"served_pincodes={len(data.get('served_pincodes') or [])} | "
            f"oda_pincodes={len(data.get('oda_pincodes') or [])} | "
            f"company_details={'yes' if data.get('company_details') else 'no'}"
        )

        return data

    # ─── Zone matrix enrichment ──────────────────────────────────────────────

    _ALL_ZONES_SET = frozenset([
        "N1","N2","N3","N4","S1","S2","S3","S4",
        "E1","E2","W1","W2","C1","C2","NE1","NE2","X1","X2","X3",
    ])

    def _enrich_zone_matrix(self, data: Dict) -> Dict:
        """
        Post-extraction zone matrix enrichment. Runs after all other parsers have
        had their chance. Goals:

        1. Validate rate mode (road vs air vs effective).
        2. Use served_pincodes to compute zone distribution — helps confirm which
           zones the transporter actually covers and detect missing zone rows.
        3. If zone matrix is partial (< 19 origins), fill missing origins via
           distance interpolation from the known origins.
        4. If zone matrix is empty but pincodes were found, report zone distribution
           so the caller can build a partial matrix from a rate card elsewhere.
        5. Cross-check zone matrix keys against pincode distribution to flag
           suspicious zone assignments.
        """
        try:
            from parsers.oicr_engine import get_oicr_engine, ALL_ZONES
            oicr = get_oicr_engine()
        except Exception as e:
            print(f"[PDFParser] _enrich_zone_matrix: OICR unavailable: {e}")
            return data

        zone_matrix    = data.get("zone_matrix") or {}
        served_pincodes = data.get("served_pincodes") or []
        oda_pincodes   = data.get("oda_pincodes") or []

        # ── 1. Rate mode classification ───────────────────────────────────────
        if zone_matrix:
            mode_info = oicr.classify_rate_mode(zone_matrix)
            data["_rate_mode"] = mode_info["mode"]
            if mode_info.get("warning"):
                print(f"[PDFParser] Rate mode: {mode_info['mode']} — "
                      f"{mode_info['warning']}")
            else:
                print(f"[PDFParser] Rate mode: {mode_info['mode']} "
                      f"(min={mode_info['min_rate']} "
                      f"avg={mode_info['avg_rate']:.1f} "
                      f"max={mode_info['max_rate']} Rs/kg)")

        # ── 2. Pincode zone distribution ──────────────────────────────────────
        all_pincodes = list(set(served_pincodes + oda_pincodes))
        if len(all_pincodes) >= 10:
            zone_dist = oicr.infer_zones_from_pincodes(all_pincodes)
            if zone_dist:
                total_pins = sum(zone_dist.values())
                top_zones  = sorted(zone_dist.items(), key=lambda x: -x[1])
                dist_str   = " | ".join(
                    f"{z}={c}({c*100//total_pins}%)"
                    for z, c in top_zones[:8]
                )
                print(f"[PDFParser] Pincode zone distribution ({total_pins} pincodes): "
                      f"{dist_str}")
                data["zone_distribution"] = zone_dist

                # ── 2a. Cross-check: warn if zone matrix has origins not in
                #        pincode coverage (possible ghost zones)
                if zone_matrix:
                    ghost_zones = [
                        z for z in zone_matrix
                        if z not in zone_dist and z not in ("X1","X2","X3")
                    ]
                    if ghost_zones:
                        print(f"[PDFParser] NOTE: zone matrix has origins with no "
                              f"pincodes in zone_distribution: {ghost_zones} "
                              f"(may be extrapolated or distant hubs — verify)")

                # ── 2b. If zone matrix is empty, use pincodes to build partial
                #        zone list so the frontend knows which zones are served
                if not zone_matrix and zone_dist:
                    data["inferred_served_zones"] = [z for z, _ in top_zones]
                    print(f"[PDFParser] No rate matrix found — inferred "
                          f"{len(data['inferred_served_zones'])} served zones "
                          f"from pincodes")

        # ── 3. Log coverage — no gap-filling, only report what vendor provided ──
        if zone_matrix:
            found_count   = len(zone_matrix)
            missing_count = len(self._ALL_ZONES_SET - set(zone_matrix.keys()))
            if missing_count > 0:
                print(f"[PDFParser] Zone matrix: {found_count} origin(s) from vendor; "
                      f"{missing_count} origin(s) not in document (left blank)")
            else:
                print(f"[PDFParser] Zone matrix: all 19 origins present")

        return data

    # ─── Passive learning ────────────────────────────────────────────────────

    def _passive_learn(self, data: Dict):
        """
        Auto-confirm charge and zone labels that were matched with high confidence
        and produced actual data (i.e., the downstream fields are non-empty).
        This makes the system smarter with each successful parse — like autocorrect.
        """
        try:
            from knowledge.ml_dictionary_engine import record_passive_confirmation
            charges = data.get("charges", {})
            for field, value in charges.items():
                if value and field not in (None, "weightSlabRates"):
                    record_passive_confirmation("charge", field, field, confidence=0.75)
        except Exception:
            pass  # passive learning is best-effort

    # ─── Service-type charge table row picker ────────────────────────────────

    # ─── Air/Road table splitter ─────────────────────────────────────────────

    _AIR_CELL_RE  = re.compile(r'^\s*by\s*air\b', re.I)
    _ROAD_CELL_RE = re.compile(r'^\s*by\s*(?:road|surface|ground)\b', re.I)

    def _split_air_road_table(self, table: List[List[str]]) -> List[List[str]]:
        """
        Many Indian carrier PDFs embed "By Air" and "By Road" sub-section headers
        as rows inside a single pdfplumber table.  This method keeps only road
        segments, discarding air segments.  Tables with no mode markers are returned
        unchanged.
        """
        mode_rows: List[tuple] = []
        for ri, row in enumerate(table):
            for cell in row:
                if cell and isinstance(cell, str):
                    s = cell.strip()
                    if self._AIR_CELL_RE.match(s):
                        mode_rows.append((ri, "air"))
                        break
                    if self._ROAD_CELL_RE.match(s):
                        mode_rows.append((ri, "road"))
                        break

        if not mode_rows:
            return table

        segments = []
        for i, (ri, mode) in enumerate(mode_rows):
            end = mode_rows[i + 1][0] if i + 1 < len(mode_rows) else len(table)
            segments.append((ri, end, mode))

        preamble = table[:mode_rows[0][0]]
        road_rows: List[List[str]] = []
        air_count = road_count = 0
        for start, end, mode in segments:
            if mode == "road":
                road_rows.extend(table[start:end])
                road_count += 1
            else:
                air_count += 1

        print(f"[PDFParser] Mode-split: stripped {air_count} AIR segment(s), "
              f"kept {road_count} ROAD segment(s)")

        return preamble + road_rows if road_rows else preamble

    # ─── Air-mode text stripper ──────────────────────────────────────────────

    _AIR_SECTION_TEXT_RE  = re.compile(r'(?i)(?:^|\n)(?:by\s+air\b[^\n]*)\n')
    _ROAD_SECTION_TEXT_RE = re.compile(r'(?i)(?:^|\n)(?:by\s+(?:road|surface|ground)\b[^\n]*)\n')

    def _strip_air_mode_from_text(self, air_sections: list, text: str) -> str:
        """
        Remove "By Air ..." blocks from text so charge regexes (especially
        minWeight) don't pick up air-mode values.

        Pass 1: strip section_segmenter-identified AIR_FREIGHT blocks.
        Pass 2: split on inline "By Air..." / "By Road..." line markers.
        """
        if air_sections:
            air_texts = {s.text for s in air_sections}
            remaining = "\n\n".join(
                chunk for chunk in text.split("\n\n")
                if chunk.strip() not in air_texts
            )
            if remaining.strip() != text.strip():
                print(f"[PDFParser] Air-section strip: removed "
                      f"{len(air_sections)} AIR_FREIGHT section(s) from charge text")
                return remaining

        markers: List[tuple] = []
        for m in self._AIR_SECTION_TEXT_RE.finditer(text):
            markers.append((m.start(), "air"))
        for m in self._ROAD_SECTION_TEXT_RE.finditer(text):
            markers.append((m.start(), "road"))

        if not markers:
            return text

        markers.sort(key=lambda x: x[0])
        kept: List[str] = [text[:markers[0][0]]]
        for i, (pos, mode) in enumerate(markers):
            seg_end = markers[i + 1][0] if i + 1 < len(markers) else len(text)
            if mode == "road":
                kept.append(text[pos:seg_end])

        result = "".join(kept)
        if result.strip() != text.strip():
            air_cnt  = sum(1 for _, m in markers if m == "air")
            road_cnt = sum(1 for _, m in markers if m == "road")
            print(f"[PDFParser] Air-text strip: removed {air_cnt} air block(s), "
                  f"kept {road_cnt} road block(s)")
        return result

    # ─── Service-type charge table row picker ────────────────────────────────

    def _pick_surface_row(self, table: List[List[str]]) -> List[List[str]]:
        """
        TCI (and many Indian transporters) present charges in a multi-service table:

          Row 0: [DWB Charges, FOD, Min Freight, Min Weight, COD, DACC, Volumetric, NGT]
          Row 1: [SFC values:  150,  100,  350,  30,  100,  100, 10, 50]  ← we want this
          Row 2: [AIR values:  200,  200,  500,   5,  300,  200, 10, 100]
          Row 3: [Rail values: 2500, ...]

        If such a pattern is detected, return the table with ONLY the SFC/surface row
        under the header. The other service rows (AIR/Rail) are discarded — we only
        generate LTL (Surface Express) USTFs.

        Detection: header row contains DWB/charge keywords; subsequent rows are
        pure numeric; first numeric row = SFC (cheapest/ground service).
        """
        if not table or len(table) < 3:
            return table

        # Look for a header row with charge keywords
        _CHARGE_KW = re.compile(
            r'(?i)\b(?:dwb|docket|minimum|min|cod|dacc|fod|volumetric|fuel|ngt|'
            r'lr\b|waybill|bilty|handling|surcharge|charges?)\b'
        )
        _PURE_NUMERIC = re.compile(r'^[\d.,\s/\-NnaAN/A]*$')

        header_idx = None
        for ri, row in enumerate(table[:8]):
            row_text = " ".join(str(c) for c in row if c)
            matches = _CHARGE_KW.findall(row_text)
            if len(matches) >= 3:   # at least 3 charge keywords = charge header
                header_idx = ri
                break

        if header_idx is None:
            return table

        # Find contiguous numeric rows after header
        numeric_rows = []
        for row in table[header_idx + 1:]:
            # Filter out None/empty cells — pdfplumber returns None for empty cells
            cells = [str(c).strip() for c in row if c is not None and str(c).strip() not in ('', 'None')]
            if not cells:
                continue
            # First cell may be a service label (SFC, AIR, Rail, etc.)
            rest_cells = cells[1:] if len(cells) > 1 else cells
            rest_text = " ".join(rest_cells)
            if _PURE_NUMERIC.match(rest_text):
                numeric_rows.append(row)
            elif numeric_rows:
                break  # stop at first non-numeric after we've started collecting

        if len(numeric_rows) >= 2:
            # Multiple service rows detected; keep only the first (SFC / surface)
            sfc_row = numeric_rows[0]
            label = str(sfc_row[0]).strip().upper() if sfc_row else ""
            print(f"[PDFParser] Multi-service charge table: keeping row 1 "
                  f"('{label}') of {len(numeric_rows)} service rows")
            return table[:header_idx + 1] + [sfc_row]

        return table

    # ─── Charge extraction from text ─────────────────────────────────────────

    def _extract_charges_from_text(self, text: str) -> Dict:
        lower  = text.lower()
        charges: Dict = {}

        for field, pattern in _CHARGE_PATTERNS:
            m = re.search(pattern, lower)
            if not m:
                continue

            # ── Dual-rate ODA: "Rs. X/Kg or Rs. Y/consignment (higher)" ──────
            if field == "odaCharges_dual":
                try:
                    per_kg  = float(m.group(1))  # absolute ₹/kg (NOT a percentage)
                    minimum = float(m.group(2))  # flat floor per consignment
                    charges["odaCharges"] = {
                        "type": "per_kg_minimum",
                        "perKg": per_kg,      # chargeFormulas.js reads odaConfig.perKg
                        "minimum": minimum,   # chargeFormulas.js reads odaConfig.minimum
                    }
                    print(f"[PDFParser] ODA dual-rate: perKg={per_kg} minimum={minimum} → per_kg_minimum")
                except (ValueError, IndexError):
                    pass
                continue

            # Skip single-value ODA patterns if already captured by dual-rate above
            if field in ("odaCharges_f", "odaCharges_v") and "odaCharges" in charges:
                continue

            try:
                val = float(m.group(1))
            except (ValueError, IndexError):
                continue

            if field.endswith("_v"):
                canon = field[:-2]
                entry = charges.setdefault(canon, {})
                entry["v"] = val
                # For ROV/FOV and insurance, detect whether the percentage is
                # "on invoice value" or "on freight" so chargeFormulas.js knows
                # what base to apply it to (without a basis, these charges are
                # always treated as 0 — see chargeFormulas.js rovCharges logic).
                if canon in ("rovCharges", "insuranceCharges"):
                    context = lower[m.start():m.end() + 40]
                    if "invoice" in context:
                        entry["basis"] = "invoice"
                    elif "freight" in context:
                        entry["basis"] = "freight"
            elif field.endswith("_f"):
                canon = field[:-2]
                # Sanity cap for fixed charges — reject volumetric artefacts
                cap = _CHARGE_SANITY_MAX.get(canon)
                if cap and val > cap:
                    print(f"[PDFParser] REJECTED {canon}_f={val} (exceeds sanity cap {cap}) — "
                          f"likely volumetric constant, not a charge")
                    continue
                oda_entry = charges.setdefault(canon, {})
                oda_entry["f"] = val
                if canon == "odaCharges":
                    oda_entry.setdefault("type", "per_shipment")
            else:
                # Sanity cap for scalar charges
                cap = _CHARGE_SANITY_MAX.get(field)
                if cap and val > cap:
                    print(f"[PDFParser] REJECTED {field}={val} (exceeds sanity cap {cap}) — "
                          f"likely volumetric constant, not a charge")
                    continue
                charges.setdefault(field, val)

        # ── Post-processing ───────────────────────────────────────────────────
        # Upgrade scalar charge captures to {v,f} by scanning full lines.
        # Catches "1.25% of COD value (Min Rs.85)" where regex captured only 1.25
        # but the full line has both % and Rs amount.
        try:
            from knowledge.charge_normalizer import ChargeNormalizer
            _cn_text = ChargeNormalizer()
            _VF_FIELDS = {"codCharges", "topayCharges", "daccCharges", "rovCharges", "insuranceCharges"}
            for _field in _VF_FIELDS:
                _existing = charges.get(_field)
                # If we only have a scalar or single-key dict, look for the full line
                if _existing is None or (isinstance(_existing, dict) and len(_existing) < 2):
                    for line in lower.split('\n'):
                        _fname = _field.replace('Charges','').lower()
                        if _fname not in line and _field.lower() not in line:
                            continue
                        _normalized = _cn_text.normalize(line, field=_field)
                        if isinstance(_normalized, dict) and _normalized.get('f') and _normalized.get('f', 0) > 0:
                            charges[_field] = _normalized
                            break
        except Exception:
            pass

        # Extra: "14% Fuel" style (number before keyword)
        if "fuel" not in charges:
            m = re.search(r'(\d+(?:\.\d+)?)\s*%\s*fuel', lower)
            if m:
                charges["fuel"] = float(m.group(1))

        # "1CFT=10kg" → divisor = 28316.8 / kg_per_cft (TCI surface formula)
        # The _cft_kg pattern captured kg/CFT; convert to cm³/kg now.
        if "_cft_kg" in charges:
            kg_per_cft = float(charges.pop("_cft_kg"))
            if 0 < kg_per_cft < 200:
                divisor_cm3 = round(28316.8 / kg_per_cft)
                if "divisor" not in charges:
                    charges["divisor"]  = float(divisor_cm3)
                    charges["kFactor"]  = float(divisor_cm3)
                    print(f"[PDFParser] 1CFT={kg_per_cft}kg → divisor={divisor_cm3} cm³/kg")

        # FOD (Freight on Delivery) is TCI's term for topay
        if "topayCharges" not in charges:
            fod_m = re.search(
                rf"fod\s*(?:\(freight\s*on\s*delivery\))?{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
                lower
            )
            if fod_m:
                val = float(fod_m.group(1))
                if val < 5000:
                    charges["topayCharges"] = {"v": 0.0, "f": val}
                    print(f"[PDFParser] FOD → topayCharges.f={val}")

        # COD = "Cheque/DD on Delivery" — TCI label
        if "codCharges" not in charges:
            cod_m = re.search(
                rf"cod\s*(?:\(cheque[/\\\s]*dd\s*on\s*delivery\))?{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
                lower
            )
            if cod_m:
                val = float(cod_m.group(1))
                if val < 5000:
                    charges["codCharges"] = {"v": 0.0, "f": val}

        # DACC = "Delivery Against Consignee Copy" — TCI label
        if "daccCharges" not in charges:
            dacc_m = re.search(
                rf"dacc\s*(?:\(delivery\s*against\s*consignee\s*copy\))?{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
                lower
            )
            if dacc_m:
                val = float(dacc_m.group(1))
                if val < 10000:
                    charges["daccCharges"] = val

        return charges

    # ─── Company extraction ───────────────────────────────────────────────────

    def _extract_company_from_text(self, text: str) -> Dict:
        info: Dict = {}
        for field, pattern in _COMPANY_PATTERNS.items():
            m = re.search(pattern, text, re.IGNORECASE)
            if not m:
                continue
            val = m.group(1).strip()
            if field == "contactPhone":
                val = re.sub(r'[\s\-\+\(\)]', '', val)
                # Strip country code +91 or 0091
                if val.startswith("0091") and len(val) == 14:
                    val = val[4:]
                elif val.startswith("91") and len(val) == 12:
                    val = val[2:]
                # Strip leading 0 from landline if result is 11 digits (0XX-XXXXXXXX)
                # Keep as-is — landlines with 0 prefix are valid
            elif field in ("gstNo", "cinNo"):
                val = val.upper()
            elif field == "contactEmail":
                val = val.lower()
            elif field == "address":
                # Also extract pincode embedded in address (e.g. "Jaipur - 302001")
                # if no explicit pin: label was found
                if "pincode" not in info:
                    pin_m = re.search(r'\b([1-9]\d{5})\b', val)
                    if pin_m:
                        info["pincode"] = pin_m.group(1)
                # Strip the pincode from the address value for cleanliness
                val = re.sub(r'[\s\-,]+[1-9]\d{5}\b\s*$', '', val).strip()
            info[field] = val
        return info

    # ─── Pincode extraction ───────────────────────────────────────────────────

    def _extract_pincodes_from_table(self, table: List[List[str]]) -> List[int]:
        pincodes: List[int] = []
        for row in table:
            for cell in row:
                for m in re.finditer(r'\b([1-9]\d{5})\b', str(cell)):
                    pin = int(m.group(1))
                    if 100000 <= pin <= 999999:
                        pincodes.append(pin)
        return pincodes

    def _extract_pincodes_from_text(self, text: str) -> List[int]:
        all_matches = re.findall(r'\b([1-9]\d{5})\b', text)
        pincodes = [int(p) for p in all_matches if 100000 <= int(p) <= 999999]
        if len(pincodes) >= 20:   # only if dense — avoid false positives
            return pincodes
        return []

    # ─── Text to rows ─────────────────────────────────────────────────────────

    def _text_to_rows(self, text: str) -> List[List[str]]:
        rows: List[List[str]] = []
        for line in text.split("\n"):
            line = line.strip()
            if not line or line.startswith("---"):
                continue
            parts = re.split(r"\t|\s{2,}|\|", line)
            parts = [p.strip() for p in parts if p.strip()]
            if parts:
                rows.append(parts)
        return rows
