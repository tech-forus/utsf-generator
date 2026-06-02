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

# ─── Tesseract Auto-Configuration ───────────────────────────────────────────
try:
    import pytesseract
    # Common Windows installation path
    _win_tess = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    try:
        pytesseract.get_tesseract_version()
    except Exception:
        if os.path.exists(_win_tess):
            pytesseract.pytesseract.tesseract_cmd = _win_tess
except ImportError:
    pass

from parsers.base_parser import BaseParser


# ── Regex patterns for charge extraction from free text ──────────────────────
# Expanded from 10 to 60+ patterns matching all canonical UTSF charge fields

# Sanity caps for fixed charge fields — values above these are almost certainly
# parsing artefacts (e.g. volumetric constant 1728 = 1 CFT in cubic inches).
_CHARGE_SANITY_MAX: Dict[str, float] = {
    # IQR-derived caps from 12 real transporter UTSFs
    # (replaced hardcoded values by training_pipeline.py)
    "docketCharges":      636,
    "daccCharges":        10000,
    "codCharges":         5000,
    "handlingCharges":    50000,
    "greenTax":           2000,
    "minCharges":         1250,
    "odaCharges":         50000,
    "topayCharges":       5000,
    "appointmentCharges": 10000,
    "fuel":               35.0,   # real max=30 (TCI/Gati), allow 35
    "gst":                50.0,
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
    # "fuel surcharge ... @ 18%" / "FSC @ 18%" / "fuel surcharge applicable on ... @ 18%"
    ("fuel",            r"fuel\s*surcharge[^%\n]{0,80}@\s*(\d+(?:\.\d+)?)\s*%"),
    ("fuel",            r"f\.?s\.?c\.?[^%\n]{0,40}@\s*(\d+(?:\.\d+)?)\s*%"),
    # "Current applicable rate: 18 percent" / "rate: 18 pct"
    ("fuel",            r"(?:current\s*(?:applicable\s*)?rate|fsc\s*rate)\s*[:\-]?\s*(\d+(?:\.\d+)?)\s*(?:percent|pct|%)"),
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
    # "Green / Environment Cess: Rs. 40/-" — slash + word before cess keyword
    ("greenTax",        rf"green\s*[/\s]\s*environment\s*cess{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    ("greenTax",        rf"ngt\s*charge{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),  # TCI "NGT Charge Delhi/NCR"
    ("greenTax",        rf"environmental\s*(?:surcharge|charge|cess){_NL}(\d+(?:\.\d+)?)"),
    ("greenTax",        rf"ecology\s*(?:charge|cess){_NL}(\d+(?:\.\d+)?)"),
    ("greenTax",        rf"pollution\s*(?:charge|cess){_NL}(\d+(?:\.\d+)?)"),

    # ── ROV / FOV ─────────────────────────────────────────────────────────────
    ("rovCharges_v",    rf"(?:r\.?o\.?v\.?|f\.?o\.?v\.?|risk\s*(?:of\s*value)?|owner.?s?\s*risk){_NL}(\d+(?:\.\d+)?)\s*%"),
    ("rovCharges_f",    rf"(?:r\.?o\.?v\.?|f\.?o\.?v\.?)\s*(?:min(?:imum)?)?{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),

    # ── ODA ───────────────────────────────────────────────────────────────────
    # "ODA Charges: 850 INR or 4 INR/kg (whichever is higher)" — captures f=850, v=4
    ("odaCharges_fv",   rf"o\.?d\.?a\.?\s*(?:charges?)?{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)\s*(?:inr|rs\.?)?\s*or\s*(\d+(?:\.\d+)?)\s*(?:inr|rs\.?)?\s*/\s*kg"),
    ("odaCharges_f",    rf"o\.?d\.?a\.?\s*(?:charges?)?{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)\s*(?:per\s*(?:shipment|consignment|docket|kg))?"),
    ("odaCharges_v",    rf"o\.?d\.?a\.?{_NL}(\d+(?:\.\d+)?)\s*%"),
    # Insurance
    ("insuranceCharges_v", rf"insurance{_NL}(\d+(?:\.\d+)?)\s*%"),
    ("insuranceCharges_f", rf"(?:cargo|transit|goods)\s*insurance{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    # COD — f pattern REQUIRES Rs/₹ so it doesn't steal the % value
    ("codCharges_v",    rf"c\.?o\.?d\.?\s*(?:charges?)?{_NL}(\d+(?:\.\d+)?)\s*%"),
    ("codCharges_f",    rf"c\.?o\.?d\.?\s*(?:charges?)?{_NL}(?:rs\.?|₹|inr)\s*(\d+(?:\.\d+)?)"),
    # Handling
    ("handlingCharges_f", rf"handling\s*(?:charges?){_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    # DACC — allow cross-line since "DACC (long description):\nRs. 75" is common
    ("daccCharges",     rf"dacc{_ANY}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    ("daccCharges",     rf"demurrage{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    # Topay — require "charges/surcharge" OR ":" (label:value format like "To Pay: Rs.55")
    ("topayCharges_v",  rf"to\s*pay\s*(?:charges?|surcharge){_NL}(\d+(?:\.\d+)?)\s*%"),
    ("topayCharges_f",  rf"to\s*pay\s*(?:charges?|surcharge){_NL}(?:rs\.?|₹|inr)\s*(\d+(?:\.\d+)?)"),
    ("topayCharges_f",  rf"to\s*pay\s*[:\-]{_NL}(?:rs\.?|₹|inr)\s*(\d+(?:\.\d+)?)"),  # "To Pay: Rs.55"
    # Appointment
    ("appointmentCharges_f", rf"appointment{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
]

_COMPANY_PATTERNS = {
    # GST: accept with or without spaces/dashes between segments
    # Standard: 29AAACR5055K1ZB  |  Spaced: 29 AAACR 5055 K 1 Z B
    # Dashed: 36-BBCFS-9921K-2ZT  |  Last two chars can be space-separated: "Z B"
    "gstNo":        r'\b(\d{2}[\s\-]*[A-Z]{5}[\s\-]*\d{4}[\s\-]*[A-Z][\s\-]*\d[\s\-]*[A-Z0-9][\s\-]*[A-Z0-9])\b',
    # PAN: accept with or without spaces between segments (BB CFS 9921 K → BBCFS9921K)
    "panNo":        r'\b(?:PAN\s*[:\-]?\s*)?([A-Z]{2}[\s\-]*[A-Z]{3}[\s\-]*\d{4}[\s\-]*[A-Z])\b',
    # Phone: also handles dashes within local part (+91-40-6789 0101, 040-6789 0101)
    "contactPhone": r'(\+?91[\s\-]?(?:\d[\s\-]?){9}\d|0\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}|\b\d{10}\b)',
    # Email: also match "word @ domain" with spaces around @
    "contactEmail": r'\b([\w.+-]+\s*@\s*[\w-]+\.[\w.]+)\b',
    "website":      r'\b((?:https?://)?(?:www\.)?[\w-]+\.(?:com|in|co\.in|net|org)(?:/[\w/.-]*)?)\b',
    "cinNo":        r'\b([LUu]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6})\b',
    # Address: labeled "Registered Address:", "Address:", "principal place of business at"
    "address":      r'(?:registered\s+address|regd\.?\s*(?:office|addr\.?)|address|reg\.\s*office|principal\s+place\s+of\s+business\s+at)\s*[:\-]?\s*(.{10,120})',
    # Rating: "Rating: 4.2", "Grade 3.9 out of 5", "3.9 / 5"
    "rating":       r'(?:rating|grade|score)\s*[:\-]?\s*(\d+(?:\.\d+)?)\s*(?:/\s*5|out\s+of\s+5)?',
    # Company name: "XYZ Pvt. Ltd. (hereinafter referred to as 'the Vendor')"
    "companyName":  r'^([A-Z][A-Za-z\s\.\-&,]+(?:Pvt\.?\s*Ltd\.?|Private\s+Limited|Ltd\.?|LLP|Corp\.?))',
    # City: "City: Hyderabad" or "HQ City & State: Hyderabad, Telangana"
    "city":         r'(?:city|hq\s*city[^:]*)\s*[:\-]\s*([A-Za-z][A-Za-z\s]+?)(?:,|\s*[–\-]\s*\d|\n|$)',
    # State: "State: Telangana" or parsed from "city, state" pattern
    "state":        r'(?:state|province)\s*[:\-]\s*([A-Za-z][A-Za-z\s]+?)(?:,|\s*[–\-]\s*\d|\n|$)',
    # Pincode: labeled "Pincode: 500032" or after dash "– 500032"
    "contact_pincode": r'(?:pincode|pin\s*code|zip)\s*[:\-]\s*([1-9]\d{5})',
}


# ── Text quality thresholds ───────────────────────────────────────────────────
_MIN_TEXT_CHARS  = 80   # truly empty — always fall through to next engine
_SCANNED_CHARS   = 500  # text present but no tables — likely just PDF metadata
                        # from a scanned/image-based PDF; fall through to OCR

# ── Parse result cache (process-lifetime, keyed on file path + mtime) ─────────
# Prevents the same scanned PDF being run through Docling twice in one session
# (main.py parses each file with a primary parser then a fallback pass).
# Key: (abspath, mtime_int) → (text, tables)
_PDF_PARSE_CACHE: dict = {}


def _is_thin(text: str, tables: list) -> bool:
    """
    Return True when the current extraction result is too thin to stop at.

    Two triggers:
      1. Truly empty   : fewer than 80 chars (garbage or empty page)
      2. Scanned-PDF   : fewer than 500 chars AND zero tables extracted
                         (pdfplumber grabbed only font/metadata artefacts, not content)

    Scanned rate-card PDFs typically produce 100-300 chars and 0 tables from
    pdfplumber even though they contain rich tabular data — OCR is required.
    """
    chars = len(text.strip())
    if chars < _MIN_TEXT_CHARS:
        return True
    if chars < _SCANNED_CHARS and not tables:
        return True
    return False


class PDFParser(BaseParser):
    SUPPORTED_EXTENSIONS = [".pdf"]

    def parse(self, file_path: str, doc_context=None) -> Dict[str, Any]:
        fname = os.path.basename(file_path)

        # ── Cache check ───────────────────────────────────────────────────────
        # Scanned PDFs go through Docling (90–300 s). main.py parses each file
        # with a primary parser then a fallback pass — without this cache that
        # doubles the Docling cost. Cache key = (abspath, mtime) so stale
        # entries are automatically invalidated when the user re-uploads a file.
        try:
            _abs  = os.path.abspath(file_path)
            _mtime = int(os.path.getmtime(_abs))
            _cache_key = (_abs, _mtime)
            if _cache_key in _PDF_PARSE_CACHE:
                print(f"[PDFParser] Cache hit — skipping re-parse of {fname}")
                return _PDF_PARSE_CACHE[_cache_key]
        except OSError:
            _cache_key = None

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
        if _is_thin(text, tables):
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
        if _is_thin(text, tables):
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
        if _is_thin(text, tables):
            try:
                pypdf_text = self._parse_pypdf2(file_path)
                if len(pypdf_text.strip()) > len(text.strip()):
                    text   = pypdf_text
                    method = "pypdf"
                    print(f"[PDFParser] pypdf: {len(text)} chars")
            except Exception as e:
                print(f"[PDFParser] pypdf error: {e}")

        # ── Stage 5: OCR (pytesseract) ────────────────────────────────────────
        if _is_thin(text, tables):
            if len(text.strip()) >= _MIN_TEXT_CHARS:
                print(f"[PDFParser] Scanned PDF detected ({len(text.strip())} chars, "
                      f"0 tables) — falling through to OCR")
            else:
                print(f"[PDFParser] Text layer empty/thin — attempting OCR")
            ocr_text, ocr_tables = self._try_ocr_tesseract(file_path)
            if len(ocr_text.strip()) > len(text.strip()):
                text   = ocr_text
                method = "tesseract_ocr"
                print(f"[PDFParser] Tesseract OCR: {len(text)} chars, "
                      f"{len(ocr_tables)} tables")
            if ocr_tables:
                tables = ocr_tables

        # ── Stage 6: EasyOCR fallback ─────────────────────────────────────────
        if _is_thin(text, tables):
            ocr_text = self._try_ocr_easyocr(file_path)
            if len(ocr_text.strip()) > len(text.strip()):
                text   = ocr_text
                method = "easyocr"
                print(f"[PDFParser] EasyOCR: {len(text)} chars")

        # ── Stage 7: Docling — LAST RESORT for scanned PDFs only ─────────────
        # Docling loads ~300MB of ML layout models — expensive first-time cost.
        # Models are cached as a process-level singleton via ImageParser so the
        # 300MB load cost is paid ONCE per session, not once per file.
        # This stage also extracts structured Table objects (not just markdown)
        # so downstream ExcelParser can process them without a second OCR pass.
        if _is_thin(text, tables):
            print(f"[PDFParser] All text extractors failed — trying Docling (ML layout)")
            try:
                import concurrent.futures
                from parsers.image_parser import ImageParser as _IP

                # Reuse the singleton converter from ImageParser — avoids the
                # 300MB re-load that was happening every time this block fired.
                dc = _IP._get_docling_converter()
                if dc is None:
                    raise ImportError("Docling unavailable")

                # Estimate page count for timeout scaling
                try:
                    import pypdfium2 as _pdfium
                    _page_count = len(_pdfium.PdfDocument(file_path))
                except Exception:
                    _page_count = 10

                _docling_timeout = max(120, _page_count * 20)

                def _run_docling():
                    result = dc.convert(file_path)
                    doc    = result.document
                    # ── Extract structured tables (the real Docling superpower) ──
                    # Convert each detected Table object to a list-of-lists so
                    # ExcelParser can parse it with full zone/charge detection.
                    extracted_tables = []
                    for tbl in doc.tables:
                        try:
                            rows = []
                            for row in tbl.data.grid:
                                rows.append([str(cell.text).strip() for cell in row])
                            if rows:
                                extracted_tables.append(rows)
                        except Exception:
                            pass
                    return doc.export_to_markdown(), extracted_tables

                print(f"[PDFParser] Docling: {_page_count} pages, "
                      f"timeout={_docling_timeout}s (singleton converter)")
                executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
                future = executor.submit(_run_docling)
                try:
                    docling_text, docling_tables = future.result(timeout=_docling_timeout)
                    if len(docling_text.strip()) > len(text.strip()):
                        text   = docling_text
                        method = "docling"
                        print(f"[PDFParser] Docling: {len(text)} chars, "
                              f"{len(docling_tables)} structured tables")
                    # Merge Docling-detected tables (structured beats text-derived)
                    if docling_tables:
                        tables = docling_tables + tables
                    executor.shutdown(wait=False)
                except concurrent.futures.TimeoutError:
                    print(f"[PDFParser] Docling timed out after {_docling_timeout}s — skipping")
                    future.cancel()
                    executor.shutdown(wait=False)
            except ImportError:
                pass
            except Exception as _dl_err:
                print(f"[PDFParser] Docling error: {_dl_err}")

        if _is_thin(text, tables):
            print(f"[PDFParser] WARNING: All extraction methods exhausted. "
                  f"File may be encrypted or empty.")

        print(f"[PDFParser] Final: {len(text)} chars via {method}")

        # ── Extract structured data ───────────────────────────────────────────
        data = self._extract_data(text, tables, fname, file_path=file_path)

        result = {"text": text, "tables": tables, "data": data}

        # ── Store in cache (only if Docling or OCR was involved — cheap parses
        # don't need caching, but never hurts) ───────────────────────────────
        if _cache_key:
            _PDF_PARSE_CACHE[_cache_key] = result

        return result

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

    def _try_ocr_tesseract(self, file_path: str):
        """
        Convert PDF pages to images and OCR with pytesseract.
        Tries pdf2image first (300 dpi), then pymupdf page rendering (3× scale).

        Returns (text: str, tables: List[List[List[str]]]).

        For each page, runs two passes:
          1. image_to_string  — raw text (charges, company info)
          2. HOCR table reconstruction — structured rows for zone/pincode tables
             Uses ImageParser._hocr_reconstruct_table() so scanned rate cards
             produce the same List[row][cell] format as digital PDFs.
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

        # Attempt 3: pypdfium2 rendering (300 dpi) — available when fitz/pdf2image are not
        if not images:
            try:
                import pypdfium2 as pdfium
                from PIL import Image
                doc = pdfium.PdfDocument(file_path)
                for pnum in range(len(doc)):
                    page = doc[pnum]
                    # scale=4 ≈ 300 dpi for a 72-dpi PDF page
                    bitmap = page.render(scale=4, rotation=0)
                    img = bitmap.to_pil()
                    images.append(img)
                print(f"[PDFParser] pypdfium2 render: {len(images)} pages @ 300 dpi")
            except ImportError:
                print("[PDFParser] pypdfium2 not available for rendering")
            except Exception as e:
                print(f"[PDFParser] pypdfium2 render error: {e}")

        if not images:
            return "", []

        # ── Vision LLM pass (primary for scanned PDFs) ───────────────────────
        # Send each page image to qwen2.5-vl before falling through to Tesseract.
        # If vision produces zone_matrix + charges, store in _vision_data so
        # the calling parse() method can use it; still run Tesseract for text.
        try:
            from intelligence.ollama_client import VisionExtractor
            ve = VisionExtractor.get()
            if ve.is_available():
                self._vision_pages = []
                for i, img in enumerate(images):
                    vr = ve.extract_from_image(img)
                    if vr:
                        self._vision_pages.append(vr)
                        print(f"[PDFParser] Page {i+1} vision: "
                              f"zones={len(vr.get('zone_matrix') or {})} "
                              f"charges={len(vr.get('charges') or {})}")
        except Exception as _ve:
            print(f"[PDFParser] Vision pass error: {_ve}")

        # ── Import HOCR table extractor (reuse ImageParser's spatial reconstruction) ─
        try:
            import pytesseract
            from parsers.image_parser import ImageParser
            _img_parser = ImageParser()
        except ImportError:
            print("[PDFParser] pytesseract not installed")
            return "", []

        parts: List[str] = []
        all_tables: List[List[List[str]]] = []

        try:
            for i, img in enumerate(images):
                processed = self._preprocess_for_ocr(img)

                # ── Pass 1: raw text (PSM 3 preferred, PSM 6 fallback) ───────
                page_text = ""
                for psm in ("--psm 3 --oem 3", "--psm 6 --oem 3"):
                    try:
                        page_text = pytesseract.image_to_string(
                            processed, lang="eng", config=psm)
                        if len(page_text.strip()) > 50:
                            break
                    except Exception:
                        page_text = ""
                parts.append(f"\n--- Page {i+1} ---\n{page_text}")

                # ── Pass 2: HOCR spatial table reconstruction ─────────────────
                try:
                    hocr_table = _img_parser._hocr_reconstruct_table(processed, pytesseract)
                    if hocr_table:
                        all_tables.append(hocr_table)
                        print(f"[PDFParser] Page {i+1} HOCR: "
                              f"{len(hocr_table)} rows × {len(hocr_table[0])} cols")
                except Exception as _he:
                    pass

            result = "\n".join(parts)
            print(f"[PDFParser] Tesseract: {len(result)} chars, "
                  f"{len(all_tables)} tables from {len(images)} pages")
            return result, all_tables

        except Exception as e:
            print(f"[PDFParser] tesseract error: {e}")
            return "", []

    def _try_ocr_easyocr(self, file_path: str) -> str:
        """EasyOCR as second OCR engine — no poppler/tesseract dependency.
        Pages are preprocessed via _preprocess_for_ocr before reading."""
        images = []
        # Try fitz first, fall back to pypdfium2 for rendering
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
        except ImportError:
            try:
                import pypdfium2 as pdfium
                from PIL import Image
                doc = pdfium.PdfDocument(file_path)
                for pnum in range(len(doc)):
                    bitmap = doc[pnum].render(scale=4, rotation=0)
                    images.append(bitmap.to_pil())
            except Exception:
                return ""
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
        self, text: str, tables: List[List[List[str]]], fname: str = "",
        file_path: str = ""
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
            # TRANSIT_DAYS (day counts), AIR_FREIGHT (air-mode rates ≠ road rates).
            _BLOCKED_CATS = frozenset(("VOLUMETRIC", "LEGAL", "TRANSIT_DAYS", "AIR_FREIGHT"))
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

        # ── Detect hub origin from full text BEFORE section filter strips it ────
        # "EX DELHI" / "FROM MUMBAI" phrases appear in rate-table sections that
        # the section filter blocks from OICR. Detect here and forward explicitly.
        _hub_zone_hint: Optional[str] = None
        try:
            _hub_match = re.search(
                r'\bex[.\s]+([A-Z][A-Za-z\s/]{2,20}?)(?:\s*\n|\s{2,}|$)',
                text, re.IGNORECASE | re.MULTILINE
            ) or re.search(
                r'\bfrom[.\s]+([A-Z][A-Za-z\s/]{2,20}?)(?:\s*\n|\s{2,}|$)',
                text, re.IGNORECASE | re.MULTILINE
            )
            if _hub_match:
                from parsers.oicr_engine import city_to_zones
                _hub_city = _hub_match.group(1).strip().upper()
                _hz = city_to_zones(_hub_city)
                if _hz:
                    _hub_zone_hint = _hz[0]
                    print(f"[PDFParser] Hub origin pre-detected: '{_hub_city}' -> {_hub_zone_hint}")
        except Exception as _hz_err:
            print(f"[PDFParser] Hub origin detection error: {_hz_err}")

        # ── OICR engine — runs on section-filtered text, not full text ────────
        # Pre-process tables BEFORE OICR sees them — pick SFC row from multi-service tables
        tables = [self._pick_surface_row(t) for t in tables]

        _OICR_SANITY = {
            "docketCharges": 2000, "daccCharges": 10000, "codCharges": 5000,
            "greenTax": 2000,      "minCharges": 50000,  "topayCharges": 5000,
            "handlingCharges": 50000, "odaCharges": 50000,
        }
        try:
            from parsers.oicr_engine import get_oicr_engine
            oicr = get_oicr_engine()
            oicr_pdf = oicr.process_pdf_text(_charge_text_for_oicr, tables, hub_zone=_hub_zone_hint)
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

            # Zone matrix — always try all tables to handle multi-page splits
            zm = ep._try_parse_zone_matrix(table, f"PDF_Table_{ti}")
            if zm:
                existing = data.get("zone_matrix") or {}
                if len(zm) > len(existing):
                    data["zone_matrix"] = zm
                    print(f"[PDFParser] Table {ti}: zone matrix {len(zm)} origins total")
                elif existing and zm:
                    # Multi-page carry: merge new origins not already present
                    new_origs = {k: v for k, v in zm.items() if k not in existing}
                    if new_origs:
                        data["zone_matrix"] = {**existing, **new_origs}
                        print(f"[PDFParser] Table {ti}: merged {len(new_origs)} new origins -> {len(data['zone_matrix'])} total")

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

        # Zone recovery: PDF has text but no zone matrix → pages may have image-embedded tables
        if not data.get("zone_matrix") and text.strip():
            print(f"[PDFParser] Text found but no zone matrix — attempting page-image OCR recovery")
            try:
                self._zone_image_recovery(file_path, data, ep)
            except Exception as _rec_err:
                print(f"[PDFParser] Image recovery error: {_rec_err}")

        # ── Section-aware text-level extraction ─────────────────────────────
        if text:
            # Reuse sections already computed above (from pre-OICR segmentation)
            sections_map = _sections_map

            # Text-level charge extraction: use full text minus AIR_FREIGHT sections.
            # _extract_charges_from_text uses tight regexes, safe on most content,
            # but AIR_FREIGHT sections contain real charge keywords (DWB, Fuel, Min)
            # that belong to a different mode — strip them out before matching.
            # Other non-CHARGES sections (COMPANY_INFO etc.) are still included so
            # fuel / ROV charges that land in non-ideal sections aren't silently dropped.
            _air_sections = _sections_map.get("AIR_FREIGHT", [])
            if _air_sections:
                _air_texts = {s.text for s in _air_sections}
                _safe_parts = [
                    sec.text for secs in _sections_map.values()
                    for sec in secs if sec.text not in _air_texts
                ]
                _safe_text = "\n\n".join(_safe_parts) if _safe_parts else text
                print(f"[PDFParser] Air-section filter: stripped {len(_air_sections)} "
                      f"AIR_FREIGHT section(s) from regex charge extraction")
            else:
                _safe_text = text
            text_charges = self._extract_charges_from_text(_safe_text)
            if text_charges:
                data.setdefault("charges", {})
                for k, v in text_charges.items():
                    data["charges"].setdefault(k, v)

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

        # ── Passive learning: auto-confirm high-confidence extractions ────────
        self._passive_learn(data)

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

    def _zone_image_recovery(self, file_path: str, data: Dict, ep) -> None:
        """
        For PDFs with a text layer (company info) but image-only rate tables.
        Renders each page as a PIL image and runs ImageParser/Docling OCR.
        """
        try:
            import pdfplumber
            from parsers.image_parser import ImageParser
            import tempfile, os
            ip = ImageParser()
            with pdfplumber.open(file_path) as pdf:
                for pnum, page in enumerate(pdf.pages):
                    try:
                        print(f"[PDFParser] OCR Recovery: Rendering page {pnum+1}/{len(pdf.pages)}...")
                        pil_img = page.to_image(resolution=150).original
                        tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
                        pil_img.save(tmp.name)
                        tmp.close()
                        
                        print(f"[PDFParser] OCR Recovery: Running OCR on page {pnum+1}...")
                        result = ip.parse(tmp.name)
                        img_data = result.get("data", {})
                        if img_data.get("zone_matrix"):
                            existing = data.get("zone_matrix") or {}
                            zm = img_data["zone_matrix"]
                            if len(zm) > len(existing):
                                data["zone_matrix"] = zm
                                print(f"[PDFParser] Image recovery page {pnum+1}: {len(zm)} zone origins found")
                            elif existing and zm:
                                new_origs = {k: v for k, v in zm.items() if k not in existing}
                                if new_origs:
                                    data["zone_matrix"] = {**existing, **new_origs}
                                    print(f"[PDFParser] Image recovery page {pnum+1}: merged {len(new_origs)} new zone origins")
                        os.unlink(tmp.name)
                    except Exception as _pg_err:
                        print(f"[PDFParser] Page {pnum+1} recovery error: {_pg_err}")
        except Exception as e:
            print(f"[PDFParser] Zone image recovery failed: {e}")

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

            # _fv pattern: two capture groups — group(1)=fixed, group(2)=per-kg variable
            if field.endswith("_fv"):
                try:
                    f_val = float(m.group(1))
                    v_val = float(m.group(2))
                except (ValueError, IndexError):
                    continue
                canon = field[:-3]
                entry = charges.setdefault(canon, {})
                entry["f"] = f_val
                entry["v"] = v_val
                entry.setdefault("type", "per_kg_minimum")
                continue

            try:
                val = float(m.group(1))
            except (ValueError, IndexError):
                continue

            if field.endswith("_v"):
                canon = field[:-2]
                charges.setdefault(canon, {})["v"] = val
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
                    print(f"[PDFParser] 1CFT={kg_per_cft}kg -> divisor={divisor_cm3} cm3/kg")

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
                    print(f"[PDFParser] FOD -> topayCharges.f={val}")

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

        # INR preference: if docketCharges is suspiciously small (USD), look for INR equivalent
        if charges.get("docketCharges", 9999) < 10:
            _inr_m = re.search(r'(?:equivalent|equal)\s+to\s+(?:rs\.?|inr)\s*(\d+(?:\.\d+)?)', lower)
            if _inr_m:
                inr_val = float(_inr_m.group(1))
                if 50 < inr_val < 5000:
                    charges["docketCharges"] = inr_val

        return charges

    # ─── Company extraction ───────────────────────────────────────────────────

    def _extract_company_from_text(self, text: str) -> Dict:
        info: Dict = {}
        for field, pattern in _COMPANY_PATTERNS.items():
            try:
                m = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if not m:
                    continue
                val = (m.group(1) or "").strip()
                if not val:
                    continue
            except Exception:
                continue
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
                val = re.sub(r'\s*@\s*', '@', val).lower()
                if not re.match(r'^[\w.+-]+@[\w-]+\.[\w.]+$', val):
                    continue  # reject if still malformed after normalization
            elif field == "rating":
                try:
                    val = float(val)
                except ValueError:
                    continue
            elif field == "address":
                val = re.sub(r'\s+', ' ', val).strip().rstrip('.,;')
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
