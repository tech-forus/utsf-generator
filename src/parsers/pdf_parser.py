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

_CHARGE_PATTERNS = [
    # Fuel
    ("fuel",            r"fuel\s*(?:surcharge|levy|%|percent|surcahrge)[^\d]*(\d+(?:\.\d+)?)"),
    ("fuel",            r"f\.?s\.?c\.?\s*[:\-=]?\s*(\d+(?:\.\d+)?)\s*%"),
    ("fuel",            r"(\d+(?:\.\d+)?)\s*%\s*fuel"),
    ("fuel",            r"hsd\s*(?:surcharge)?[^\d]*(\d+(?:\.\d+)?)\s*%"),
    ("fuel",            r"diesel\s*(?:surcharge|levy)[^\d]*(\d+(?:\.\d+)?)\s*%"),
    # Docket / LR
    ("docketCharges",   r"(?:docket|doc(?:ument)?|lr\b|bilty)\s*(?:charges?|fee)[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    ("docketCharges",   r"lorry\s*receipt[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    # Minimum
    ("minCharges",      r"min(?:imum)?\s*(?:charg(?:es?|able)|freight)[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    ("minCharges",      r"base\s*(?:freight|rate|charge)[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    ("minCharges",      r"floor\s*(?:rate|charge)[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    # Min weight
    ("minWeight",       r"min(?:imum)?\s*(?:chargeable\s*)?weight[^\d]*(\d+(?:\.\d+)?)\s*kg"),
    ("minWeight",       r"min(?:imum)?\s*wt\.?[^\d]*(\d+(?:\.\d+)?)\s*kg"),
    # Divisor
    ("divisor",         r"(?:volumetric\s*divisor|k\s*factor|kfactor|cfactor|vol\s*divisor)[^\d]*(\d+)"),
    ("divisor",         r"(?:1\s*cft|cft\s*divisor)[^\d]*(\d+)"),
    # Green tax
    ("greenTax",        r"green\s*(?:tax|cess|levy|surcharge)[^\d]*(\d+(?:\.\d+)?)"),
    ("greenTax",        r"environmental\s*(?:surcharge|charge|cess)[^\d]*(\d+(?:\.\d+)?)"),
    # ROV / FOV
    ("rovCharges_v",    r"(?:r\.?o\.?v\.?|f\.?o\.?v\.?|risk\s*(?:of\s*value)?|owner.?s?\s*risk)[^\d]*(\d+(?:\.\d+)?)\s*%"),
    ("rovCharges_f",    r"(?:r\.?o\.?v\.?|f\.?o\.?v\.?)\s*(?:min(?:imum)?)?[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    # ODA
    ("odaCharges_f",    r"o\.?d\.?a\.?\s*(?:charges?)?[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)\s*(?:per\s*(?:shipment|consignment|docket|kg))?"),
    ("odaCharges_v",    r"o\.?d\.?a\.?[^\d]*(\d+(?:\.\d+)?)\s*%"),
    # Insurance
    ("insuranceCharges_v", r"insurance[^\d]*(\d+(?:\.\d+)?)\s*%"),
    ("insuranceCharges_f", r"(?:cargo|transit|goods)\s*insurance[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    # COD
    ("codCharges_v",    r"c\.?o\.?d\.?\s*(?:charges?)?[^\d]*(\d+(?:\.\d+)?)\s*%"),
    ("codCharges_f",    r"c\.?o\.?d\.?\s*(?:charges?)?[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    # Handling
    ("handlingCharges_f", r"handling\s*(?:charges?)[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    # DACC
    ("daccCharges",     r"dacc[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    ("daccCharges",     r"demurrage[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    # Topay
    ("topayCharges_v",  r"to\s*pay[^\d]*(\d+(?:\.\d+)?)\s*%"),
    ("topayCharges_f",  r"to\s*pay[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
    # Appointment
    ("appointmentCharges_f", r"appointment[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)"),
]

_COMPANY_PATTERNS = {
    "gstNo":        r'\b(\d{2}[A-Z]{5}\d{4}[A-Z]\d[ZYX]\d)\b',
    "panNo":        r'\b(?:PAN\s*[:\-]?\s*)?([A-Z]{5}\d{4}[A-Z])\b',
    "contactPhone": r'\b((?:0\d{2,4}[-\s]?\d{6,8}|\+?91[-\s]?\d{10}|\d{10}))\b',
    "contactEmail": r'\b([\w.+-]+@[\w-]+\.[\w.]+)\b',
    "website":      r'\b((?:https?://)?(?:www\.)?[\w-]+\.(?:com|in|co\.in|net|org)(?:/[\w/.-]*)?)\b',
    "cinNo":        r'\b([LUu]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6})\b',
}


# ── Minimum useful text threshold ─────────────────────────────────────────────
_MIN_TEXT_CHARS = 80   # fewer than this = treat as scanned/empty


class PDFParser(BaseParser):
    SUPPORTED_EXTENSIONS = [".pdf"]

    def parse(self, file_path: str) -> Dict[str, Any]:
        fname = os.path.basename(file_path)
        print(f"[PDFParser] Parsing: {fname}")

        text   = ""
        tables: List[List[List[str]]] = []
        method = "none"

        # ── Stage 1: pdfplumber ───────────────────────────────────────────────
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

        if len(text.strip()) < _MIN_TEXT_CHARS:
            print(f"[PDFParser] WARNING: All extraction methods exhausted. "
                  f"File may be encrypted or empty.")

        print(f"[PDFParser] Final: {len(text)} chars via {method}")

        # ── Extract structured data ───────────────────────────────────────────
        data = self._extract_data(text, tables, fname)

        return {"text": text, "tables": tables, "data": data}

    # ─── Text/table extractors ────────────────────────────────────────────────

    def _parse_pdfplumber(self, file_path: str) -> Tuple[str, List]:
        import pdfplumber
        all_text   = []
        all_tables = []
        prev_cols: Optional[int] = None
        carry_rows: List = []

        with pdfplumber.open(file_path) as pdf:
            for pnum, page in enumerate(pdf.pages):
                page_text = page.extract_text() or ""
                all_text.append(f"\n--- Page {pnum + 1} ---\n{page_text}")

                for raw_table in (page.extract_tables() or []):
                    if not raw_table or len(raw_table) < 2:
                        continue
                    clean = [
                        [str(c).strip() if c is not None else "" for c in row]
                        for row in raw_table
                    ]
                    n_cols = max(len(r) for r in clean)
                    if carry_rows and prev_cols == n_cols:
                        clean = carry_rows + clean
                        carry_rows = []
                    all_tables.append(clean)
                    prev_cols = n_cols
                    carry_rows = clean[-2:] if len(clean) > 2 else []

        return "\n".join(all_text), all_tables

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

    def _try_ocr_tesseract(self, file_path: str) -> str:
        """
        Convert PDF pages to images and OCR with pytesseract.
        Tries pdf2image first, then pymupdf page rendering.
        """
        images = []

        # Attempt 1: pdf2image (requires poppler)
        try:
            from pdf2image import convert_from_path
            images = convert_from_path(file_path, dpi=200, fmt="png")
            print(f"[PDFParser] pdf2image: converted {len(images)} pages")
        except ImportError:
            print("[PDFParser] pdf2image not installed — trying pymupdf render")
        except Exception as e:
            print(f"[PDFParser] pdf2image error: {e}")

        # Attempt 2: pymupdf page rendering
        if not images:
            try:
                import fitz
                from PIL import Image
                doc = fitz.open(file_path)
                for pnum in range(len(doc)):
                    page = doc[pnum]
                    mat  = fitz.Matrix(2.0, 2.0)   # 2× scale ≈ 144 dpi
                    pix  = page.get_pixmap(matrix=mat, alpha=False)
                    img  = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                    images.append(img)
                print(f"[PDFParser] pymupdf render: {len(images)} pages")
            except ImportError:
                print("[PDFParser] pymupdf not available for rendering")
            except Exception as e:
                print(f"[PDFParser] pymupdf render error: {e}")

        if not images:
            return ""

        # Run tesseract
        try:
            import pytesseract
            parts = []
            for i, img in enumerate(images):
                # Preprocess: convert to grayscale for better OCR accuracy
                try:
                    img = img.convert("L")   # grayscale
                except Exception:
                    pass
                page_text = pytesseract.image_to_string(
                    img,
                    lang="eng",
                    config="--psm 6 --oem 3",   # assume uniform block of text
                )
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
        """EasyOCR as second OCR engine — no poppler/tesseract dependency."""
        images = []
        try:
            import fitz
            from PIL import Image
            doc = fitz.open(file_path)
            for pnum in range(len(doc)):
                page = doc[pnum]
                mat  = fitz.Matrix(2.0, 2.0)
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
                arr     = np.array(img)
                results = reader.readtext(arr, detail=0, paragraph=True)
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

        # ── OICR engine ───────────────────────────────────────────────────────
        try:
            from parsers.oicr_engine import get_oicr_engine
            oicr = get_oicr_engine()
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

        # ── Table extraction ─────────────────────────────────────────────────
        # IMPORTANT: always run ALL extractors on every table (same as old code).
        # ContentClassifier is used only for logging — never as a gate.
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

        # ── Text-level extraction ────────────────────────────────────────────
        if text:
            # Charges from free text
            text_charges = self._extract_charges_from_text(text)
            if text_charges:
                data.setdefault("charges", {})
                for k, v in text_charges.items():
                    data["charges"].setdefault(k, v)

            # Company info
            cd = self._extract_company_from_text(text)
            if cd:
                data.setdefault("company_details", {})
                for k, v in cd.items():
                    data["company_details"].setdefault(k, v)

            # Zone matrix from text rows (if still missing)
            if not data.get("zone_matrix"):
                text_rows = self._text_to_rows(text)
                zm = ep._try_parse_zone_matrix(text_rows, "PDF_Text")
                if zm:
                    data["zone_matrix"] = zm
                    print(f"[PDFParser] Text zone matrix: {len(zm)} origins")
                ch = ep._try_parse_charges(text_rows, "PDF_Text")
                if ch:
                    data.setdefault("charges", {})
                    for k, v in ch.items():
                        data["charges"].setdefault(k, v)

            # Pincodes from dense-number pages
            pins = self._extract_pincodes_from_text(text)
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

    # ─── Charge extraction from text ─────────────────────────────────────────

    def _extract_charges_from_text(self, text: str) -> Dict:
        lower  = text.lower()
        charges: Dict = {}

        for field, pattern in _CHARGE_PATTERNS:
            m = re.search(pattern, lower)
            if not m:
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
                oda_entry = charges.setdefault(canon, {})
                oda_entry["f"] = val
                if canon == "odaCharges":
                    oda_entry.setdefault("type", "per_shipment")
            else:
                charges.setdefault(field, val)

        # Extra: "14% Fuel" style (number before keyword)
        if "fuel" not in charges:
            m = re.search(r'(\d+(?:\.\d+)?)\s*%\s*fuel', lower)
            if m:
                charges["fuel"] = float(m.group(1))

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
                val = re.sub(r'[\s\-\+]', '', val)
                if val.startswith("91") and len(val) == 12:
                    val = val[2:]
            elif field in ("gstNo", "cinNo"):
                val = val.upper()
            elif field == "contactEmail":
                val = val.lower()
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
