"""
Image Parser — Advanced OCR Pipeline
======================================
Extracts text, tables, and structured logistics data from images
(PNG, JPG, TIFF, scanned PDFs rendered as images, photos of rate cards).

Pipeline:
  1. Image preprocessing (grayscale, denoising, adaptive threshold, deskew)
  2. Multi-pass OCR with different Tesseract PSM modes
  3. OpenCV-based table cell detection (if OpenCV available)
  4. Structured table assembly from detected cells
  5. Charge and zone data extraction via ExcelParser patterns
  6. Confidence scoring on extracted data

Install deps:
  pip install pytesseract Pillow opencv-python-headless
  (also install Tesseract OCR binary: https://github.com/tesseract-ocr/tesseract)
"""

import os
import re
from typing import Dict, Any, List, Optional, Tuple

from parsers.base_parser import BaseParser


class ImageParser(BaseParser):
    SUPPORTED_EXTENSIONS = [".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp", ".webp"]

    # ── OCR availability flags ─────────────────────────────────────────────────
    _TESSERACT_OK: Optional[bool] = None   # None = not yet tested
    _OPENCV_OK:    Optional[bool] = None

    @classmethod
    def _check_tesseract(cls) -> bool:
        if cls._TESSERACT_OK is None:
            try:
                import pytesseract
                pytesseract.get_tesseract_version()
                cls._TESSERACT_OK = True
            except Exception:
                cls._TESSERACT_OK = False
        return cls._TESSERACT_OK

    @classmethod
    def _check_opencv(cls) -> bool:
        if cls._OPENCV_OK is None:
            try:
                import cv2  # noqa
                cls._OPENCV_OK = True
            except ImportError:
                cls._OPENCV_OK = False
        return cls._OPENCV_OK

    # ── Public entry point ────────────────────────────────────────────────────

    def parse(self, file_path: str, doc_context=None) -> Dict[str, Any]:
        text       = ""
        tables     = []
        data: Dict = {}

        try:
            from PIL import Image
            img = Image.open(file_path)
            img.load()
        except Exception as e:
            print(f"[ImageParser] Cannot open image {os.path.basename(file_path)}: {e}")
            return {"text": "", "tables": [], "data": {}}

        if not self._check_tesseract():
            print("[ImageParser] Tesseract not available — skipping OCR")
            print("  Install: pip install pytesseract Pillow")
            print("  Also: https://github.com/tesseract-ocr/tesseract (binary)")
            return {"text": f"[Image: {os.path.basename(file_path)} — OCR not available]",
                    "tables": [], "data": {}}

        try:
            import pytesseract
            from PIL import Image, ImageEnhance, ImageFilter

            # ── Step 1: Preprocess ──────────────────────────────────────────
            processed = self._preprocess(img)

            # ── Step 2: HOCR spatial reconstruction (primary table extractor) ─
            # Uses image_to_data word-level bboxes with adaptive Y-tolerance.
            # Falls back to multi-pass OCR for text when HOCR table is thin.
            hocr_table = self._hocr_reconstruct_table(processed, pytesseract)
            if hocr_table:
                tables = [hocr_table]
                # Derive plain text from HOCR rows (for charge/company regex)
                text = "\n".join("  ".join(row) for row in hocr_table)
            else:
                text = self._multi_pass_ocr(processed, pytesseract)

            # ── Step 3: Structural table extraction (grid-based fallback) ───
            if not tables:
                if self._check_opencv():
                    tables = self._extract_tables_opencv(processed, pytesseract)
                else:
                    tables = self._extract_tables_tsv(processed, pytesseract)

            # ── Step 4: Parse extracted data ─────────────────────────────────
            data = self._parse_extracted(text, tables)

        except Exception as e:
            import traceback
            print(f"[ImageParser] OCR error: {e}")
            traceback.print_exc()

        return {"text": text, "tables": tables, "data": data}

    # ── Preprocessing ─────────────────────────────────────────────────────────

    def _preprocess(self, img):
        """
        Prepare image for OCR: grayscale → denoise → adaptive threshold → deskew.
        Returns a PIL Image ready for Tesseract.
        """
        from PIL import Image, ImageEnhance, ImageFilter

        # Convert to RGB first (handles palette/RGBA modes)
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")

        # Scale up small images (helps OCR accuracy)
        w, h = img.size
        if max(w, h) < 1200:
            scale = 1200 / max(w, h)
            img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)

        # Grayscale
        gray = img.convert("L")

        if self._check_opencv():
            gray = self._preprocess_opencv(gray)
        else:
            # PIL-only pipeline
            # Contrast enhancement
            gray = ImageEnhance.Contrast(gray).enhance(2.0)
            # Sharpening
            gray = gray.filter(ImageFilter.SHARPEN)
            gray = gray.filter(ImageFilter.UnsharpMask(radius=2, percent=150, threshold=3))

        return gray

    def _preprocess_opencv(self, pil_gray):
        """
        OpenCV-enhanced preprocessing: denoising + adaptive threshold + deskew.
        """
        import cv2
        import numpy as np
        from PIL import Image

        arr = np.array(pil_gray)

        # Denoise
        arr = cv2.fastNlMeansDenoising(arr, h=10, templateWindowSize=7, searchWindowSize=21)

        # Adaptive threshold → crisp black-on-white text
        binary = cv2.adaptiveThreshold(
            arr, 255,
            cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY,
            blockSize=31, C=10
        )

        # Deskew using moments
        coords = np.column_stack(np.where(binary < 128))
        if len(coords) > 100:
            angle = cv2.minAreaRect(coords)[-1]
            if angle < -45:
                angle = 90 + angle
            if abs(angle) > 0.5:  # Only correct if skew > 0.5°
                h, w = binary.shape
                M = cv2.getRotationMatrix2D((w // 2, h // 2), angle, 1.0)
                binary = cv2.warpAffine(binary, M, (w, h),
                                        flags=cv2.INTER_CUBIC,
                                        borderMode=cv2.BORDER_REPLICATE)

        return Image.fromarray(binary)

    # ── HOCR spatial reconstruction ───────────────────────────────────────────

    def _hocr_reconstruct_table(self, img, pytesseract) -> List[List[str]]:
        """
        Plan Feature 1 — primary table extractor.

        Uses pytesseract.image_to_data() to get per-word bounding boxes, then
        reconstructs table structure with TWO adaptive thresholds derived from
        the actual font metrics in the image (not hardcoded pixels):

          Row grouping  : words with |top_A - top_B| < median_line_height * 0.6
                          are in the same row.
          Column grouping: within a row, gaps > median_word_width * 2 start a
                          new column.

        This replaces KMeans (sklearn dep) and block_num grouping (fragile on
        noisy scans where Tesseract mis-assigns paragraph IDs).

        Returns List[row] where each row is List[cell_text].
        Returns [] if fewer than 2 rows or fewer than 2 columns detected.
        """
        import numpy as np

        try:
            df = pytesseract.image_to_data(
                img,
                output_type=pytesseract.Output.DATAFRAME,
                config="--oem 1 --psm 6 -l eng",
            )
        except Exception as e:
            print(f"[ImageParser] HOCR image_to_data failed: {e}")
            return []

        # Filter confident words (conf > 30, non-empty text)
        df = df[df["conf"] > 30].copy()
        df["text"] = df["text"].fillna("").astype(str).str.strip()
        df = df[df["text"] != ""]
        if df.empty:
            return []

        # ── Adaptive thresholds from actual font metrics ───────────────────
        heights = df["height"].dropna().values
        widths  = df["width"].dropna().values
        if len(heights) == 0 or len(widths) == 0:
            return []

        median_h = float(np.median(heights))
        median_w = float(np.median(widths))
        row_tol  = max(4, median_h * 0.6)   # Y-tolerance for same-row grouping
        col_gap  = max(8, median_w * 2.0)   # X-gap threshold for column break

        # ── Row grouping by Y-proximity ───────────────────────────────────
        words = df.sort_values("top")[["text", "left", "top", "width"]].to_dict("records")
        row_groups: List[List[dict]] = []
        for word in words:
            placed = False
            for group in row_groups:
                rep_top = group[0]["top"]
                if abs(word["top"] - rep_top) <= row_tol:
                    group.append(word)
                    placed = True
                    break
            if not placed:
                row_groups.append([word])

        # Sort each row by X, sort rows by their representative Y
        row_groups.sort(key=lambda g: g[0]["top"])

        # ── Column grouping by X-gap within each row ──────────────────────
        table: List[List[str]] = []
        for group in row_groups:
            group.sort(key=lambda w: w["left"])
            columns: List[List[str]] = [[group[0]["text"]]]
            for word in group[1:]:
                prev = group[group.index(word) - 1]
                gap  = word["left"] - (prev["left"] + prev["width"])
                if gap > col_gap:
                    columns.append([])
                columns[-1].append(word["text"])
            table.append([" ".join(col) for col in columns])

        # ── Quality gate ──────────────────────────────────────────────────
        if len(table) < 2:
            return []
        max_cols = max(len(r) for r in table)
        if max_cols < 2:
            return []

        print(f"[ImageParser] HOCR: {len(table)} rows × {max_cols} cols "
              f"(row_tol={row_tol:.1f}px, col_gap={col_gap:.1f}px)")
        return table

    # ── Multi-pass OCR ────────────────────────────────────────────────────────

    def _multi_pass_ocr(self, img, pytesseract) -> str:
        """
        Run OCR with multiple PSM modes and merge the best result.
        PSM 3  = fully automatic page segmentation (default)
        PSM 6  = single uniform block of text (good for tables)
        PSM 11 = sparse text — good for scattered labels
        """
        config_base = "--oem 3 -l eng"
        results: List[Tuple[str, int]] = []

        for psm in (3, 6, 11):
            try:
                t = pytesseract.image_to_string(img, config=f"{config_base} --psm {psm}")
                t = t.strip()
                if t:
                    # Score by number of recognisable logistics keywords
                    score = self._ocr_score(t)
                    results.append((t, score))
            except Exception:
                pass

        if not results:
            return ""

        # Return the highest-scoring pass
        results.sort(key=lambda x: x[1], reverse=True)
        return results[0][0]

    def _ocr_score(self, text: str) -> int:
        """Score OCR output by presence of logistics-relevant tokens."""
        keywords = [
            "zone", "rate", "charges", "fuel", "docket", "freight",
            "rs", "kg", "pincode", "n1", "n2", "s1", "e1", "w1",
            "gst", "pan", "oda", "rov", "surcharge", "minimum",
        ]
        lower = text.lower()
        return sum(1 for kw in keywords if kw in lower)

    # ── Table extraction ──────────────────────────────────────────────────────

    def _extract_tables_opencv(self, img, pytesseract) -> List[List[List[str]]]:
        """
        Use OpenCV to detect table grid lines → extract cell regions → OCR each cell.
        Returns list-of-tables where each table is a list-of-rows of cell strings.
        """
        import cv2
        import numpy as np
        from PIL import Image

        arr = np.array(img)
        # Make sure it's binary
        if arr.max() > 1:
            _, arr = cv2.threshold(arr, 128, 255, cv2.THRESH_BINARY_INV)

        # Detect horizontal and vertical lines
        h_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (40, 1))
        v_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 40))

        h_lines = cv2.morphologyEx(arr, cv2.MORPH_OPEN, h_kernel)
        v_lines = cv2.morphologyEx(arr, cv2.MORPH_OPEN, v_kernel)

        grid = cv2.add(h_lines, v_lines)
        if grid.sum() == 0:
            # RC-3: Try projection profile before KMeans TSV — works on whitespace-separated tables
            proj_table = self._projection_profile_table(arr, img, pytesseract)
            if proj_table:
                return [proj_table]
            return self._extract_tables_tsv(img, pytesseract)

        # Find contours of cells
        contours, _ = cv2.findContours(grid, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
        cells = []
        img_h, img_w = arr.shape
        for cnt in contours:
            x, y, w, bw = cv2.boundingRect(cnt)
            # Filter noise and near-full-image rectangles
            if 20 < w < img_w * 0.95 and 10 < bw < img_h * 0.95:
                cells.append((x, y, w, bw))

        if len(cells) < 4:
            return self._extract_tables_tsv(img, pytesseract)

        # Sort cells into rows
        cells.sort(key=lambda c: (c[1], c[0]))
        orig_arr = np.array(img)

        table: List[List[str]] = []
        current_row: List[Tuple] = []
        row_y = cells[0][1]

        for cell in cells:
            x, y, w, bw = cell
            if abs(y - row_y) > 15:  # New row
                if current_row:
                    current_row.sort(key=lambda c: c[0])
                    row_text = []
                    for cx, cy, cw, ch in current_row:
                        crop = orig_arr[cy:cy+ch, cx:cx+cw]
                        cell_text = pytesseract.image_to_string(
                            Image.fromarray(crop),
                            config="--oem 3 --psm 7 -l eng"
                        ).strip()
                        row_text.append(cell_text)
                    table.append(row_text)
                current_row = [(x, y, w, bw)]
                row_y = y
            else:
                current_row.append((x, y, w, bw))

        # Last row
        if current_row:
            current_row.sort(key=lambda c: c[0])
            row_text = []
            for cx, cy, cw, ch in current_row:
                crop = orig_arr[cy:cy+ch, cx:cx+cw]
                cell_text = pytesseract.image_to_string(
                    Image.fromarray(crop),
                    config="--oem 3 --psm 7 -l eng"
                ).strip()
                row_text.append(cell_text)
            table.append(row_text)

        return [table] if len(table) > 1 else []

    def _projection_profile_table(
        self, binary_arr, orig_img, pytesseract
    ) -> List[List[str]]:
        """
        RC-3: Detect table structure via horizontal/vertical projection profiles.

        Works for tables with WHITESPACE column separation (no visible borders),
        which causes OpenCV grid detection to return empty. Relies only on numpy
        (already required by cv2) — no sklearn dependency.

        Algorithm:
          1. Horizontal projection → valleys = row separators
          2. Vertical projection   → valleys = column separators
          3. Per-cell OCR with PSM 7 (single text line)

        Returns: List[row] where each row is List[cell_text], or [] on failure.
        """
        import numpy as np
        from PIL import Image

        try:
            h, w = binary_arr.shape

            # ── Row separators ──────────────────────────────────────────────────
            # Sum white pixels per row; valleys = whitespace between text rows
            h_profile = np.sum(binary_arr == 255, axis=1).astype(float)
            h_smooth  = np.convolve(h_profile, np.ones(3) / 3, mode="same")
            h_thresh  = h_smooth.mean() * 0.15
            row_seps  = self._find_profile_valleys(h_smooth, h_thresh, min_gap=8)

            # ── Column separators ───────────────────────────────────────────────
            v_profile = np.sum(binary_arr == 255, axis=0).astype(float)
            v_smooth  = np.convolve(v_profile, np.ones(5) / 5, mode="same")
            v_thresh  = v_smooth.mean() * 0.10
            col_seps  = self._find_profile_valleys(v_smooth, v_thresh, min_gap=15)

            # Need at least 2 rows and 1 column separator to form a table
            if len(row_seps) < 2 or len(col_seps) < 1:
                return []

            orig_arr = np.array(orig_img)
            col_bounds = list(zip([0] + col_seps, col_seps + [w]))
            row_bounds = list(zip(row_seps, row_seps[1:]))

            table: List[List[str]] = []
            for r_start, r_end in row_bounds:
                if r_end - r_start < 5:
                    continue  # skip hairline gaps
                row: List[str] = []
                for c_start, c_end in col_bounds:
                    if c_end - c_start < 5:
                        continue
                    cell_img = orig_arr[r_start:r_end, c_start:c_end]
                    if cell_img.size == 0:
                        row.append("")
                        continue
                    cell_text = pytesseract.image_to_string(
                        Image.fromarray(cell_img),
                        config="--oem 1 --psm 7 -l eng"
                    ).strip()
                    row.append(cell_text)
                if any(row):
                    table.append(row)

            if len(table) < 2:
                return []

            print(f"[ImageParser] Projection profile: {len(table)} rows × "
                  f"{len(table[0])} cols detected")
            return table

        except Exception as e:
            print(f"[ImageParser] Projection profile failed: {e}")
            return []

    @staticmethod
    def _find_profile_valleys(profile, threshold: float, min_gap: int = 8) -> List[int]:
        """
        Find midpoints of contiguous regions where profile < threshold.
        min_gap: minimum pixels between returned separator positions.
        """
        valleys: List[int] = []
        in_valley   = False
        valley_start = 0

        for i, val in enumerate(profile):
            if val < threshold and not in_valley:
                in_valley    = True
                valley_start = i
            elif val >= threshold and in_valley:
                in_valley = False
                mid = (valley_start + i) // 2
                if not valleys or mid - valleys[-1] >= min_gap:
                    valleys.append(mid)

        # Handle valley that extends to the end
        if in_valley:
            mid = (valley_start + len(profile)) // 2
            if not valleys or mid - valleys[-1] >= min_gap:
                valleys.append(mid)

        return valleys

    def _extract_tables_tsv(self, img, pytesseract) -> List[List[List[str]]]:
        """
        Fallback table extraction using Tesseract's TSV output.
        Groups words by (block_num, par_num, line_num) to reconstruct rows.
        """
        import pandas as pd

        try:
            tsv = pytesseract.image_to_data(img, output_type=pytesseract.Output.DATAFRAME,
                                            config="--oem 3 --psm 6 -l eng")
        except Exception:
            return []

        # Keep only confident words
        tsv = tsv[tsv["conf"] > 30].copy()
        if tsv.empty:
            return []

        tsv["text"] = tsv["text"].fillna("").astype(str).str.strip()
        tsv = tsv[tsv["text"] != ""]

        # Group into lines using (block_num, par_num, line_num)
        rows: List[List[str]] = []
        for _, group in tsv.groupby(["block_num", "par_num", "line_num"]):
            group = group.sort_values("left")
            line_words = list(group["text"])
            if line_words:
                rows.append(line_words)

        if not rows:
            return []

        # Try to identify column boundaries from word x-positions
        table = self._words_to_table(tsv)
        return [table] if len(table) > 1 else []

    def _words_to_table(self, tsv) -> List[List[str]]:
        """
        Convert TSV DataFrame into a row×col table.

        Plan Feature 1: replaces KMeans (sklearn dep) with the same adaptive
        Y-proximity + X-gap algorithm used by _hocr_reconstruct_table(), making
        the TSV fallback consistent and sklearn-free.
        """
        import numpy as np

        if tsv.empty:
            return []

        heights  = tsv["height"].dropna().values
        widths   = tsv["width"].dropna().values
        median_h = float(np.median(heights)) if len(heights) else 12.0
        median_w = float(np.median(widths))  if len(widths)  else 20.0
        row_tol  = max(4, median_h * 0.6)
        col_gap  = max(8, median_w * 2.0)

        words = (
            tsv.sort_values("top")[["text", "left", "top", "width"]]
            .to_dict("records")
        )

        # Y-proximity row grouping (same algorithm as HOCR)
        row_groups: List[List[dict]] = []
        for word in words:
            placed = False
            for group in row_groups:
                if abs(word["top"] - group[0]["top"]) <= row_tol:
                    group.append(word)
                    placed = True
                    break
            if not placed:
                row_groups.append([word])

        row_groups.sort(key=lambda g: g[0]["top"])

        table: List[List[str]] = []
        for group in row_groups:
            group.sort(key=lambda w: w["left"])
            columns: List[List[str]] = [[group[0]["text"]]]
            for word in group[1:]:
                prev = group[group.index(word) - 1]
                gap  = word["left"] - (prev["left"] + prev["width"])
                if gap > col_gap:
                    columns.append([])
                columns[-1].append(word["text"])
            table.append([" ".join(col) for col in columns])

        return table

    # ── Data extraction from OCR output ──────────────────────────────────────

    def _parse_extracted(self, text: str, tables: List) -> Dict:
        """Parse OCR text + tables into structured logistics data."""
        data: Dict = {}

        from parsers.excel_parser import ExcelParser
        ep = ExcelParser()

        # Parse tables for zone matrix and charges
        for table in tables:
            if not table:
                continue
            zm = ep._try_parse_zone_matrix(table)
            if zm:
                data["zone_matrix"] = zm

            ch = ep._try_parse_charges(table)
            if ch:
                data.setdefault("charges", {}).update(ch)

        # Parse text lines as key-value pairs (works for rate card photos)
        if text:
            text_rows = self._text_to_rows(text)
            if text_rows:
                # Try charges
                ch = ep._try_parse_charges(text_rows)
                if ch:
                    data.setdefault("charges", {}).update(ch)
                # Try company info
                cd = self._extract_company_from_text(text)
                if cd:
                    data.setdefault("company_details", {}).update(cd)

        return data

    def _text_to_rows(self, text: str) -> List[List[str]]:
        """Convert OCR text lines into rows for ExcelParser pattern matching."""
        rows = []
        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
            # Split on common delimiters: tab, |, multiple spaces, colon
            parts = re.split(r"\t|\s{2,}|\|", line)
            parts = [p.strip() for p in parts if p.strip()]
            if parts:
                rows.append(parts)
        return rows

    def _extract_company_from_text(self, text: str) -> Dict:
        """Extract company info (GST, phone, email) via regex from OCR text."""
        info: Dict = {}

        # GST number: 2 digits + 5 uppercase + 4 digits + 1 uppercase + 1 digit + Z/Y/X + 1 digit
        gst = re.search(r'\b(\d{2}[A-Z]{5}\d{4}[A-Z]\d[ZYX]\d)\b', text, re.IGNORECASE)
        if gst:
            info["gstNo"] = gst.group(1).upper()

        # Phone: 10-digit Indian mobile or STD
        phone = re.search(r'\b((?:0\d{2,4}[-\s]?\d{6,8}|\d{10}))\b', text)
        if phone:
            info["contactPhone"] = re.sub(r'[\s\-]', '', phone.group(1))

        # Email
        email = re.search(r'\b[\w.+-]+@[\w-]+\.[\w.]+\b', text, re.IGNORECASE)
        if email:
            info["contactEmail"] = email.group(0).lower()

        # PAN: 5 uppercase + 4 digits + 1 uppercase
        pan = re.search(r'\b([A-Z]{5}\d{4}[A-Z])\b', text)
        if pan:
            info["panNo"] = pan.group(1)

        return info
