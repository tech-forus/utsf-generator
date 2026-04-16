"""
Word Document Parser
====================
Extracts text and tables from .docx (Word) files using python-docx.

Note: .doc (old Word 97-2003) is NOT supported — open in Word and
      Save As .docx first.
"""

import os
from typing import Dict, Any, List, Optional
from parsers.base_parser import BaseParser


class WordParser(BaseParser):
    SUPPORTED_EXTENSIONS = [".docx"]

    def parse(self, file_path: str) -> Dict[str, Any]:
        try:
            from docx import Document
        except ImportError:
            print("[WordParser] python-docx not installed — pip install python-docx")
            return {"text": "", "tables": [], "data": {}}

        doc = Document(file_path)

        # ── Extract paragraphs ─────────────────────────────────────────────────
        paragraphs = [p.text.strip() for p in doc.paragraphs if p.text.strip()]
        text_body = "\n".join(paragraphs)

        # ── Extract tables ─────────────────────────────────────────────────────
        raw_tables = []
        table_text = ""
        sheets: Dict[str, List[List]] = {}

        for i, tbl in enumerate(doc.tables):
            rows = []
            for row in tbl.rows:
                cells = [cell.text.strip() for cell in row.cells]
                # Merge duplicate adjacent cells (Word table merging artefact)
                deduped = []
                for c in cells:
                    if not deduped or c != deduped[-1]:
                        deduped.append(c)
                if any(deduped):
                    rows.append(deduped)

            if rows:
                name = f"Table {i + 1}"
                raw_tables.append({"name": name, "rows": rows})
                sheets[name] = rows
                table_text += f"\n=== {name} ===\n"
                for row in rows[:100]:
                    table_text += "\t".join(row) + "\n"

        full_text = text_body
        if table_text:
            full_text += "\n\n--- Tables ---\n" + table_text

        # ── Auto-detect structured data from tables ────────────────────────────
        # Reuse Excel parser's detection logic (same row/col format)
        data = {}
        if sheets:
            try:
                from parsers.excel_parser import ExcelParser
                data = ExcelParser()._auto_detect(sheets)
            except Exception:
                pass

        return {
            "text": full_text,
            "tables": raw_tables,
            "data": data,
        }
