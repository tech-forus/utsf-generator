"""
Base Parser Interface
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List
import os


class BaseParser(ABC):
    """Base class for all file parsers."""

    SUPPORTED_EXTENSIONS: List[str] = []

    def __init__(self):
        self.extracted_text: str = ""
        self.extracted_tables: List[List] = []
        self.raw_data: Any = None
        self._doc_ctx: Any = None   # DocumentContext injected by main.py pipeline

    @abstractmethod
    def parse(self, file_path: str, doc_context: Any = None) -> Dict[str, Any]:
        """
        Parse a file and return extracted data.
        Returns dict with keys: text, tables, data

        doc_context: optional DocumentContext instance — if provided, the parser
        will call doc_context.absorb() on headers/footers/sheet-names and may
        read accumulated state (transport_mode, effective_date, etc.)
        """
        pass

    def _attach_doc_context(self, doc_context: Any) -> None:
        """Store the DocumentContext for sub-methods to access via self._doc_ctx."""
        self._doc_ctx = doc_context

    def can_parse(self, file_path: str) -> bool:
        ext = os.path.splitext(file_path)[1].lower()
        return ext in self.SUPPORTED_EXTENSIONS

    @staticmethod
    def safe_float(val, default=0.0) -> float:
        """Safely convert to float."""
        try:
            if val is None or val == "" or val == "-":
                return default
            return float(str(val).replace(",", "").replace("₹", "").strip())
        except (ValueError, TypeError):
            return default

    @staticmethod
    def safe_int(val, default=0) -> int:
        try:
            return int(float(str(val).replace(",", "").strip()))
        except (ValueError, TypeError):
            return default
