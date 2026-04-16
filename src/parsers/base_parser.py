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

    @abstractmethod
    def parse(self, file_path: str) -> Dict[str, Any]:
        """
        Parse a file and return extracted data.
        Returns dict with keys: text, tables, data
        """
        pass

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
