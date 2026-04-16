"""
Ollama Client
=============
Connects to local Ollama instance (localhost:11434) for intelligent
data extraction from unstructured transporter documents.

Uses qwen2.5-coder:3b (or whatever model is available) to:
1. Extract company details from free text
2. Parse complex charge structures
3. Identify and map zone price matrices
4. Extract pincode serviceability from mixed formats
5. Detect cross-zone reclassifications
"""

import json
import re
import time
from typing import Dict, Any, Optional, List
import urllib.request
import urllib.error


OLLAMA_BASE = "http://localhost:11434"
DEFAULT_MODEL = "qwen2.5-coder:3b"
FALLBACK_MODELS = ["qwen2.5:3b", "llama3.2:3b", "llama3.1:8b", "mistral:7b"]


def _http_post(url: str, payload: Dict, timeout: int = 120) -> Optional[Dict]:
    """Simple HTTP POST using urllib (no requests dependency)."""
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.URLError as e:
        print(f"[Ollama] Connection error: {e}")
        return None
    except Exception as e:
        print(f"[Ollama] Error: {e}")
        return None


def get_available_models() -> List[str]:
    """Get list of available Ollama models."""
    result = _http_post(f"{OLLAMA_BASE}/api/tags", {}, timeout=5)
    if not result:
        return []
    return [m["name"] for m in result.get("models", [])]


def detect_best_model() -> Optional[str]:
    """Find the best available model for extraction."""
    available = get_available_models()
    if not available:
        return None

    # Prefer coder models for structured extraction
    preferred = [
        "qwen2.5-coder:3b", "qwen2.5-coder:7b",
        "qwen2.5:3b", "qwen2.5:7b",
        "llama3.2:3b", "llama3.1:8b",
        "mistral:7b", "deepseek-coder:6.7b"
    ]

    for model in preferred:
        if any(model in a or a in model for a in available):
            return model

    return available[0] if available else None


class OllamaExtractor:
    """
    Uses Ollama LLM to extract structured data from unstructured text.
    """

    def __init__(self, model: str = None):
        self.model = model or detect_best_model() or DEFAULT_MODEL
        self._available = self._check_connection()
        if self._available:
            print(f"[Ollama] Connected — using model: {self.model}")
        else:
            print(f"[Ollama] Not available — will use regex-only extraction")

    def _check_connection(self) -> bool:
        models = get_available_models()
        return len(models) > 0

    def is_available(self) -> bool:
        return self._available

    def extract(self, prompt: str, context_text: str, max_length: int = 8000) -> Optional[Dict]:
        """
        Send text to Ollama for extraction. Returns parsed JSON or None.
        """
        if not self._available:
            return None

        # Truncate context if too long
        if len(context_text) > max_length:
            context_text = context_text[:max_length] + "\n...[truncated]"

        full_prompt = f"{prompt}\n\nDATA:\n{context_text}\n\nJSON:"

        payload = {
            "model": self.model,
            "prompt": full_prompt,
            "stream": False,
            "options": {
                "temperature": 0.1,
                "top_p": 0.9,
                "num_predict": 4096
            }
        }

        response = _http_post(f"{OLLAMA_BASE}/api/generate", payload, timeout=180)
        if not response:
            return None

        raw_text = response.get("response", "")
        return self._parse_json_response(raw_text)

    def _parse_json_response(self, text: str) -> Optional[Dict]:
        """Extract JSON from LLM response."""
        # Try direct JSON parse
        text = text.strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # Find JSON block
        for pattern in [r"```json\n?(.*?)\n?```", r"```\n?(.*?)\n?```", r"\{.*\}"]:
            match = re.search(pattern, text, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(1) if "```" in pattern else match.group(0))
                except json.JSONDecodeError:
                    continue

        return None

    def extract_company_details(self, text: str) -> Dict:
        """Extract company details from free text."""
        from intelligence.prompts import COMPANY_EXTRACTION_PROMPT
        result = self.extract(COMPANY_EXTRACTION_PROMPT, text)
        return result or {}

    def extract_charges(self, text: str) -> Dict:
        """Extract charge configuration from text."""
        from intelligence.prompts import CHARGES_EXTRACTION_PROMPT
        result = self.extract(CHARGES_EXTRACTION_PROMPT, text)
        return result or {}

    def extract_zone_matrix(self, text: str) -> Dict:
        """Extract zone price matrix from text/table."""
        from intelligence.prompts import ZONE_MATRIX_EXTRACTION_PROMPT
        result = self.extract(ZONE_MATRIX_EXTRACTION_PROMPT, text)
        if result and "zoneMatrix" in result:
            return result["zoneMatrix"]
        if result and "zone_matrix" in result:
            return result["zone_matrix"]
        return result or {}

    def extract_serviceability(self, text: str, context: str = "") -> Dict:
        """Extract pincode serviceability from text."""
        from intelligence.prompts import SERVICEABILITY_PROMPT
        result = self.extract(
            SERVICEABILITY_PROMPT,
            text + ("\n\nContext: " + context if context else "")
        )
        return result or {}

    def smart_merge(self, extracted_pieces: List[Dict]) -> Dict:
        """
        Use AI to intelligently merge multiple extracted pieces
        into a single coherent raw data dict.
        """
        if len(extracted_pieces) == 1:
            return extracted_pieces[0]

        from intelligence.prompts import MERGE_PROMPT
        context = json.dumps(extracted_pieces, indent=2)[:6000]
        result = self.extract(MERGE_PROMPT, context)
        return result or extracted_pieces[0]
