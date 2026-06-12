"""
Shared pincode JSON cache.

GeoValidator, ZoneMapper, and OICREngine each parse the same master
pincodes.json (~25k rows) into their own internal structures. Reading and
json.load-ing the file is the expensive part and was previously repeated
once per instance. This module caches the raw parsed JSON (list of dicts)
per absolute path so the file is read and parsed at most once per run;
each caller still builds its own derived structures from the shared list.
"""

import json
import os
from typing import List, Dict

_cache: Dict[str, List[Dict]] = {}


def load_pincodes_raw(path: str) -> List[Dict]:
    """Return the parsed pincodes.json contents, loading from disk only once
    per absolute path per process."""
    abs_path = os.path.abspath(path)
    if abs_path not in _cache:
        with open(path, "r", encoding="utf-8") as f:
            _cache[abs_path] = json.load(f)
    return _cache[abs_path]
