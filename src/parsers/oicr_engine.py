"""
OICR Engine — Optical Intelligence Combined Recognition
=========================================================
v8.0 — 2026-04-08

A document intelligence layer that sits above basic OCR/PDF extraction.
Handles TCI-style documents: station-rate Excel cards, charge proposal PDFs,
city-based zone matrices, and scanned logistics documents.

Capabilities:
  1. Station-rate matrix -> FC4 zone matrix  (TCI / V-Express style)
  2. City-based zone matrix -> canonical zone codes
  3. TCI charge table extraction  (DWB, FOV/ROV, ODA, min charges)
  4. Company info extraction from proposal PDFs
  5. OCR post-processing (Tesseract + pdfplumber combined)
  6. Zone matrix extrapolation from single-origin to full matrix
  7. Freight rate normalisation (per-kg, per-shipment, weight-band)
"""

import re
import os
import json
import math
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict

# ─── Version ──────────────────────────────────────────────────────────────────
OICR_VERSION = "8.0"

# ─── Canonical zone list ──────────────────────────────────────────────────────
ALL_ZONES = [
    "N1","N2","N3","N4",
    "S1","S2","S3","S4",
    "E1","E2",
    "W1","W2",
    "C1","C2",
    "NE1","NE2",
    "X1","X2","X3",
]

# ─── City / Station -> FC4 zone mapping ───────────────────────────────────────
# Covers common Indian logistics city codes and full names
CITY_ZONE_MAP: Dict[str, List[str]] = {
    # Delhi / NCR -> N1
    "DEL": ["N1"], "DELHI": ["N1"], "DEL/NCR": ["N1"], "NCR": ["N1"],
    "WITHIN CITY": ["N1"],  # TCI uses this for local delivery
    "USER": None,  # TCI "USER" row = the customer rate row — skip as origin label

    # ── TCI Proposal city/region headers (from actual PDF analysis) ────────────
    "DEL/NCR":        ["N1"],   # TCI header: Delhi/NCR
    "DEL/N CR":       ["N1"],
    "REST OF NORTH":  ["N2","N3","N4"],
    "REST\nOF NORTH": ["N2","N3","N4"],
    "RON":            ["N2","N3","N4"],
    "H\nP":           ["N4"],   # Himachal Pradesh
    "HP":             ["N4"],
    "JAMMU":          ["X3"],
    "SRINAGAR":       ["X3"],
    "BLR/MAA/PCY":    ["S1","S2"],   # Bangalore/Madras/Pondicherry
    "BLR/ MAA/\nPCY": ["S1","S2"],
    "HYD/ AP/ TS\nTN / KA": ["S2","S3"],   # Hyderabad/AP/Telangana/TN/Karnataka
    "HYD/AP/TS":      ["S2","S3"],
    "KERALA":         ["S4"],
    "AMD/\nBOM/\nPUNE":    ["W1"],   # Ahmedabad/Mumbai/Pune
    "AMD/BOM/PUNE":   ["W1"],
    "REST\nOF WEST":  ["W2"],
    "REST OF WEST":   ["W2"],
    "CENTRAL":        ["C1","C2"],
    "GUWAHATI":       ["NE1"],
    "ASSAM":          ["NE1","NE2"],
    "JH/OD":          ["E1"],   # Jharkhand/Odisha
    "CG":             ["C1"],   # Chhattisgarh
    "WEST BENGAL":    ["E1","E2"],
    "BIHAR":          ["E2"],
    "KOLKATA":        ["E1"],
    "OKL":            ["N1"],   # Okhla — TCI's Delhi depot code
    "FARIDABAD": ["N1"], "GHAZIABAD": ["N1"], "GURUGRAM": ["N1"], "NOIDA": ["N1"],

    # Rest of North -> N2/N3
    "REST OF NORTH": ["N2","N3"], "REST NORTH": ["N2","N3"], "RON": ["N2","N3"],
    "CHANDIGARH": ["N2"], "AMBALA": ["N2"],
    "JAIPUR": ["N3"], "RAJASTHAN": ["N3"],
    "UP": ["N2"], "UTTAR PRADESH": ["N2"],
    "HARYANA": ["N2"], "PUNJAB": ["N2"],
    "UTTARAKHAND": ["N3"], "UTTARANCHAL": ["N3"],
    "LUCKNOW": ["N2"], "AGRA": ["N2"], "KANPUR": ["N2"],

    # HP / Jammu -> X3 (Special)
    "HP": ["X3"], "HIMACHAL PRADESH": ["X3"], "SHIMLA": ["X3"],
    "JAMMU": ["X3"], "JAMMU HP": ["X3"], "JAMMU, HP": ["X3"],
    "J&K": ["X3"], "JK": ["X3"], "JAMMU HP": ["X3"],

    # Srinagar / Kashmir -> X3
    "SRINAGAR": ["X3"], "SRINAGAR KASHMIR": ["X3"], "KASHMIR": ["X3"],
    "LADAKH": ["X3"], "LEH": ["X3"],

    # South -> S1/S2/S3
    "BANGALORE": ["S2"], "BENGALURU": ["S2"], "BLR": ["S2"],
    "CHENNAI": ["S1"], "MAA": ["S1"], "MADRAS": ["S1"],
    "PONDY": ["S1"], "PONDICHERRY": ["S1"], "PCY": ["S1"],
    "BLR/MAA/PCY": ["S1","S2"],
    "HYDERABAD": ["S3"], "HYD": ["S3"], "TELANGANA": ["S3"],
    "KERALA": ["S4"], "KOCHI": ["S4"], "COCHIN": ["S4"], "TRIVANDRUM": ["S4"],
    "TAMILNADU": ["S1"], "TAMIL NADU": ["S1"], "ANDHRA PRADESH": ["S3"],
    "KARNATAKA": ["S2"],

    # Kolkata / East -> E1
    "KOLKATA": ["E1"], "OKL": ["E1"], "KOL": ["E1"], "CALCUTTA": ["E1"],
    "WEST BENGAL": ["E1"], "WB": ["E1"],
    "ODISHA": ["E1"], "BHUBANESWAR": ["E1"],
    "JH/OD": ["E1"], "JHARKHAND": ["E1"], "RANCHI": ["E1"],

    # Bihar / Far East -> E2
    "BIHAR": ["E2"], "PATNA": ["E2"],
    "ASSAM": ["NE1"], "GUWAHATI": ["NE1"],
    "CG": ["C1"], "CHATTISGARH": ["C1"], "CHHATTISGARH": ["C1"], "RAIPUR": ["C1"],

    # West -> W1/W2
    "MUMBAI": ["W1"], "BOM": ["W1"], "PUNE": ["W1"],
    "AMD/BARODA": ["W1"], "AHMEDABAD": ["W1"], "BARODA": ["W1"],
    "GUJARAT": ["W1"], "SURAT": ["W1"],
    "MAHARASHTRA": ["W2"], "NAGPUR": ["W2"],
    "GOA": ["W2"],

    # Central -> C1/C2
    "BHOPAL": ["C1"], "INDORE": ["C1"], "MADHYA PRADESH": ["C1"], "MP": ["C1"],
    "NAGPUR": ["C2"],

    # Northeast -> NE1/NE2
    "NORTHEAST": ["NE1","NE2"], "NORTH EAST": ["NE1","NE2"],
    "ASSAM": ["NE1"], "GUWAHATI": ["NE1"],
    "ARUNACHAL PRADESH": ["NE2"], "MANIPUR": ["NE2"],
    "MEGHALAYA": ["NE2"], "MIZORAM": ["NE2"], "NAGALAND": ["NE2"],
    "TRIPURA": ["NE2"], "SIKKIM": ["NE2"],
    "SPECIAL": ["NE1","NE2","X1","X2","X3"],  # TCI "Special Zone"
    "SPECIAL ZONE": ["NE1","NE2","X1","X2","X3"],

    # Islands -> X1/X2
    "ANDAMAN": ["X1"], "PORT BLAIR": ["X1"], "PORTBLAIR": ["X1"],
    "LAKSHADWEEP": ["X2"],
}

# ─── TCI station code -> approximate from-zone ────────────────────────────────
# "OKL" = Kolkata origin -> E1
STATION_ZONE_MAP: Dict[str, str] = {
    "OKL": "N1",   # Okhla (Delhi industrial hub — TCI's primary origin depot)
    "DEL": "N1",   # Delhi
    "NDLS": "N1",  # New Delhi
    "BOM": "W1",   # Mumbai (Bombay)
    "MAA": "S1",   # Chennai (Madras)
    "BLR": "S2",   # Bangalore
    "HYD": "S3",   # Hyderabad
    "AMD": "W1",   # Ahmedabad
    "PNQ": "W1",   # Pune
    "KOL": "E1",   # Kolkata
    "CCU": "E1",   # Kolkata (IATA)
    "BLR": "S2",   # Bangalore
}

# ─── Zone geographic centroids (lat, lon) ─────────────────────────────────────
ZONE_COORDS: Dict[str, Tuple[float, float]] = {
    "N1": (28.7, 77.1), "N2": (26.8, 80.9), "N3": (27.0, 74.6), "N4": (30.7, 76.7),
    "S1": (13.1, 80.3), "S2": (12.9, 77.6), "S3": (17.4, 78.5), "S4": (10.0, 76.3),
    "E1": (22.6, 88.4), "E2": (25.6, 85.1),
    "W1": (19.1, 72.9), "W2": (18.5, 73.9),
    "C1": (23.3, 77.4), "C2": (21.1, 79.1),
    "NE1": (26.1, 91.7), "NE2": (25.6, 94.1),
    "X1": (11.6, 92.7), "X2": (10.6, 72.6), "X3": (34.1, 74.8),
}


def _haversine(c1: Tuple[float, float], c2: Tuple[float, float]) -> float:
    """Great-circle distance in km between two (lat, lon) coordinates."""
    lat1, lon1 = math.radians(c1[0]), math.radians(c1[1])
    lat2, lon2 = math.radians(c2[0]), math.radians(c2[1])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    return 6371 * 2 * math.asin(math.sqrt(a))


# ─── State (full name + common abbreviations/typos) -> zone(s) ────────────────
# Used by detect_city_rate_card to handle state-column lookups.
# Canonical spellings AND the most common misspellings that appear in rate cards.
STATE_ZONE_MAP: Dict[str, List[str]] = {
    # North
    "UTTAR PRADESH": ["N2"], "UP": ["N2"], "U.P.": ["N2"], "U.P": ["N2"],
    "HARYANA": ["N2"], "PUNJAB": ["N2"], "CHANDIGARH": ["N2"],
    "HIMACHAL PRADESH": ["N4"], "HP": ["N4"], "H.P.": ["N4"], "HIMACHAL": ["N4"],
    "UTTARAKHAND": ["N3"], "UTTRAKHAND": ["N3"], "UTTARANCHAL": ["N3"],
    "RAJASTHAN": ["N3"], "RJ": ["N3"],
    # J&K / Ladakh (Special)
    "JAMMU & KASHMIR": ["X3"], "JAMMU AND KASHMIR": ["X3"],
    "J&K": ["X3"], "JK": ["X3"], "LADAKH": ["X3"],
    # South
    "KARNATAKA": ["S2"], "KA": ["S2"],
    "TAMIL NADU": ["S1"], "TAMILNADU": ["S1"], "TN": ["S1"],
    "PONDICHERRY": ["S1"], "PUDUCHERRY": ["S1"],
    "TELANGANA": ["S3"], "TS": ["S3"],
    "ANDHRA PRADESH": ["S3"], "AP": ["S3"], "A.P.": ["S3"],
    "KERALA": ["S4"], "KL": ["S4"],
    # West
    "GUJARAT": ["W1"], "GUJRAT": ["W1"], "GUJRAT": ["W1"], "GJ": ["W1"],
    "MAHARASHTRA": ["W2"], "MH": ["W2"],
    "GOA": ["W2"],
    "MUMBAI (MAHARASHTRA)": ["W1"],
    # East
    "WEST BENGAL": ["E1"], "WB": ["E1"],
    "ODISHA": ["E1"], "ORISSA": ["E1"],
    "JHARKHAND": ["E1"], "JH": ["E1"],
    "BIHAR": ["E2"], "BR": ["E2"],
    # Central
    "MADHYA PRADESH": ["C1"], "MP": ["C1"], "M.P.": ["C1"],
    "CHHATTISGARH": ["C1"], "CHATTISGARH": ["C1"], "CG": ["C1"],
    # Northeast
    "ASSAM": ["NE1"], "AS": ["NE1"],
    "MEGHALAYA": ["NE2"], "TRIPURA": ["NE2"], "MANIPUR": ["NE2"],
    "NAGALAND": ["NE2"], "MIZORAM": ["NE2"], "ARUNACHAL PRADESH": ["NE2"],
    "SIKKIM": ["NE2"],
    # Special
    "ANDAMAN & NICOBAR": ["X1"], "ANDAMAN AND NICOBAR": ["X1"],
    "ANDAMAN": ["X1"], "A&N": ["X1"],
    "LAKSHADWEEP": ["X2"],
    # Broad regional labels on rate cards
    "REST OF INDIA": ["N2","N3","N4","S2","S3","S4","E2","W2","C1","C2","NE1","NE2"],
    "REST OF STATE": ["N2","N3","N4","S2","S3","S4","E2","W2","C1","C2","NE1","NE2"],
    "ALL INDIA": ["N1","N2","N3","N4","S1","S2","S3","S4","E1","E2","W1","W2","C1","C2","NE1","NE2"],
}


def city_to_zones(city_text: str) -> List[str]:
    """Map a city name / abbreviation to one or more FC4 zone codes."""
    txt = city_text.strip().upper()
    if txt in CITY_ZONE_MAP:
        return CITY_ZONE_MAP[txt]
    # State-level lookup (handles full state names that are not in CITY_ZONE_MAP)
    if txt in STATE_ZONE_MAP:
        return STATE_ZONE_MAP[txt]
    # Partial match against CITY_ZONE_MAP (guards against substring false-positives
    # by requiring at least 4 chars in both the key and the cell text)
    for key, zones in CITY_ZONE_MAP.items():
        if len(key) >= 4 and len(txt) >= 4 and (key in txt or txt in key):
            return zones
    # Partial match against STATE_ZONE_MAP
    for key, zones in STATE_ZONE_MAP.items():
        if len(key) >= 4 and len(txt) >= 4 and (key in txt or txt in key):
            return zones
    return []


def station_to_zone(station_code: str) -> Optional[str]:
    """Map a TCI station code to a single FC4 zone."""
    return STATION_ZONE_MAP.get(station_code.upper().strip())


# ─── Charge label patterns ────────────────────────────────────────────────────
# Each: (canonical_field, [regex_patterns], value_type)
# value_type: 'scalar', 'pct', 'vf', 'percent_min_fixed'
# CRITICAL: All [^\d]* spans capped at 60 chars + no-newline to prevent
# the 1728 CFT constant (L*B*H/1728) from contaminating charge fields.
_NL = r"[^\d\n]{0,60}"

_CHARGE_REGEXES = [
    # DWB / Docket / LR charges
    ("docketCharges", [
        rf"(?:dwb|docket(?:\s*charge)?|lr\s*charge|waybill){_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
        rf"(?:document|doc)\s*(?:charge|fee){_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
    ], "scalar"),

    # Fuel surcharge
    ("fuel", [
        rf"fuel\s*(?:surcharge|%|percent|sc){_NL}(\d+(?:\.\d+)?)\s*%",
        r"(\d+(?:\.\d+)?)\s*%\s*(?:fuel|fsc|f/s)",
    ], "pct"),

    # Minimum charges — require currency signal to avoid matching minWeight
    ("minCharges", [
        rf"min(?:imum)?\s*(?:chargeable\s*)?(?:basic\s*)?freight{_NL}(?:rs\.?|₹|inr)\s*(\d+(?:\.\d+)?)",
        rf"sfc\s*(?:rs\.?|-)\s*(\d+(?:\.\d+)?)",   # TCI "SFC-Rs-350"
        rf"min(?:imum)?\s*billing{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
    ], "scalar"),

    # Minimum weight — MUST end with kg
    ("minWeight", [
        rf"min(?:imum)?\s*(?:chargeable\s*)?weight\s*(?:\(docket\))?{_NL}(\d+(?:\.\d+)?)\s*kg",
        rf"sfc\s*[^\d\n]{{0,15}}(\d+(?:\.\d+)?)\s*kg",
        rf"min\s*wt{_NL}(\d+(?:\.\d+)?)\s*kg",
    ], "scalar"),

    # ROV / FOV
    ("rovCharges", [
        rf"(?:rov|fov|freight\s*on\s*value)\s*(?:owners?\s*risk)?{_NL}(\d+(?:\.\d+)?)\s*%\s*min\s*(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
        rf"(?:rov|fov){_NL}(\d+(?:\.\d+)?)\s*%",
    ], "vf"),

    # ODA
    ("odaCharges", [
        rf"(?:oda|out\s*delivery\s*area){_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)\s*(?:per\s*(?:shipment|consignment|docket))?",
    ], "scalar"),

    # Green tax / NGT (TCI-specific Delhi surcharge)
    ("greenTax", [
        rf"green\s*(?:tax|cess|surcharge){_NL}(\d+(?:\.\d+)?)",
        rf"ngt\s*charge{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
    ], "scalar"),

    # DACC — cap to same line
    ("daccCharges", [
        rf"(?:dacc|delivery\s*against\s*consignee\s*copy){_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
    ], "scalar"),

    # COD — cap to same line
    ("codCharges", [
        rf"(?:cod|cash\s*on\s*delivery){_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
    ], "scalar"),

    # FOD = topay for TCI
    ("topayCharges", [
        rf"(?:fod|freight\s*on\s*delivery){_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
    ], "scalar"),

    # Handling
    ("handlingCharges", [
        rf"handling\s*(?:charge(?:s)?)?{_NL}(?:rs\.?\s*)?(\d+(?:\.\d+)?)\s*(?:per\s*kg)?",
    ], "scalar"),
]

# ─── Company info patterns ────────────────────────────────────────────────────
_COMPANY_REGEXES = {
    "gstNo":        r'\b(\d{2}[A-Z]{5}\d{4}[A-Z]\d[A-Z0-9]{2})\b',  # full GST format
    "panNo":        r'\bPAN\b[:\s]*([A-Z]{5}\d{4}[A-Z])\b',
    "contactPhone": r'\b((?:0\d{2,4}[\s-]?\d{6,8}|\+?91[\s-]?\d{10}|\d{10}))\b',
    "contactEmail": r'\b([\w.+-]+@[\w-]+\.(?:com|in|co\.in|net|org))\b',
    "website":      r'(?:https?://)?(?:www\.)?([\w-]+\.(?:com|in|co\.in)(?:/[\w/.-]*)?)',
    "cinNo":        r'\bCIN\b[:\s]*([LU]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6})\b',
}

# Known transporter company names for classification
_COMPANY_NAMES = {
    "TCI FREIGHT": {"canonical": "TCI Freight Private Limited", "short": "TCI Freight"},
    "TCI EXPRESS": {"canonical": "TCI Express Limited", "short": "TCI Express"},
    "DELHIVERY":   {"canonical": "Delhivery Private Limited", "short": "Delhivery"},
    "V-XPRESS":    {"canonical": "V-Xpress Logistics Pvt. Ltd.", "short": "V-Xpress"},
    "V EXPRESS":   {"canonical": "V-Xpress Logistics Pvt. Ltd.", "short": "V-Xpress"},
    "BLUEDART":    {"canonical": "BlueDart Express Limited", "short": "BlueDart"},
    "EKART":       {"canonical": "Ekart Logistics Pvt. Ltd.", "short": "Ekart"},
    "XPRESSBEES":  {"canonical": "Xpressbees Logistics Solutions Pvt. Ltd.", "short": "XpressBees"},
    "DTDC":        {"canonical": "DTDC Express Limited", "short": "DTDC"},
    "ECOM EXPRESS":{"canonical": "Ecom Express Pvt. Ltd.", "short": "Ecom Express"},
    "SHADOWFAX":   {"canonical": "Shadowfax Technologies Pvt. Ltd.", "short": "Shadowfax"},
}


class OICREngine:
    """
    OICR (Optical Intelligence Combined Recognition) Engine v8.0

    Processes logistics documents to extract structured transporter data:
    - Zone rate matrices (from city-based or station-based tables)
    - Pricing charges (fuel, docket, ODA, ROV, min charges etc.)
    - Company identity information
    - Pincode serviceability

    Designed specifically for Indian B2B logistics transporter data formats.
    """

    def __init__(self, pincodes_path: str = None):
        self._pincode_zone: Dict[str, str] = {}  # pincode str -> zone
        self._loaded = False
        if pincodes_path and os.path.isfile(pincodes_path):
            self._load_pincodes(pincodes_path)
        else:
            self._try_auto_load()

    def _try_auto_load(self):
        """Try to auto-find and load pincodes.json."""
        here = os.path.dirname(os.path.abspath(__file__))
        candidates = [
            os.path.join(here, "..", "..", "data", "pincodes.json"),
            os.path.join(here, "..", "data", "pincodes.json"),
            os.environ.get("UTSF_DATA", ""),
        ]
        data_env = os.environ.get("UTSF_DATA", "")
        if data_env:
            candidates.append(os.path.join(data_env, "pincodes.json"))
        for path in candidates:
            path = os.path.normpath(path) if path else ""
            if path and os.path.isfile(path):
                self._load_pincodes(path)
                return

    def _load_pincodes(self, path: str):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            # Supports both list-of-dicts and dict format
            if isinstance(data, list):
                for entry in data:
                    pin = str(entry.get("pincode", "")).strip()
                    zone = str(entry.get("zone", "")).strip()
                    if pin and zone:
                        self._pincode_zone[pin] = zone
            elif isinstance(data, dict):
                for pin, info in data.items():
                    if isinstance(info, dict):
                        self._pincode_zone[str(pin)] = info.get("zone", "")
                    else:
                        self._pincode_zone[str(pin)] = str(info)
            self._loaded = True
            print(f"[OICR] Loaded {len(self._pincode_zone):,} pincodes")
        except Exception as e:
            print(f"[OICR] Could not load pincodes: {e}")

    def pincode_to_zone(self, pincode) -> Optional[str]:
        """Return the FC4 zone for a pincode, or None if not found."""
        pin_str = str(int(float(str(pincode).strip()))) if pincode else ""
        zone = self._pincode_zone.get(pin_str)
        if zone in ALL_ZONES:
            return zone
        return None

    # ─── 1a. Station-rate detection from raw rows ─────────────────────────────

    def detect_station_rate_from_rows(self, rows: List[List], sheet_name: str = "") -> Optional[Dict]:
        """
        Detect TCI-style station-rate format from raw rows (list-of-lists).
        Looks for header row with From Station, PIN Code, and Rate columns.
        Returns {'zone_matrix': {...}, 'served_pincodes': [...]} or None.
        """
        if not rows or len(rows) < 5:
            return None

        header_idx = None
        from_col = rate_col = pin_col = km_col = None

        for ri, row in enumerate(rows[:12]):
            h = [str(c).lower().strip() for c in row]
            fc = rc = pc = kc = None
            for ci, cell in enumerate(h):
                if ("from" in cell and ("station" in cell or cell.strip() == "from")):
                    fc = ci
                if ("rate" in cell or "out card" in cell or "out_card" in cell
                        or ("freight" in cell and "rate" in cell)):
                    if rc is None:
                        rc = ci
                if "pin" in cell and pc is None:
                    pc = ci
                if cell.strip() in ("km", "kms", "distance") and kc is None:
                    kc = ci
            if pc is not None and (rc is not None or kc is not None):
                header_idx = ri
                from_col, rate_col, pin_col, km_col = fc, rc, pc, kc
                break

        if header_idx is None or pin_col is None:
            return None

        # ── Better rate column selection ──────────────────────────────────────
        # TCI Excels often have multiple rate columns: "Out Card", "Express Rate",
        # "To Pay", etc. We want the BASE freight rate, not the all-in or express rate.
        # Score columns by preference: out card / base > freight > surface > generic > express / topay
        if rate_col is not None:
            best_rc, best_score = rate_col, 0
            for ri, row in enumerate(rows[:header_idx + 1]):
                h = [str(c).lower().strip() for c in row]
                for ci, cell in enumerate(h):
                    s = 0
                    if "out card" in cell or "out_card" in cell:    s = 6
                    elif "base" in cell and "rate" in cell:         s = 5
                    elif "freight" in cell and "rate" in cell:      s = 4
                    elif "surface" in cell:                         s = 3
                    elif "rate" in cell and "express" not in cell and "topay" not in cell and "to pay" not in cell: s = 2
                    elif "rate" in cell:                            s = 1
                    if s > best_score:
                        best_score, best_rc = s, ci
            rate_col = best_rc

        data_rows = rows[header_idx + 1:]

        # Determine origin station/zone
        origin_station = "OKL"
        if from_col is not None:
            origins = set()
            for row in data_rows[:100]:
                if from_col < len(row):
                    v = str(row[from_col]).strip().upper()
                    if v and v not in ("", "NAN", "NONE"):
                        origins.add(v)
            if 1 <= len(origins) <= 5:
                origin_station = list(origins)[0]
            elif len(origins) > 5:
                return None  # Too many origins — not a single-origin card

        origin_zone = STATION_ZONE_MAP.get(origin_station)
        if not origin_zone:
            inferred = city_to_zones(origin_station)
            origin_zone = inferred[0] if inferred else "N1"

        print(f"[OICR] Station-rate rows ({sheet_name}): origin={origin_station}->{origin_zone}")

        # Extract pincodes + per-zone rates
        zone_rate_samples: Dict[str, List[float]] = defaultdict(list)
        served_pincodes: List[int] = []

        for row in data_rows:
            try:
                if pin_col >= len(row):
                    continue
                pin_val = str(row[pin_col]).strip()
                if not pin_val or pin_val.lower() in ("", "nan", "none", "pin code"):
                    continue
                pin_int = int(float(pin_val))
                if not (100000 <= pin_int <= 999999):
                    continue
                served_pincodes.append(pin_int)

                dest_zone = self.pincode_to_zone(pin_int)
                if not dest_zone:
                    continue

                if rate_col is not None and rate_col < len(row):
                    rate_str = str(row[rate_col]).strip().replace(",", "")
                    if rate_str and rate_str.lower() not in ("", "nan", "none"):
                        rate = float(rate_str)
                        if 0.5 <= rate <= 150.0:   # base freight per kg; reject outliers
                            zone_rate_samples[dest_zone].append(rate)
            except (ValueError, TypeError):
                continue

        if not zone_rate_samples and not served_pincodes:
            return None

        if not zone_rate_samples:
            print(f"[OICR] Station-rate: only pincodes ({len(served_pincodes)}) — no rates")
            return {"served_pincodes": list(set(served_pincodes))}

        # ── Trimmed mean per destination zone ────────────────────────────────
        # Simple average lets a single high-rate remote station (Leh, Andaman)
        # drag the zone average up. Trimmed mean removes top/bottom 15% first.
        def _trimmed_mean(vals: list) -> float:
            s = sorted(vals)
            n = len(s)
            if n <= 2:
                return round(sum(s) / n, 2)
            cut = max(1, int(n * 0.15))
            trimmed = s[cut: n - cut] if n > 2 * cut else s
            return round(sum(trimmed) / len(trimmed), 2)

        origin_rates: Dict[str, float] = {
            dz: _trimmed_mean(rates)
            for dz, rates in zone_rate_samples.items()
        }

        print(f"[OICR] Station-rate: {origin_zone}->{len(origin_rates)} zones: "
              f"{dict(sorted(origin_rates.items()))}")

        # Return only the vendor's actual rates — single origin row, no extrapolation.
        return {
            "zone_matrix":      {origin_zone: origin_rates},
            "served_pincodes":  list(set(served_pincodes)),
        }

    # ─── 1. Station-rate matrix detection (DataFrame variant) ─────────────────

    def detect_station_rate_format(self, df) -> Optional[Dict]:
        """
        Detect TCI-style station-to-station rate table and extract zone matrix.

        Expected columns: From Station | To Station | [Branch] | Rate | KM | ... | PIN Code | City
        All rows have the same From Station (e.g. "OKL" = Kolkata).

        Returns dict with 'zone_matrix', 'served_pincodes', 'charges' on success.
        """
        if df is None or len(df) < 5:
            return None

        headers = [str(c).strip() for c in df.columns]
        h_lower = [h.lower() for h in headers]

        # Find key columns
        from_col = rate_col = km_col = pin_col = None
        for i, h in enumerate(h_lower):
            if "from" in h and ("station" in h or h.strip() == "from"):
                from_col = i
            if ("rate" in h or "out card" in h or "freight" in h) and rate_col is None:
                rate_col = i
            if h.strip() in ("km", "distance", "kms"):
                km_col = i
            if "pin" in h and pin_col is None:
                pin_col = i

        # Need at least a rate column and a pincode column
        if pin_col is None or (rate_col is None and km_col is None):
            return None

        # Check "From Station" is consistent (single origin)
        if from_col is not None:
            col_data = df.iloc[:, from_col].dropna().astype(str).str.strip()
            unique_origins = col_data[col_data != ""].unique()
            if len(unique_origins) == 0 or len(unique_origins) > 5:
                return None  # Multiple origins -> not a single-origin rate card
            origin_station = unique_origins[0].upper().strip()
        else:
            # No from column -> assume single origin from filename/context
            origin_station = "OKL"  # Default to Kolkata

        origin_zone = STATION_ZONE_MAP.get(origin_station)
        if not origin_zone:
            # Try to infer from city name
            inferred = city_to_zones(origin_station)
            origin_zone = inferred[0] if inferred else "E1"

        print(f"[OICR] Station-rate format: origin={origin_station} -> zone={origin_zone}")

        # Extract pincodes + rates
        zone_rate_samples: Dict[str, List[float]] = defaultdict(list)  # dest_zone -> [rates]
        served_pincodes: List[int] = []

        for idx, row in df.iterrows():
            try:
                pin_val = row.iloc[pin_col]
                if not pin_val or str(pin_val).strip() in ("", "nan", "None"):
                    continue
                pin_int = int(float(str(pin_val).strip()))
                if pin_int < 100000 or pin_int > 999999:
                    continue
                served_pincodes.append(pin_int)

                dest_zone = self.pincode_to_zone(pin_int)
                if not dest_zone:
                    continue

                rate_val = None
                if rate_col is not None:
                    try:
                        r = str(row.iloc[rate_col]).strip().replace(",", "")
                        rate_val = float(r)
                    except (ValueError, TypeError):
                        pass

                if rate_val and 1.0 <= rate_val <= 200.0:  # sanity check
                    zone_rate_samples[dest_zone].append(rate_val)

            except (ValueError, TypeError, IndexError):
                continue

        if not zone_rate_samples:
            print(f"[OICR] Station-rate: no zone rates extracted (pincodes={len(served_pincodes)})")
            return {"served_pincodes": served_pincodes} if served_pincodes else None

        # ── Trimmed mean per destination zone (DataFrame variant) ───────────────
        def _tmean(vals: list) -> float:
            s = sorted(vals)
            n = len(s)
            if n <= 2:
                return round(sum(s) / n, 2)
            cut = max(1, int(n * 0.15))
            trimmed = s[cut: n - cut] if n > 2 * cut else s
            return round(sum(trimmed) / len(trimmed), 2)

        origin_rates: Dict[str, float] = {
            dz: _tmean(rates) for dz, rates in zone_rate_samples.items()
        }

        print(f"[OICR] Station-rate: {origin_zone} -> {len(origin_rates)} zones: "
              f"{sorted(origin_rates.items())}")

        # Return only the vendor's actual rates — no extrapolation to other origins.
        return {
            "zone_matrix": {origin_zone: origin_rates},
            "served_pincodes": list(set(served_pincodes)),
        }

    # ─── Corner-cell patterns that indicate a zone matrix header ─────────────────
    # These appear in the top-left cell of a "To-> / From|" style rate table.
    _CORNER_CELL_RE = re.compile(
        r'^\s*(?:from[/\\]?to|to[/\\]?from|from|to|'
        r'origin[/\\]?dest(?:ination)?|o(?:rig)?[/\\]d(?:est)?|'
        r'from\s*station|zone|lanes?|route)\s*$',
        re.I
    )

    _ZONE_SET = frozenset(ALL_ZONES)

    def _cell_to_zones(self, cell: str) -> List[str]:
        """
        Map a cell value to FC4 zone code(s). Checks, in order:
          1. Direct canonical zone code (N1, S2, NE1 ...)
          2. CITY_ZONE_MAP lookup (city / region name)
          3. Partial city match
          4. Pincode lookup (6-digit cell)
        """
        txt = cell.strip().upper()
        if not txt or self._CORNER_CELL_RE.match(txt):
            return []
        # 1. Direct canonical
        if txt in self._ZONE_SET:
            return [txt]
        # 2. City/region dict
        zones = city_to_zones(txt)
        if zones:
            return zones
        # 3. Pincode: "400001" -> lookup zone
        if txt.isdigit() and len(txt) == 6:
            z = self.pincode_to_zone(int(txt))
            if z:
                return [z]
        return []

    # ─── 2. City-based zone matrix detection ──────────────────────────────────

    def detect_city_zone_matrix(self, table_rows: List[List[str]],
                                 context: str = "") -> Optional[Dict]:
        """
        Parse a zone matrix table where rows = origins, columns = destinations.

        Handles three header formats:
          A) City names:  FROM/TO | DEL/NCR | BANGALORE | CHENNAI
          B) Zone codes:  To->     | N1      | N2        | S1      (FC4 standard)
          C) Mixed:       FROM    | NORTH   | SOUTH     | EAST

        Detection: finds the row with ≥3 resolvable zone/city hits.
        From-column: the column immediately left of the first resolved dest column.
        """
        if not table_rows or len(table_rows) < 2:
            return None

        _ZONE_SET = self._ZONE_SET

        # ── Find header row ───────────────────────────────────────────────────
        header_row_idx = None
        dest_zones: Dict[int, List[str]] = {}  # col_idx -> [zone codes]

        for i, row in enumerate(table_rows[:20]):   # search first 20 rows
            cells = [str(c).strip().upper() for c in row]
            col_zones: Dict[int, List[str]] = {}
            for j, cell in enumerate(cells):
                zones = self._cell_to_zones(cell)
                if zones:
                    col_zones[j] = zones
            if len(col_zones) >= 3:
                header_row_idx = i
                dest_zones = col_zones
                break

        if header_row_idx is None or not dest_zones:
            return None

        # ── Identify from-column ──────────────────────────────────────────────
        # from_col is the column immediately left of the first resolved dest col.
        # If the first resolved col IS col 0 (no label column), from_col = 0 too.
        first_dest_col = min(dest_zones.keys())
        from_col = max(0, first_dest_col - 1)

        # ── Parse data rows ───────────────────────────────────────────────────
        zone_rates: Dict[str, Dict[str, float]] = {}
        max_dest_col = max(dest_zones.keys())

        for i, row in enumerate(table_rows):
            if i <= header_row_idx:
                continue
            cells = [str(c).strip() for c in row]
            if len(cells) <= max_dest_col:
                continue

            # Resolve origin zone from from-column
            from_cell = cells[from_col].upper().strip()
            from_zones = self._cell_to_zones(from_cell)

            if not from_zones:
                # "USER" / blank = single-origin with context
                if "user" in from_cell.lower() or from_cell in ("", "-", "NONE"):
                    from_zones = self._cell_to_zones(context.upper()) or ["E1"]
                else:
                    continue   # unresolvable origin — skip row

            # Extract rates for each destination column
            for col_idx, d_zones in dest_zones.items():
                if col_idx >= len(cells):
                    continue
                try:
                    rate_str = cells[col_idx].replace(",", "").strip()
                    if not rate_str or rate_str.upper() in ("N/A", "-", "NA", ""):
                        continue
                    rate = float(rate_str)
                    if not (0.5 <= rate <= 500.0):
                        continue
                except (ValueError, TypeError):
                    continue

                for fz in from_zones:
                    zone_rates.setdefault(fz, {})
                    for dz in d_zones:
                        if dz in zone_rates[fz]:
                            # Running average when multiple rows map same pair
                            zone_rates[fz][dz] = round(
                                (zone_rates[fz][dz] + rate) / 2, 3)
                        else:
                            zone_rates[fz][dz] = rate

        if not zone_rates:
            return None

        # ── Road-rate sanity pass ─────────────────────────────────────────────
        # Drop any origin whose MINIMUM destination rate looks like an air rate
        # (road freight: 5–50 Rs/kg; air: typically >50 Rs/kg for short hauls).
        zone_rates = self._filter_air_rows(zone_rates)
        if not zone_rates:
            return None

        # Return only what the vendor explicitly provided — no gap-filling.
        print(f"[OICR] City zone matrix: {len(zone_rates)} origins extracted")
        return zone_rates

    # ─── Road-rate filter ─────────────────────────────────────────────────────

    def _filter_air_rows(
        self, zone_rates: Dict[str, Dict[str, float]]
    ) -> Dict[str, Dict[str, float]]:
        """
        Remove origin rows whose rates look like air freight (min > 60 Rs/kg).
        A transporter may show both road and air rates in the same table; we only
        want road.  If ALL rows look like air, keep them (may be an air-only carrier;
        let the caller decide).
        """
        ROAD_MAX_MIN = 60.0   # if cheapest dest rate > this, likely air

        air_rows = [
            orig for orig, dests in zone_rates.items()
            if dests and min(dests.values()) > ROAD_MAX_MIN
        ]
        if len(air_rows) == len(zone_rates):
            # Every row looks like air — don't filter (may be a valid air matrix,
            # or the rates are Rs/100kg / effective — keep and let upstream decide)
            if air_rows:
                print(f"[OICR] WARNING: all origin rows have min-rate > {ROAD_MAX_MIN} "
                      f"(possibly air rates or effective all-in rates)")
            return zone_rates

        for orig in air_rows:
            print(f"[OICR] Dropping origin '{orig}' — min rate "
                  f"{min(zone_rates[orig].values()):.1f} > {ROAD_MAX_MIN} Rs/kg "
                  f"(looks like air/all-in, not road base rate)")
            del zone_rates[orig]

        return zone_rates

    # ─── Pincode zone inference ───────────────────────────────────────────────

    def infer_zones_from_pincodes(
        self, pincodes: List[int]
    ) -> Dict[str, int]:
        """
        Group pincodes by canonical zone.
        Returns {zone_code: count} sorted by count descending.
        """
        zone_counts: Dict[str, int] = defaultdict(int)
        unresolved = 0
        for pin in pincodes:
            zone = self.pincode_to_zone(pin)
            if zone:
                zone_counts[zone] += 1
            else:
                unresolved += 1
        if unresolved:
            print(f"[OICR] infer_zones: {unresolved}/{len(pincodes)} pincodes had no zone match")
        return dict(sorted(zone_counts.items(), key=lambda x: -x[1]))

    # ─── Partial zone matrix fill ─────────────────────────────────────────────

    def fill_partial_zone_matrix(
        self,
        partial_matrix: Dict[str, Dict[str, float]],
    ) -> Dict[str, Dict[str, float]]:
        """
        Given a partial zone matrix (fewer than 19 origins), fill in missing
        origins using distance-weighted interpolation from the known origins.

        Strategy per missing origin Z:
          • Find the K=3 closest KNOWN origins by haversine distance.
          • For each destination D, compute a distance-weighted average of the
            known rates, scaled by the ratio dist(Z,D)/dist(known,D).
          • Apply the same dampening formula as _extrapolate_zone_matrix.

        This preserves the relative structure of the known rates while filling
        gaps that would otherwise leave the frontend with an incomplete matrix.
        """
        known = {z: dests for z, dests in partial_matrix.items()
                 if z in ZONE_COORDS and dests}
        if not known:
            return partial_matrix

        missing = [z for z in ALL_ZONES if z not in known and z in ZONE_COORDS]
        if not missing:
            return partial_matrix   # already complete

        print(f"[OICR] Filling {len(missing)} missing origins via interpolation: "
              f"{sorted(missing)}")

        # Build set of all destination zones that appear in the known matrix
        all_dest_zones = set()
        for dests in known.values():
            all_dest_zones.update(dests.keys())

        filled = {z: dict(dests) for z, dests in partial_matrix.items()}
        K = 3   # number of nearest-origin neighbours to use

        for missing_z in missing:
            mz_coord = ZONE_COORDS[missing_z]

            # Sort known origins by distance from missing_z
            neighbours = sorted(
                [(kz, _haversine(ZONE_COORDS[kz], mz_coord)) for kz in known],
                key=lambda x: x[1]
            )[:K]

            if not neighbours:
                continue

            dest_rates: Dict[str, float] = {}

            for dest_z in all_dest_zones:
                if dest_z not in ZONE_COORDS:
                    continue
                dest_coord = ZONE_COORDS[dest_z]
                dist_missing_to_dest = _haversine(mz_coord, dest_coord)

                weighted_sum   = 0.0
                weight_total   = 0.0
                for ref_z, ref_dist_to_missing in neighbours:
                    if dest_z not in known[ref_z]:
                        continue
                    ref_rate = known[ref_z][dest_z]
                    dist_ref_to_dest = _haversine(ZONE_COORDS[ref_z], dest_coord)

                    # Scale ref_rate by relative distance ratio (dampened)
                    if dist_ref_to_dest > 1.0:
                        ratio    = dist_missing_to_dest / dist_ref_to_dest
                        dampened = max(0.5, min(2.5, 0.65 + 0.35 * ratio))
                        scaled   = ref_rate * dampened
                    else:
                        scaled = ref_rate   # essentially the same location

                    # Weight = 1 / (distance to neighbour + 1) to avoid ÷0
                    w = 1.0 / (ref_dist_to_missing + 1.0)
                    weighted_sum  += scaled * w
                    weight_total  += w

                if weight_total > 0:
                    dest_rates[dest_z] = round(weighted_sum / weight_total, 2)

            if dest_rates:
                filled[missing_z] = dest_rates
                print(f"[OICR]   Interpolated {missing_z}: "
                      f"{len(dest_rates)} dest zones from {[n[0] for n in neighbours]}")

        return filled

    # ─── Rate mode classifier ─────────────────────────────────────────────────

    def classify_rate_mode(
        self,
        zone_rates: Dict[str, Dict[str, float]],
    ) -> Dict[str, Any]:
        """
        Determine whether zone_rates look like road, air, or effective (all-in) rates.

        Road freight base:  5 – 30 Rs/kg for nearby zones; up to 50 for far
        Air freight base:   50 – 200 Rs/kg
        Effective all-in:   15 – 60 Rs/kg (road base + fuel + docket baked in)

        Returns dict:
          {
            "mode": "road" | "air" | "effective" | "unknown",
            "min_rate": float, "max_rate": float, "avg_rate": float,
            "warning": str | None,
          }
        """
        all_rates = [
            r for dests in zone_rates.values()
            for r in dests.values()
            if isinstance(r, (int, float)) and r > 0
        ]
        if not all_rates:
            return {"mode": "unknown", "warning": "No rates found"}

        min_r = min(all_rates)
        max_r = max(all_rates)
        avg_r = sum(all_rates) / len(all_rates)

        if min_r > 60:
            mode    = "air"
            warning = (f"Minimum rate {min_r:.1f} Rs/kg looks like AIR freight. "
                       f"Road base rates are typically 5–30 Rs/kg.")
        elif min_r > 30:
            mode    = "effective"
            warning = (f"Minimum rate {min_r:.1f} Rs/kg may be an effective/all-in "
                       f"rate (road base + surcharges). Verify before adding fuel/docket on top.")
        elif max_r > 200:
            mode    = "mixed"
            warning = (f"Rate range {min_r:.1f}–{max_r:.1f} is very wide. "
                       f"Special-zone rates (X1/X2/X3) may be inflating the max.")
        else:
            mode    = "road"
            warning = None

        return {
            "mode":     mode,
            "min_rate": round(min_r, 2),
            "max_rate": round(max_r, 2),
            "avg_rate": round(avg_r, 2),
            "warning":  warning,
        }

    # ─── 2b. City-rate-card detection ────────────────────────────────────────────
    # Handles single-origin rate cards like:
    #   EX DELHI
    #   Destination | State | Rate (Per Kg Min 30 Kg) | TAT
    #   AGRA        | UP    | 10                      | 24 Hrs
    #   AMRITSAR    | Punjab| 12                      | 24 Hrs
    # ─────────────────────────────────────────────────────────────────────────────

    # Column-header keywords that identify the destination column
    _DEST_HEADER_RE = re.compile(
        r'(?i)\b(?:destination|dest|to\s*city|city|location|consignee\s*city|'
        r'deliver(?:y|ing)?\s*(?:city|location)?|hub|station|branch)\b'
    )
    # Column-header keywords that identify a road-rate column
    _RATE_HEADER_RE = re.compile(
        r'(?i)\b(?:rate|rates?|charge|charges?|freight|per\s*kg|rs\.?/kg|'
        r'pricing|amount|cost)\b'
    )
    # Column-header keywords for state
    _STATE_HEADER_RE = re.compile(r'(?i)\b(?:state|province|region)\b')
    # Origin signals: "EX DELHI", "FROM DELHI", "ORIGIN: MUMBAI"
    _ORIGIN_SIGNAL_RE = re.compile(
        r'(?i)\b(?:ex[:\s]+|from[:\s]+|origin[:\s]*|dispatching\s+from[:\s]+|'
        r'base[:\s]+(?:city|location)[:\s]+)'
        r'([A-Z][A-Z /&,.-]{2,40})',
        re.I,
    )

    def detect_city_rate_card(
        self,
        table_rows: List[List[str]],
        context_text: str = "",
    ) -> Optional[Dict]:
        """
        Parse a single-origin city-rate-card table where each row gives a
        destination city and a rate-per-kg.  Maps each city to its FC4 zone
        using city_to_zones(), then averages rates per zone.

        Returns a partial zone matrix (1 origin -> N destinations) on success,
        or None if the table doesn't match this format.

        The single-origin zone is inferred from context_text (e.g. "EX DELHI")
        or defaults to N1 (Delhi) since most Indian transporters publish rate cards
        from their primary Delhi hub.
        """
        if not table_rows or len(table_rows) < 3:
            return None

        # ── Find ALL header rows (a table can have multiple sections) ────────────
        # Search the whole table — pdfplumber sometimes puts multiple "By Road"
        # sub-sections in a single table; each section has its own header.
        header_segments: List[tuple] = []   # list of (header_idx, dest_col, rate_col, state_col)
        for i, row in enumerate(table_rows):
            cells = [str(c).strip() for c in row]
            dc = rc = sc = None
            for j, cell in enumerate(cells):
                if dc is None and self._DEST_HEADER_RE.search(cell):
                    dc = j
                if rc is None and self._RATE_HEADER_RE.search(cell):
                    rc = j
                if sc is None and self._STATE_HEADER_RE.search(cell):
                    sc = j
            if dc is not None and rc is not None:
                header_segments.append((i, dc, rc, sc))

        if not header_segments:
            return None

        # Use first header to detect the origin (preamble scan)
        header_idx, dest_col, rate_col, state_col = header_segments[0]

        # ── Infer origin zone from context_text ───────────────────────────────
        origin_zone = "N1"   # default: Delhi (most common hub)
        m = self._ORIGIN_SIGNAL_RE.search(context_text)
        if m:
            origin_city = m.group(1).strip().upper()
            inferred = self._cell_to_zones(origin_city)
            if inferred:
                origin_zone = inferred[0]
                print(f"[OICR] City-rate-card origin: '{origin_city}' -> {origin_zone}")
            else:
                print(f"[OICR] City-rate-card: couldn't resolve origin '{origin_city}', "
                      f"defaulting to N1")
        else:
            # Also try scanning the table preamble rows for EX/FROM/ORIGIN signals
            for row in table_rows[:header_idx]:
                row_text = " ".join(str(c).strip() for c in row)
                m2 = self._ORIGIN_SIGNAL_RE.search(row_text)
                if m2:
                    oc = m2.group(1).strip().upper()
                    inferred = self._cell_to_zones(oc)
                    if inferred:
                        origin_zone = inferred[0]
                        print(f"[OICR] City-rate-card origin (preamble): '{oc}' -> {origin_zone}")
                    break

        # ── Extract dest -> rate mappings across all header sections ─────────────
        # Build row ranges per section: each section spans from its header+1
        # to just before the next header (or end of table).
        zone_rate_samples: Dict[str, List[float]] = defaultdict(list)
        skipped = 0

        # Regex to detect air-mode section label rows
        _AIR_LABEL_RE  = re.compile(r'(?i)\bby\s+air\b')
        _ROAD_LABEL_RE = re.compile(r'(?i)\bby\s+(?:road|surface|ground|express)\b')

        # Build section ranges and determine mode for each section.
        # Scan backwards from each header to find the nearest mode label above it.
        sections = []
        for si, (hi, dc, rc, sc) in enumerate(header_segments):
            start = hi + 1
            end   = header_segments[si + 1][0] if si + 1 < len(header_segments) else len(table_rows)
            # Determine section mode: look for "By Air" / "By Road" label above this header
            mode = "road"   # default — assume road unless explicitly labelled as air
            for check_row in reversed(table_rows[max(0, hi - 5):hi + 1]):
                row_text = " ".join(str(c).strip() for c in check_row)
                if _AIR_LABEL_RE.search(row_text):
                    mode = "air"
                    break
                if _ROAD_LABEL_RE.search(row_text):
                    mode = "road"
                    break
            if mode == "air":
                print(f"[OICR] City-rate-card: skipping AIR section at header row {hi}")
            sections.append((start, end, dc, rc, sc, mode))

        for (start, end, d_col, r_col, s_col, section_mode) in sections:
            if section_mode == "air":
                continue   # ignore entire air section

            in_air_block = False   # inline "By Air" sub-block tracker
            for row in table_rows[start:end]:
                cells = [str(c).strip() for c in row]
                row_text = " ".join(cells)

                # Detect inline mode-switch labels within the section
                if _AIR_LABEL_RE.search(row_text):
                    in_air_block = True
                    continue
                if _ROAD_LABEL_RE.search(row_text):
                    in_air_block = False
                    continue
                if in_air_block:
                    continue   # skip rows inside an inline air sub-block

                if d_col >= len(cells) or r_col >= len(cells):
                    continue

                dest_cell = cells[d_col].strip().upper()
                if not dest_cell or dest_cell in ("DESTINATION", "DEST", "CITY"):
                    continue

                # Map destination city -> zone(s)
                dest_zones = self._cell_to_zones(dest_cell)

                # Fallback: state column
                if not dest_zones and s_col is not None and s_col < len(cells):
                    state_cell = cells[s_col].strip().upper()
                    dest_zones = self._cell_to_zones(state_cell)

                if not dest_zones:
                    skipped += 1
                    continue

                # Parse rate
                try:
                    rate_str = cells[r_col].replace(",", "").strip()
                    rate_str = re.sub(r'(?i)(?:rs\.?\s*|per\s*kg|/\s*kg)', '', rate_str).strip()
                    rate = float(rate_str)
                    if rate <= 0 or rate > 500:
                        continue
                except (ValueError, TypeError):
                    continue

                for dz in dest_zones:
                    zone_rate_samples[dz].append(rate)

        if not zone_rate_samples:
            return None

        if skipped:
            print(f"[OICR] City-rate-card: {skipped} rows skipped (city not in zone map)")

        # ── Trimmed-mean per dest zone ────────────────────────────────────────
        def _tmean(vals: list) -> float:
            s = sorted(vals)
            n = len(s)
            if n <= 2:
                return round(sum(s) / n, 2)
            cut = max(1, int(n * 0.15))
            trimmed = s[cut: n - cut] if n > 2 * cut else s
            return round(sum(trimmed) / len(trimmed), 2)

        origin_rates = {dz: _tmean(rates) for dz, rates in zone_rate_samples.items()}

        print(f"[OICR] City-rate-card: origin={origin_zone} -> "
              f"{len(origin_rates)} dest zones: "
              f"{dict(sorted(origin_rates.items()))}")

        # ── Road-rate sanity check ────────────────────────────────────────────
        # If the min rate across normal zones is > 60 Rs/kg this is almost
        # certainly an AIR rate section; reject it so road rates aren't
        # contaminated.
        normal_rates = [r for z, r in origin_rates.items()
                        if not z.startswith('X')]
        if normal_rates and min(normal_rates) > 60:
            print(f"[OICR] City-rate-card rejected: min normal rate "
                  f"{min(normal_rates):.1f} > 60 Rs/kg — likely AIR rates")
            return None

        # Return ONLY the vendor's actual rates — no extrapolation to other origins.
        # A city-rate-card is single-origin (the vendor only ships from one hub).
        # Only the origin zone row is filled; all other origins stay empty.
        return {origin_zone: origin_rates}

    # ─── 3. Charge extraction ──────────────────────────────────────────────────

    def extract_charges_from_text(self, text: str) -> Dict[str, Any]:
        """
        Extract all pricing charges from free text (PDF proposal, charge tables).
        Returns a charges dict compatible with fc4_encoder._encode_pricing().
        """
        charges: Dict[str, Any] = {}
        text_lower = text.lower()

        for field, patterns, value_type in _CHARGE_REGEXES:
            for pat in patterns:
                try:
                    # CRITICAL: never use re.DOTALL here — it allows patterns to
                    # cross line boundaries and pick up volumetric constants (1728)
                    # from adjacent lines in the same section.
                    m = re.search(pat, text_lower, re.IGNORECASE)
                    if not m:
                        continue
                    if value_type == "scalar" or value_type == "pct":
                        val = float(m.group(1).replace(",", ""))
                        if field not in charges or val > 0:
                            charges[field] = val
                        break
                    elif value_type == "vf":
                        v = float(m.group(1).replace(",", ""))
                        f = float(m.group(2).replace(",", "")) if m.lastindex >= 2 else 0.0
                        charges[field] = {"v": v, "f": f}
                        break
                except (ValueError, IndexError, AttributeError):
                    continue

        # Special TCI-specific: "DWB Charges Rs.200" -> docketCharges = 200
        m = re.search(r'dwb\s*charges?\s*(?:rs\.?\s*)?(\d+)', text_lower)
        if m and "docketCharges" not in charges:
            charges["docketCharges"] = float(m.group(1))

        # "Fuel Surcharge" without explicit %: check if numeric value given
        if "fuel" not in charges:
            m = re.search(r'fuel\s*surcharge[^\d%]*?(\d+(?:\.\d+)?)\s*%?', text_lower)
            if m:
                val = float(m.group(1))
                if val <= 100:  # percentage
                    charges["fuel"] = val

        # SFC minimum: "SFC-Rs-350" -> minCharges = 350
        m = re.search(r'sfc\s*[-:]\s*rs\s*[-:]?\s*(\d+)', text_lower)
        if m and "minCharges" not in charges:
            charges["minCharges"] = float(m.group(1))

        # SFC minimum weight: "SFC-30 Kg"
        m = re.search(r'sfc\s*[-:]\s*(\d+)\s*kg', text_lower)
        if m and "minWeight" not in charges:
            charges["minWeight"] = float(m.group(1))

        # FOV Owners Risk: "0.2% min Rs 100"
        m = re.search(r'fov\s*(?:owners?\s*risk)?[^\d]*?(\d+(?:\.\d+)?)\s*%\s*min\s*(?:rs\.?\s*)?(\d+)', text_lower)
        if m and "rovCharges" not in charges:
            charges["rovCharges"] = {"v": float(m.group(1)), "f": float(m.group(2))}

        # Remove obviously wrong values
        if charges.get("docketCharges", 0) > 5000:  # >5000 is not a docket charge
            del charges["docketCharges"]
        if charges.get("minCharges", 0) > 100000:
            del charges["minCharges"]

        return charges

    def extract_charges_from_table(self, table_rows: List[List[str]]) -> Dict[str, Any]:
        """
        Extract charges from a charge table (multi-row, multi-column format).
        Handles TCI's "DWB | FOD | Minimum Chargeable" header format.
        """
        charges: Dict[str, Any] = {}

        for i, row in enumerate(table_rows):
            row_text = " ".join(str(c).strip() for c in row).lower()
            # Extract numeric values from this row to pair with headers
            nums = re.findall(r'(\d+(?:\.\d+)?)', row_text)

            # DWB in header -> docket charge
            if "dwb" in row_text or "docket" in row_text:
                m = re.search(r'rs\.?\s*(\d+)', row_text)
                if m:
                    val = float(m.group(1))
                    if 50 <= val <= 2000:
                        charges.setdefault("docketCharges", val)

            # FOV / ROV
            if ("fov" in row_text or "rov" in row_text or "freight on value" in row_text):
                m_pct = re.search(r'(\d+(?:\.\d+)?)\s*%', row_text)
                m_min = re.search(r'min\s*(?:rs\.?\s*)?(\d+)', row_text)
                if m_pct:
                    v = float(m_pct.group(1))
                    f = float(m_min.group(1)) if m_min else 0.0
                    charges.setdefault("rovCharges", {"v": v, "f": f})

            # SFC/minimum charges
            if "minimum" in row_text and ("freight" in row_text or "charge" in row_text):
                m = re.search(r'sfc\s*[-:]?\s*rs\s*[-:]?\s*(\d+)', row_text)
                if m:
                    charges.setdefault("minCharges", float(m.group(1)))

            # Minimum weight
            if "minimum" in row_text and "weight" in row_text:
                m = re.search(r'sfc\s*[-:]?\s*(\d+)\s*kg', row_text)
                if m:
                    charges.setdefault("minWeight", float(m.group(1)))

            # DACC
            if "dacc" in row_text or "delivery against consignee" in row_text:
                m = re.search(r'rs\.?\s*(\d+)', row_text)
                if m:
                    charges.setdefault("daccCharges", float(m.group(1)))

            # COD
            if ("cod" in row_text and "cash on delivery" in row_text) or \
               (row_text.strip().startswith("cod") and nums):
                m = re.search(r'rs\.?\s*(\d+)', row_text)
                if m:
                    val = float(m.group(1))
                    if val > 0:
                        charges.setdefault("codCharges", {"v": 0.0, "f": val})

        return charges

    # ─── 4. Company info extraction ────────────────────────────────────────────

    def extract_company_info(self, text: str) -> Dict[str, Any]:
        """Extract company identity fields from free text."""
        info: Dict[str, Any] = {}

        # Known company names
        for key, meta in _COMPANY_NAMES.items():
            if key in text.upper():
                info.setdefault("companyName", meta["canonical"])
                info.setdefault("shortName", meta["short"])
                break

        # Regex fields
        for field, pattern in _COMPANY_REGEXES.items():
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                val = m.group(1).strip()
                if val:
                    info.setdefault(field, val)

        # Address detection: look for "TCI Tower, Nehru Place" etc.
        addr_m = re.search(
            r'(?:address|regd\.?\s*office|head\s*office)[^\n:]*?:\s*([^\n]{10,100})',
            text, re.IGNORECASE
        )
        if addr_m:
            info.setdefault("address", addr_m.group(1).strip())

        return info

    # ─── 5. OCR via Tesseract (optional) ──────────────────────────────────────

    def ocr_image(self, image_path: str) -> str:
        """
        Run Tesseract OCR on an image file.
        Returns extracted text (empty string if Tesseract unavailable).
        """
        try:
            import pytesseract
            from PIL import Image
            import cv2
            import numpy as np

            # OpenCV preprocessing for better accuracy
            img = cv2.imread(image_path)
            if img is None:
                img_pil = Image.open(image_path)
                img = np.array(img_pil.convert("RGB"))

            # Grayscale + adaptive threshold
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            # Denoise
            gray = cv2.fastNlMeansDenoising(gray, h=10)
            # Adaptive threshold
            thresh = cv2.adaptiveThreshold(
                gray, 255,
                cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY, 11, 2
            )
            # Scale up if small
            h, w = thresh.shape
            if w < 1200:
                scale = 1200 / w
                thresh = cv2.resize(thresh, None, fx=scale, fy=scale,
                                    interpolation=cv2.INTER_CUBIC)

            # Multi-pass OCR
            texts = []
            for psm in (6, 3, 11):
                cfg = f"--oem 3 --psm {psm}"
                try:
                    t = pytesseract.image_to_string(thresh, config=cfg, lang="eng")
                    if t.strip():
                        texts.append(t)
                except Exception:
                    pass

            # Pick best (longest) result
            return max(texts, key=len, default="")

        except ImportError:
            print("[OICR] Tesseract/cv2 not available — skipping OCR")
            return ""
        except Exception as e:
            print(f"[OICR] OCR error: {e}")
            return ""

    # ─── 6. Zone matrix extrapolation ─────────────────────────────────────────

    def _extrapolate_zone_matrix(
        self, origin_zone: str, origin_rates: Dict[str, float]
    ) -> Dict[str, Dict[str, float]]:
        """
        Build a full N×N zone matrix from a single origin's rates.

        Uses: rate(A->B) ≈ rate(Origin->B) × f(dist(A,B) / dist(Origin,B))

        Dampening: f(r) = 0.65 + 0.35 × r  (real freight scales sub-linearly)
        Caps:
          • Per-cell floor: same-zone rate (can't be cheaper than local)
          • Per-cell ceiling: 2.5× origin rate (prevents runaway extrapolation)

        Sanity: if the minimum input rate > 25 Rs/kg, it's likely an all-in/effective
        rate with surcharges already baked in.  Log a warning — callers should not add
        fuel/docket on top of an already-effective rate.
        """
        if not origin_rates or origin_zone not in ZONE_COORDS:
            return {origin_zone: origin_rates} if origin_rates else {}

        # ── Sanity check: detect effective-pricing (charges baked in) ─────────
        regular_rates = [r for z, r in origin_rates.items() if not z.startswith('X') and r > 0]
        if regular_rates:
            min_base = min(regular_rates)
            if min_base > 25.0:
                print(f"[OICR] WARNING: Minimum zone rate {min_base:.2f} Rs/kg looks like an "
                      f"all-in/effective rate (charges already baked in). "
                      f"Store the BASE freight rate — fuel, docket, GST should be separate fields.")
            elif min_base > 15.0:
                print(f"[OICR] NOTE: Zone rates appear high (min={min_base:.2f}). "
                      f"Verify these are base per-kg rates, not all-in rates.")

        full = {}

        # Fill any missing destination zones using distance interpolation
        origin_to_all = self._fill_zone_gaps(origin_zone, origin_rates)

        # Global rate bounds for capping
        all_vals    = [r for r in origin_to_all.values() if r > 0]
        global_max  = max(all_vals) if all_vals else 100.0
        global_min  = min(all_vals) if all_vals else 1.0

        # Build rates for every possible origin zone
        for from_z in ALL_ZONES:
            if from_z == origin_zone:
                full[from_z] = dict(origin_to_all)
                continue

            if from_z not in ZONE_COORDS:
                continue

            from_z_rates: Dict[str, float] = {}
            for to_z, base_rate in origin_to_all.items():
                if to_z not in ZONE_COORDS:
                    continue

                dist_orig_to = _haversine(ZONE_COORDS[origin_zone], ZONE_COORDS[to_z])
                dist_from_to = _haversine(ZONE_COORDS[from_z],      ZONE_COORDS[to_z])

                if dist_orig_to < 1.0:
                    # Essentially the same zone — keep base rate
                    from_z_rates[to_z] = round(base_rate, 2)
                else:
                    ratio    = dist_from_to / dist_orig_to
                    # Dampened ratio: 0.65 base + 0.35 × distance ratio
                    # Capped between 0.5× and 2.5× to prevent runaway values
                    dampened = max(0.5, min(2.5, 0.65 + 0.35 * ratio))
                    rate_val = round(base_rate * dampened, 2)
                    # Floor: never below global minimum for this transporter
                    # Ceiling: never above 2.5× the global maximum
                    from_z_rates[to_z] = round(
                        max(global_min * 0.8, min(global_max * 2.5, rate_val)), 2
                    )

            full[from_z] = from_z_rates

        return full

    def _fill_zone_gaps(
        self, origin_zone: str, origin_rates: Dict[str, float]
    ) -> Dict[str, float]:
        """
        Fill in missing destination zones using distance interpolation.

        CRITICAL RULE: X-zones (X1=Andaman, X2=Lakshadweep, X3=J&K/Ladakh) have
        extremely high rates due to remoteness / island surcharges.  Using them as
        interpolation references for regular zones (N, S, E, W, C, NE) produces
        wildly inflated rates — the "Leh/Agartala rate applied to Kolkata" bug.

        Strategy:
          • For a regular destination (N/S/E/W/C/NE): use ONLY regular zones as
            reference pool and cap interpolated rate at max_regular_rate.
          • For a special destination (X1/X2/X3): allow any zone as reference,
            cap at max_rate * 2 (these really are expensive).
        """
        SPECIAL = {'X1', 'X2', 'X3'}

        filled = dict(origin_rates)

        rates_all   = [r for r in filled.values() if r > 0]
        if not rates_all:
            return filled

        # Separate regular-zone rates so X-zone outliers can't skew the range
        rates_reg   = [r for z, r in filled.items() if z not in SPECIAL and r > 0]
        if not rates_reg:
            rates_reg = rates_all     # all rates are special — use all

        min_r   = min(rates_reg)
        max_r   = max(rates_reg)      # ceiling for regular-zone interpolation
        avg_r   = sum(rates_reg) / len(rates_reg)

        if origin_zone not in ZONE_COORDS:
            # No geo data — fall back to average for everything
            for to_z in ALL_ZONES:
                if to_z not in filled:
                    filled[to_z] = round(avg_r, 2)
            return filled

        for to_z in ALL_ZONES:
            if to_z in filled or to_z not in ZONE_COORDS:
                continue

            is_special_dest = to_z in SPECIAL
            dist_to_missing = _haversine(ZONE_COORDS[origin_zone], ZONE_COORDS[to_z])

            # Reference pool: never use X-zones for regular destinations
            ref_pool = {
                z: r for z, r in filled.items()
                if z in ZONE_COORDS and r > 0
                and (is_special_dest or z not in SPECIAL)
            }
            if not ref_pool:
                # Absolute fallback: average of what we have
                filled[to_z] = round(avg_r, 2)
                continue

            # Find the known zone whose distance-from-origin is closest to
            # the missing zone's distance-from-origin (nearest distance match)
            best_ref      = None
            best_ref_dist = float("inf")
            for known_z, known_rate in ref_pool.items():
                d = abs(_haversine(ZONE_COORDS[origin_zone], ZONE_COORDS[known_z]) - dist_to_missing)
                if d < best_ref_dist:
                    best_ref_dist = d
                    best_ref = (known_z, known_rate)

            if best_ref:
                ref_zone, ref_rate = best_ref
                ref_dist = _haversine(ZONE_COORDS[origin_zone], ZONE_COORDS[ref_zone])
                if ref_dist > 0:
                    ratio    = dist_to_missing / ref_dist
                    interp   = ref_rate * (0.7 + 0.3 * ratio)
                    # Hard cap: regular zones can't exceed the highest regular rate;
                    # special zones can go up to 2× the highest regular rate.
                    cap = max_r if not is_special_dest else max(max_r * 2.0, max(rates_all))
                    filled[to_z] = round(max(min_r, min(cap, interp)), 2)
                else:
                    filled[to_z] = round(ref_rate, 2)

        return filled

    # ─── 7. Full document processing ──────────────────────────────────────────

    def process_pdf_text(self, text: str, tables: List[List[List[str]]]) -> Dict[str, Any]:
        """
        Process extracted PDF text + tables into structured transporter data.
        Returns a merged data dict for the UTSF pipeline.
        """
        result: Dict[str, Any] = {
            "company_details": {},
            "charges": {},
            "zone_matrix": {},
            "served_pincodes": [],
        }

        # Company info from text
        company_info = self.extract_company_info(text)
        if company_info:
            result["company_details"].update(company_info)

        # Zone matrix + structured table charges — higher priority (table data is more reliable)
        # city-rate-card accumulator: merge rates from multiple tables for the same origin
        _crc_origin: Optional[str] = None
        _crc_rates:  Dict[str, float] = {}

        all_table_rows: List[List[str]] = []
        for table in (tables or []):
            if not table:
                continue
            flat_rows = []
            for row in table:
                if isinstance(row, list):
                    flat_rows.append([str(c) for c in row])
            all_table_rows.extend(flat_rows)

            # Try city-based zone matrix (from/to grid) — takes priority
            if not result["zone_matrix"]:
                zm = self.detect_city_zone_matrix(flat_rows)
                if zm:
                    result["zone_matrix"] = zm
                    print(f"[OICR] PDF zone matrix: {len(zm)} origins")

            # Try city-rate-card (single-origin, destination+rate rows)
            # Always try ALL tables and merge results for the same origin.
            # A multi-page rate card splits the North-India and Pan-India sections
            # across different tables; we need to combine them into one origin row.
            if not result["zone_matrix"]:
                zm = self.detect_city_rate_card(flat_rows, context_text=text)
                if zm:
                    origin = list(zm.keys())[0]
                    rates  = zm[origin]
                    if _crc_origin is None:
                        _crc_origin = origin
                    if origin == _crc_origin:
                        for dest, rate in rates.items():
                            # First-seen rate wins: specific city rates (e.g. AGRA=10)
                            # come before catch-all rows (REST OF INDIA=25).
                            # Never overwrite a zone that already has a rate —
                            # catch-alls should only fill zones not yet seen.
                            if dest not in _crc_rates:
                                _crc_rates[dest] = rate

            # Charge table extraction — setdefault: table charges don't overwrite each other
            table_charges = self.extract_charges_from_table(flat_rows)
            for k, v in table_charges.items():
                result["charges"].setdefault(k, v)

        # ── Commit merged city-rate-card results ──────────────────────────────
        if not result["zone_matrix"] and _crc_origin and _crc_rates:
            result["zone_matrix"] = {_crc_origin: _crc_rates}
            print(f"[OICR] City-rate-card (merged): {_crc_origin} -> "
                  f"{len(_crc_rates)} dest zones: {dict(sorted(_crc_rates.items()))}")

        # Charges from free text — LOWER priority, only fill fields not found in tables
        # Uses setdefault so table-extracted values are never overwritten
        text_charges = self.extract_charges_from_text(text)
        if text_charges:
            for k, v in text_charges.items():
                result["charges"].setdefault(k, v)  # text fills gaps only

        # ── 1CFT=Xkg -> divisor conversion (highest priority — overrides table 5000) ──
        # TCI surface formula: 1CFT=10kg -> divisor=28317/10=2832 cm³/kg
        # This MUST override the AIR formula (L*B*H/5000) from the same table.
        cft_m = re.search(r'1\s*cft\s*[=:]\s*(\d+(?:\.\d+)?)\s*kg', text.lower())
        if cft_m:
            kg_cft = float(cft_m.group(1))
            if 0 < kg_cft < 200:
                divisor_sfc = round(28316.8 / kg_cft)
                # Force-set: SFC divisor overrides AIR/any other value
                result["charges"]["divisor"] = float(divisor_sfc)
                result["charges"]["kFactor"] = float(divisor_sfc)
                print(f"[OICR] 1CFT={kg_cft}kg -> divisor={divisor_sfc} cm3/kg (SFC surface)")

        # ── Rate mode classification ───────────────────────────────────────────
        if result["zone_matrix"]:
            mode_info = self.classify_rate_mode(result["zone_matrix"])
            result["_rate_mode"] = mode_info["mode"]
            if mode_info.get("warning"):
                print(f"[OICR] Rate mode: {mode_info['mode']} — {mode_info['warning']}")
            else:
                print(f"[OICR] Rate mode: {mode_info['mode']} "
                      f"(min={mode_info['min_rate']} avg={mode_info['avg_rate']:.1f} "
                      f"max={mode_info['max_rate']} Rs/kg)")

        print(f"[OICR] PDF processed: "
              f"company={list(result['company_details'].keys())} "
              f"charges={list(result['charges'].keys())} "
              f"zone_matrix={len(result['zone_matrix'])} origins")
        return result


# ─── Singleton access ─────────────────────────────────────────────────────────
_engine: Optional[OICREngine] = None

def get_oicr_engine() -> OICREngine:
    """Get (or create) the shared OICR engine instance."""
    global _engine
    if _engine is None:
        _engine = OICREngine()
    return _engine
