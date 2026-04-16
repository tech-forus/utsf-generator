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


def city_to_zones(city_text: str) -> List[str]:
    """Map a city name / abbreviation to one or more FC4 zone codes."""
    txt = city_text.strip().upper()
    if txt in CITY_ZONE_MAP:
        return CITY_ZONE_MAP[txt]
    # Partial match
    for key, zones in CITY_ZONE_MAP.items():
        if key in txt or txt in key:
            return zones
    return []


def station_to_zone(station_code: str) -> Optional[str]:
    """Map a TCI station code to a single FC4 zone."""
    return STATION_ZONE_MAP.get(station_code.upper().strip())


# ─── Charge label patterns ────────────────────────────────────────────────────
# Each: (canonical_field, [regex_patterns], value_type)
# value_type: 'scalar', 'pct', 'vf', 'percent_min_fixed'
_CHARGE_REGEXES = [
    # DWB / Docket charges  ->  look for "DWB Charges Rs.200" or "Docket Rs 50"
    ("docketCharges", [
        r"(?:dwb|docket(?:\s*charge)?|lr\s*charge)[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
        r"(?:document|doc)\s*(?:charge|fee)[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
    ], "scalar"),

    # Fuel surcharge
    ("fuel", [
        r"fuel\s*(?:surcharge|%|percent|sc)[^\d]*(\d+(?:\.\d+)?)\s*%",
        r"(\d+(?:\.\d+)?)\s*%\s*(?:fuel|fsc|f/s)",
    ], "pct"),

    # Minimum charges
    ("minCharges", [
        r"min(?:imum)?\s*(?:chargeable\s*)?(?:basic\s*)?freight[^\d]*(?:sfc[^\d]*)?(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
        r"min(?:imum)?\s*(?:freight|charge)\s*(?:\(docket\))?[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
        r"sfc\s*(?:rs\.?\s*|-)?\s*(\d+(?:\.\d+)?)",
    ], "scalar"),

    # Minimum weight
    ("minWeight", [
        r"min(?:imum)?\s*(?:chargeable\s*)?weight\s*(?:\(docket\))?[^\d]*(?:sfc[^\d]*)?(\d+(?:\.\d+)?)\s*kg",
        r"sfc\s*[^\d]*?(\d+(?:\.\d+)?)\s*kg",
        r"min\s*wt[^\d]*(\d+(?:\.\d+)?)\s*kg",
    ], "scalar"),

    # ROV / FOV owners risk
    ("rovCharges", [
        r"(?:rov|fov|freight\s*on\s*value)\s*(?:owners?\s*risk)?[^\d]*(\d+(?:\.\d+)?)\s*%\s*min\s*(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
        r"(?:rov|fov)[^\d]*(\d+(?:\.\d+)?)\s*%.*?(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
    ], "vf"),

    # ODA charges
    ("odaCharges", [
        r"(?:oda|out\s*delivery\s*area)[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)\s*(?:per\s*(?:shipment|consignment|docket))?",
    ], "scalar"),

    # Green tax
    ("greenTax", [
        r"green\s*(?:tax|cess|surcharge)[^\d]*(\d+(?:\.\d+)?)\s*%",
    ], "pct"),

    # DACC
    ("daccCharges", [
        r"(?:dacc|delivery\s*against)[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
    ], "scalar"),

    # COD
    ("codCharges", [
        r"(?:cod|cash\s*on\s*delivery)[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
    ], "scalar"),

    # Handling
    ("handlingCharges", [
        r"handling\s*(?:charge(?:s)?)?[^\d]*(?:rs\.?\s*)?(\d+(?:\.\d+)?)\s*(?:per\s*kg)?",
    ], "scalar"),
]

# ─── Company info patterns ────────────────────────────────────────────────────
_COMPANY_REGEXES = {
    "gstNo":        r'\b(\d{2}[A-Z]{5}\d{4}[A-Z]\d[ZY]\d)\b',
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
            origin_zone = inferred[0] if inferred else "E1"

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
                        if 1.0 <= rate <= 200.0:
                            zone_rate_samples[dest_zone].append(rate)
            except (ValueError, TypeError):
                continue

        if not zone_rate_samples and not served_pincodes:
            return None

        if not zone_rate_samples:
            print(f"[OICR] Station-rate: only pincodes ({len(served_pincodes)}) — no rates")
            return {"served_pincodes": list(set(served_pincodes))}

        # Average rates per destination zone
        origin_rates: Dict[str, float] = {
            dz: round(sum(rates) / len(rates), 2)
            for dz, rates in zone_rate_samples.items()
        }

        print(f"[OICR] Station-rate: {origin_zone}->{len(origin_rates)} zones: "
              f"{dict(sorted(origin_rates.items()))}")

        # Build full matrix via extrapolation
        full_matrix = self._extrapolate_zone_matrix(origin_zone, origin_rates)
        print(f"[OICR] Full matrix extrapolated: {len(full_matrix)} origin zones")

        return {
            "zone_matrix":      full_matrix,
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

        # Average rates per destination zone (from origin_zone)
        origin_rates: Dict[str, float] = {}
        for dest_zone, rates in zone_rate_samples.items():
            origin_rates[dest_zone] = round(sum(rates) / len(rates), 2)

        print(f"[OICR] Station-rate: {origin_zone} -> {len(origin_rates)} zones: "
              f"{sorted(origin_rates.items())}")

        # Extrapolate full matrix from single origin using distance ratios
        full_matrix = self._extrapolate_zone_matrix(origin_zone, origin_rates)
        print(f"[OICR] Extrapolated full matrix: {len(full_matrix)} origin zones")

        return {
            "zone_matrix": full_matrix,
            "served_pincodes": list(set(served_pincodes)),
        }

    # ─── 2. City-based zone matrix detection ──────────────────────────────────

    def detect_city_zone_matrix(self, table_rows: List[List[str]],
                                 context: str = "") -> Optional[Dict]:
        """
        Parse a city-based zone matrix (columns = city names, rows = origin cities).
        Returns a FC4 zone matrix dict or None.

        Example:
          FROM TO | DEL/NCR | BANGALORE | CHENNAI | MUMBAI
          USER    |   7     |    11     |   11    |   9
          KOLKATA |   12    |    11     |   11    |   9
        """
        if not table_rows or len(table_rows) < 2:
            return None

        # Find header row (row with multiple city/zone names)
        header_row_idx = None
        dest_zones: Dict[int, List[str]] = {}  # col_idx -> [zone codes]

        for i, row in enumerate(table_rows):
            cells = [str(c).strip().upper() for c in row]
            zone_hits = 0
            col_zones: Dict[int, List[str]] = {}
            for j, cell in enumerate(cells):
                zones = city_to_zones(cell)
                if not zones and cell in [z for z in ALL_ZONES]:
                    zones = [cell]
                if zones:
                    zone_hits += 1
                    col_zones[j] = zones
            if zone_hits >= 3:
                header_row_idx = i
                dest_zones = col_zones
                break

        if header_row_idx is None or not dest_zones:
            return None

        # Identify from-column (first column)
        from_col = min(dest_zones.keys()) - 1 if dest_zones else 0
        from_col = max(0, from_col)

        # Parse data rows
        zone_rates: Dict[str, Dict[str, float]] = {}

        for i, row in enumerate(table_rows):
            if i <= header_row_idx:
                continue
            cells = [str(c).strip() for c in row]
            if len(cells) <= max(dest_zones.keys()):
                continue

            # Determine from zone
            from_cell = cells[from_col].upper().strip()
            from_zones = city_to_zones(from_cell)
            if not from_zones:
                # Might be "USER" or blank for single-origin
                # Use context or default
                if "user" in from_cell.lower() or from_cell in ("", "-"):
                    from_zones = city_to_zones(context.upper()) or ["E1"]
                else:
                    continue

            # Extract rates for each destination zone
            for col_idx, d_zones in dest_zones.items():
                if col_idx >= len(cells):
                    continue
                try:
                    rate_str = cells[col_idx].replace(",", "").strip()
                    if not rate_str or rate_str.upper() in ("N/A", "-", "NA", ""):
                        continue
                    rate = float(rate_str)
                    if not (1.0 <= rate <= 500.0):
                        continue
                except (ValueError, TypeError):
                    continue

                for fz in from_zones:
                    if fz not in zone_rates:
                        zone_rates[fz] = {}
                    for dz in d_zones:
                        # Average if multiple rows map to same pair
                        if dz in zone_rates[fz]:
                            zone_rates[fz][dz] = (zone_rates[fz][dz] + rate) / 2
                        else:
                            zone_rates[fz][dz] = rate

        if not zone_rates:
            return None

        # If only 1 origin zone, extrapolate
        if len(zone_rates) == 1:
            orig = list(zone_rates.keys())[0]
            zone_rates = self._extrapolate_zone_matrix(orig, zone_rates[orig])

        print(f"[OICR] City zone matrix: {len(zone_rates)} origins extracted")
        return zone_rates

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
                    m = re.search(pat, text_lower, re.IGNORECASE | re.DOTALL)
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

        Uses the principle: rate(A->B) ≈ rate(origin->B) * dist(origin->A) / dist(origin->A_ref)
        where A_ref is the zone closest to A for which we have data.

        This is conservative: we only interpolate, never extrapolate beyond data range.
        """
        if not origin_rates or origin_zone not in ZONE_COORDS:
            return {origin_zone: origin_rates} if origin_rates else {}

        full = {}

        # Base rates from origin to all zones (fill gaps via distance)
        origin_to_all = self._fill_zone_gaps(origin_zone, origin_rates)

        # Build rates for each other origin zone
        for from_z in ALL_ZONES:
            if from_z == origin_zone:
                full[from_z] = dict(origin_to_all)
                continue

            if from_z not in ZONE_COORDS:
                continue

            # Distance ratio: from_z relative to known origin
            dist_orig_from = _haversine(ZONE_COORDS[origin_zone], ZONE_COORDS[from_z])

            from_z_rates: Dict[str, float] = {}
            for to_z, base_rate in origin_to_all.items():
                if to_z not in ZONE_COORDS:
                    continue

                dist_orig_to = _haversine(ZONE_COORDS[origin_zone], ZONE_COORDS[to_z])
                dist_from_to = _haversine(ZONE_COORDS[from_z], ZONE_COORDS[to_z])

                if dist_orig_to < 1:
                    # Same zone -> same as local rate
                    from_z_rates[to_z] = round(base_rate, 2)
                else:
                    # Scale by distance ratio
                    ratio = dist_from_to / dist_orig_to
                    # Dampened ratio: real rates don't scale linearly with distance
                    dampened = 0.6 + 0.4 * ratio  # min 60% of origin rate
                    from_z_rates[to_z] = round(base_rate * dampened, 2)

            full[from_z] = from_z_rates

        return full

    def _fill_zone_gaps(
        self, origin_zone: str, origin_rates: Dict[str, float]
    ) -> Dict[str, float]:
        """
        Fill in missing destination zones using distance interpolation.
        """
        filled = dict(origin_rates)

        # Find min/max rate in known data for clamping
        rates_list = [r for r in filled.values() if r > 0]
        if not rates_list:
            return filled
        min_r, max_r = min(rates_list), max(rates_list)

        # For missing zones, interpolate from nearest known zone
        for to_z in ALL_ZONES:
            if to_z in filled or to_z not in ZONE_COORDS:
                continue
            if origin_zone not in ZONE_COORDS:
                filled[to_z] = sum(rates_list) / len(rates_list)
                continue

            dist_to_missing = _haversine(ZONE_COORDS[origin_zone], ZONE_COORDS[to_z])

            # Find nearest known zone (by distance from origin)
            best_ref = None
            best_ref_dist = float("inf")
            for known_z, known_rate in filled.items():
                if known_z not in ZONE_COORDS:
                    continue
                d = abs(_haversine(ZONE_COORDS[origin_zone], ZONE_COORDS[known_z]) - dist_to_missing)
                if d < best_ref_dist:
                    best_ref_dist = d
                    best_ref = (known_z, known_rate)

            if best_ref:
                # Interpolate: scale the closest known rate by distance ratio
                ref_zone, ref_rate = best_ref
                ref_dist = _haversine(ZONE_COORDS[origin_zone], ZONE_COORDS[ref_zone])
                if ref_dist > 0:
                    ratio = dist_to_missing / ref_dist
                    interp = ref_rate * (0.7 + 0.3 * ratio)
                    filled[to_z] = round(max(min_r, min(max_r * 1.5, interp)), 2)
                else:
                    filled[to_z] = ref_rate

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

        # Charges from text
        text_charges = self.extract_charges_from_text(text)
        if text_charges:
            result["charges"].update(text_charges)

        # Zone matrix + charges from tables
        all_table_rows: List[List[str]] = []
        for table in (tables or []):
            if not table:
                continue
            flat_rows = []
            for row in table:
                if isinstance(row, list):
                    flat_rows.append([str(c) for c in row])
            all_table_rows.extend(flat_rows)

            # Try city-based zone matrix
            if not result["zone_matrix"]:
                zm = self.detect_city_zone_matrix(flat_rows)
                if zm:
                    result["zone_matrix"] = zm
                    print(f"[OICR] PDF zone matrix: {len(zm)} origins")

            # Charge table extraction
            table_charges = self.extract_charges_from_table(flat_rows)
            for k, v in table_charges.items():
                result["charges"].setdefault(k, v)

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
