"""
UTSF Schema - Unified Transporter Save Format v2.1
====================================================
The locked canonical data model for all transporter data in FC3.

LOCKED FORMAT: This is the definitive spec. Do not change field names.
All generators MUST produce this format. All consumers MUST read it.

Top-level structure:
  version          "2.1"
  generatedAt      ISO-8601
  sourceFormat     "excel"|"pdf"|"word"|"image"|"manual"|"migration"
  meta             Company identity block
  pricing          {priceRate, zoneRates}
  serviceability   {zone: coverage}
  oda              {zone: odaInfo}  (separate ODA pincode block)
  stats            Aggregate coverage stats

Coverage modes (serviceability.{zone}.mode):
  FULL_ZONE        All pincodes in canonical zone are served
  FULL_MINUS_EXCEPT  All EXCEPT listed → stored in exceptRanges/exceptSingles
  ONLY_SERVED      Only listed pincodes → stored in servedRanges/servedSingles
  NOT_SERVED       Zone not served at all

Charge format:
  Scalars (fuel%, docketCharges, etc.): plain number
  Variable+fixed (rovCharges, odaCharges, etc.): {v: %, f: fixed}
  ODA complex types: {type: "...", ...} — see odaCharges spec below

odaCharges types:
  {v, f}                        legacy additive: f + weight * v/100
  {type:"per_kg_minimum", v, f} max(v/100 * weight, f)  [most common B2B]
  {type:"per_shipment", f}      flat per ODA shipment
  {type:"weight_band", bands}   step-function by weight
  {type:"distance_weight_matrix", matrix}  distance × weight table

Range format: {"s": startPincode, "e": endPincode}
"""

from typing import List, Dict, Any
from datetime import datetime


# ─── Canonical zones ──────────────────────────────────────────────────────────

ALL_ZONES = [
    "N1", "N2", "N3", "N4",
    "S1", "S2", "S3", "S4",
    "E1", "E2",
    "W1", "W2",
    "C1", "C2",
    "NE1", "NE2",
    "X1", "X2", "X3",   # Special: A&N Islands, Lakshadweep, Leh/Ladakh/J&K
]

REGIONS = {
    "North":      ["N1", "N2", "N3", "N4"],
    "South":      ["S1", "S2", "S3", "S4"],
    "East":       ["E1", "E2"],
    "West":       ["W1", "W2"],
    "Central":    ["C1", "C2"],
    "North East": ["NE1", "NE2"],
    "Special":    ["X1", "X2", "X3"],
}

ZONE_TO_REGION = {z: r for r, zones in REGIONS.items() for z in zones}


# ─── Mode name normalisation ───────────────────────────────────────────────────
# All incoming mode strings → canonical v2.1 names used by consumers.

# Canonical output mode names (what we WRITE)
MODE_FULL_ZONE          = "FULL_ZONE"
MODE_FULL_MINUS_EXCEPT  = "FULL_MINUS_EXCEPT"
MODE_ONLY_SERVED        = "ONLY_SERVED"
MODE_NOT_SERVED         = "NOT_SERVED"

# Normalize any legacy or FC4 mode name → canonical v2.1 name
MODE_NORMALIZE: Dict[str, str] = {
    "FULL_ZONE":             MODE_FULL_ZONE,
    "FULL_MINUS_EXCEPT":     MODE_FULL_MINUS_EXCEPT,
    "FULL_MINUS_EXCEPTIONS": MODE_FULL_MINUS_EXCEPT,   # FC4 variant
    "EXCLUDING":             MODE_FULL_MINUS_EXCEPT,   # FC4 canonical
    "ONLY_SERVED":           MODE_ONLY_SERVED,
    "INCLUDING":             MODE_ONLY_SERVED,          # FC4 canonical
    "NOT_SERVED":            MODE_NOT_SERVED,
}

# For code that still reads FC4-style names from stored data
FC4_MODE_NORMALIZE = MODE_NORMALIZE   # alias


# ─── Pincode range helpers ─────────────────────────────────────────────────────

def compress_to_ranges(pincodes: List[int], min_range_size: int = 3) -> Dict:
    """
    Compress a sorted list of pincodes into {s,e} ranges + singles.
    Returns: {"ranges": [{"s": start, "e": end}], "singles": [int]}
    """
    if not pincodes:
        return {"ranges": [], "singles": []}

    sorted_pins = sorted(set(pincodes))
    ranges: List[Dict] = []
    singles: List[int] = []

    i = 0
    while i < len(sorted_pins):
        j = i
        while j + 1 < len(sorted_pins) and sorted_pins[j + 1] == sorted_pins[j] + 1:
            j += 1
        run_len = j - i + 1
        if run_len >= min_range_size:
            ranges.append({"s": sorted_pins[i], "e": sorted_pins[j]})
        else:
            singles.extend(sorted_pins[i:j+1])
        i = j + 1

    return {"ranges": ranges, "singles": singles}


def expand_ranges(ranges: List[Dict], singles: List[int]) -> List[int]:
    """Expand compressed {s,e} ranges + singles back to full pincode list."""
    result = list(singles) if singles else []
    for r in (ranges or []):
        if isinstance(r, (list, tuple)) and len(r) == 2:
            result.extend(range(int(r[0]), int(r[1]) + 1))
        elif isinstance(r, dict):
            result.extend(range(int(r["s"]), int(r["e"]) + 1))
    return result


def determine_coverage_mode(
    served_pincodes: set,
    zone_pincodes: set,
    threshold: float = 0.5
) -> str:
    """
    Choose the most compact coverage mode based on coverage ratio.
    Returns canonical v2.1 mode name.
    """
    if not zone_pincodes or not served_pincodes:
        return MODE_NOT_SERVED

    overlap = served_pincodes & zone_pincodes
    if not overlap:
        return MODE_NOT_SERVED

    coverage = len(overlap) / len(zone_pincodes)

    if coverage >= 0.999:
        return MODE_FULL_ZONE
    elif coverage >= threshold:
        return MODE_FULL_MINUS_EXCEPT   # store exceptions (smaller list)
    else:
        return MODE_ONLY_SERVED          # store served (smaller list)


# ─── Empty entry builders ──────────────────────────────────────────────────────

def empty_zone_entry() -> Dict:
    """Return an empty v2.1 zone serviceability entry."""
    return {
        "mode": MODE_NOT_SERVED,
        # FULL_MINUS_EXCEPT: pincodes NOT served from canonical zone
        "exceptRanges": [],
        "exceptSingles": [],
        # ONLY_SERVED: pincodes that ARE served from canonical zone
        "servedRanges": [],
        "servedSingles": [],
        # FC4 cross-zone: pincodes from OTHER canonical zones priced as THIS zone
        "crossZoneRanges": [],
        "crossZoneSingles": [],
        # ODA pincodes (inline — also in separate oda block)
        "odaRanges": [],
        "odaSingles": [],
        # Stats
        "totalInZone": 0,
        "servedCount": 0,
        "odaCount": 0,
        "coveragePercent": 0.0,
    }


def empty_priceRate() -> Dict:
    """Return the locked priceRate structure with all charge fields."""
    return {
        # Base
        "minWeight":        0.5,    # Minimum chargeable weight (kg)
        "minCharges":       0.0,    # Floor for baseFreight (NOT additive)
        "divisor":          5000,   # Volumetric divisor (legacy alias)
        "kFactor":          5000,   # Volumetric kFactor (primary)
        # Fixed-per-shipment
        "docketCharges":    0.0,
        "greenTax":         0.0,
        "daccCharges":      0.0,
        "miscCharges":      0.0,
        "dodCharges":       0.0,    # Delivery on Demand flat fee
        # Percentage of freight
        "fuel":             0.0,    # Fuel surcharge %
        # Variable+fixed charges {v: %, f: fixed}
        "rovCharges":       {"v": 0.0, "f": 0.0},
        "insuranceCharges": {"v": 0.0, "f": 0.0},
        "odaCharges":       {"v": 0.0, "f": 0.0},
        "handlingCharges":  {"v": 0.0, "f": 0.0},
        "fmCharges":        {"v": 0.0, "f": 0.0},
        "appointmentCharges": {"v": 0.0, "f": 0.0},
        "codCharges":       {"v": 0.0, "f": 0.0},
        "prepaidCharges":   {"v": 0.0, "f": 0.0},
        "topayCharges":     {"v": 0.0, "f": 0.0},
        # Invoice value surcharge (null = disabled)
        "invoiceValueCharges": None,
    }


# ─── v2.1 Empty Template ───────────────────────────────────────────────────────

UTSF_EMPTY_TEMPLATE = {
    "version": "2.1",
    "generatedAt": None,
    "sourceFormat": "excel",
    "generatedBy": "utsf-generator",
    "sourceFiles": [],
    "dataQuality": 0.0,

    "meta": {
        "id": None,
        "companyName": None,
        "shortName": None,
        "vendorCode": None,
        "customerID": None,
        "transporterType": "regular",    # regular|temporary
        "transportMode": "LTL",          # LTL|FTL|B2C|surface|air
        "gstNo": None,
        "panNo": None,
        "website": None,
        "address": None,
        "state": None,
        "city": None,
        "pincode": "",
        "contactPhone": None,
        "contactEmail": None,
        "rating": 4.0,
        "isVerified": False,
        "chargesVerified": False,
        "approvalStatus": "pending",
        "createdAt": None,
        "updatedAt": None,
    },

    "pricing": {
        "effectiveFrom": None,
        "effectiveTo": None,
        "rateVersion": "1.0",
        "currency": "INR",
        "priceRate": None,    # filled by empty_priceRate()
        "zoneRates": {},      # {originZone: {destZone: ratePerKg}}
    },

    "serviceability": {},   # {zone: zoneCoverageEntry}

    "oda": {},              # {zone: {odaCount, odaRanges, odaSingles}}

    "stats": {
        "totalPincodes": 0,
        "totalOdaPincodes": 0,
        "zonesServed": 0,
        "activeZones": [],
        "avgCoveragePercent": 0.0,
        "coverageByRegion": {},
        "dataQuality": {
            "overall": 0.0,
            "missingFields": [],
        },
    },
}

# Backward-compat alias (code still referencing FC4_EMPTY_TEMPLATE will get v2.1)
FC4_EMPTY_TEMPLATE = UTSF_EMPTY_TEMPLATE


# ─── Data Quality Scoring ──────────────────────────────────────────────────────

def calculate_data_quality(utsf: Dict) -> float:
    """
    Score the data quality of a v2.1 UTSF file (0-100).

    Breakdown:
      meta fields        : 30 pts
      zoneRates          : 30 pts
      serviceability     : 25 pts
      pricing completeness: 15 pts
    """
    score = 0.0

    meta = utsf.get("meta", {})
    pricing = utsf.get("pricing", {})
    svc = utsf.get("serviceability", {})
    pr = pricing.get("priceRate") or {}

    # ── Meta (30 pts) ────────────────────────────────────────────────────
    meta_fields = {
        "companyName": 8,
        "transportMode": 4,
        "gstNo": 5,
        "contactPhone": 3,
        "contactEmail": 3,
        "rating": 3,
        "isVerified": 2,
        "address": 2,
    }
    for field, pts in meta_fields.items():
        val = meta.get(field)
        if val is not None and val != "" and val != 0.0 and val is not False:
            score += pts

    # ── Zone rates (30 pts) ──────────────────────────────────────────────
    zr = pricing.get("zoneRates", {})
    if zr:
        score += min(30, len(zr) * 2)

    # ── Serviceability (25 pts) ──────────────────────────────────────────
    active_zones = [
        z for z, d in svc.items()
        if d.get("mode", MODE_NOT_SERVED) != MODE_NOT_SERVED
    ]
    if active_zones:
        score += min(25, len(active_zones) * 2)

    # ── Pricing completeness (15 pts) ────────────────────────────────────
    if pr.get("docketCharges", 0) > 0:                              score += 3
    if pr.get("fuel", 0) > 0:                                       score += 3
    _rov = pr.get("rovCharges", {})
    if isinstance(_rov, dict) and (_rov.get("v", 0) or _rov.get("f", 0)): score += 3
    _oda = pr.get("odaCharges", {})
    oda_present = (
        isinstance(_oda, dict) and (
            _oda.get("v", 0) or _oda.get("f", 0) or
            _oda.get("bands") or _oda.get("matrix") or
            _oda.get("type") == "per_shipment"
        )
    )
    if oda_present:                                                  score += 3
    if pr.get("minCharges", 0) > 0:                                 score += 3

    return round(min(score, 100.0), 1)


# ─── Validation ───────────────────────────────────────────────────────────────

def validate_utsf(utsf: Dict) -> List[str]:
    """
    Validate v2.1 UTSF structure. Returns list of error strings (empty = valid).
    Accepts both v2.x and FC4 formats for migration compatibility.
    """
    errors: List[str] = []

    # Version / format check
    version = str(utsf.get("version", ""))
    fmt = utsf.get("format", "")
    if not version.startswith(("2.", "4.")) and fmt not in ("FC4",):
        errors.append(f"Unexpected version '{version}' — expected 2.x or FC4")

    # Meta / company (accept both v2 meta and FC4 company)
    meta = utsf.get("meta") or utsf.get("company") or {}
    name = meta.get("companyName") or meta.get("name")
    if not name:
        errors.append("meta.companyName (or company.name) is required")

    # Pricing
    pricing = utsf.get("pricing", {})
    zr = pricing.get("zoneRates") or pricing.get("zoneMatrix", {})
    if not zr:
        errors.append("pricing.zoneRates is empty — no zone rates defined")
    else:
        for orig, dests in zr.items():
            if orig not in ALL_ZONES:
                errors.append(f"zoneRates: unknown origin zone '{orig}'")
            if not isinstance(dests, dict):
                errors.append(f"zoneRates[{orig}] is not a dict")
                continue
            for dest, rate in dests.items():
                if dest not in ALL_ZONES:
                    errors.append(f"zoneRates[{orig}][{dest}]: unknown dest zone")
                if not isinstance(rate, (int, float)) or rate < 0:
                    errors.append(f"zoneRates[{orig}][{dest}]: invalid rate {rate!r}")

    # Serviceability
    svc = utsf.get("serviceability", {})
    for zone, data in svc.items():
        if zone not in ALL_ZONES:
            errors.append(f"serviceability: unknown zone '{zone}'")
            continue
        raw_mode = data.get("mode", MODE_NOT_SERVED)
        mode = MODE_NORMALIZE.get(raw_mode, raw_mode)
        if mode not in (MODE_FULL_ZONE, MODE_FULL_MINUS_EXCEPT, MODE_ONLY_SERVED, MODE_NOT_SERVED):
            errors.append(f"serviceability.{zone}.mode: invalid '{raw_mode}'")

    return errors


# Backward-compat alias
def validate_fc4(utsf: Dict) -> List[str]:
    return validate_utsf(utsf)
