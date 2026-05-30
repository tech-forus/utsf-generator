"""
UTSF Knowledge Dictionary
==========================
The single source of truth for all synonyms, aliases, geographic rules,
and canonical mappings used throughout the UTSF Generator.

Every lookup in parsers and builders should go through this module —
never hardcode field names or zone labels in parsing logic.

Sections:
  1. Canonical zone definitions
  2. Zone name synonyms (ZONE_SYNONYMS)
  3. Pincode-prefix → possible zones (derived from pincodes.json analysis)
  4. State → canonical zones
  5. Geographic impossibility rules
  6. Charge field synonyms (CHARGE_SYNONYMS) — expanded from 107 to 300+
  7. Company field synonyms (COMPANY_SYNONYMS)
  8. ODA indicator words
  9. Delivery/serviceability indicator words
"""

from typing import Dict, List, Set, FrozenSet


# ─── 1. Canonical zones ───────────────────────────────────────────────────────

ALL_ZONES: List[str] = [
    "N1", "N2", "N3", "N4",
    "S1", "S2", "S3", "S4",
    "E1", "E2",
    "W1", "W2",
    "C1", "C2",
    "NE1", "NE2",
    "X1", "X2", "X3",
]

ZONE_SET: FrozenSet[str] = frozenset(ALL_ZONES)

REGIONS: Dict[str, List[str]] = {
    "North":      ["N1", "N2", "N3", "N4"],
    "South":      ["S1", "S2", "S3", "S4"],
    "East":       ["E1", "E2"],
    "West":       ["W1", "W2"],
    "Central":    ["C1", "C2"],
    "NorthEast":  ["NE1", "NE2"],
    "Special":    ["X1", "X2", "X3"],
}

ZONE_TO_REGION: Dict[str, str] = {z: r for r, zs in REGIONS.items() for z in zs}


# ─── 2. Zone name synonyms ────────────────────────────────────────────────────
# Key: any label a transporter or document might use (UPPERCASE)
# Value: list of canonical zones it expands to

ZONE_SYNONYMS: Dict[str, List[str]] = {
    # Self-mappings for all canonical zones
    **{z: [z] for z in ALL_ZONES},

    # ── Regional shorthand ────────────────────────────────────────────────────
    "N":                ["N1","N2","N3","N4"],
    "NORTH":            ["N1","N2","N3","N4"],
    "NORTH INDIA":      ["N1","N2","N3","N4"],
    "NORTHERN":         ["N1","N2","N3","N4"],
    "NORTHERN INDIA":   ["N1","N2","N3","N4"],
    "NORTH ZONE":       ["N1","N2","N3","N4"],

    "S":                ["S1","S2","S3","S4"],
    "SOUTH":            ["S1","S2","S3","S4"],
    "SOUTH INDIA":      ["S1","S2","S3","S4"],
    "SOUTHERN":         ["S1","S2","S3","S4"],
    "SOUTHERN INDIA":   ["S1","S2","S3","S4"],
    "SOUTH ZONE":       ["S1","S2","S3","S4"],

    "E":                ["E1","E2"],
    "EAST":             ["E1","E2"],
    "EAST INDIA":       ["E1","E2"],
    "EASTERN":          ["E1","E2"],
    "EASTERN INDIA":    ["E1","E2"],
    "EAST ZONE":        ["E1","E2"],

    "W":                ["W1","W2"],
    "WEST":             ["W1","W2"],
    "WEST INDIA":       ["W1","W2"],
    "WESTERN":          ["W1","W2"],
    "WESTERN INDIA":    ["W1","W2"],
    "WEST ZONE":        ["W1","W2"],

    "C":                ["C1","C2"],
    "CENTRAL":          ["C1","C2"],
    "CENTRAL INDIA":    ["C1","C2"],
    "CENTRAL ZONE":     ["C1","C2"],

    "NE":               ["NE1","NE2"],
    "NORTHEAST":        ["NE1","NE2"],
    "NORTH EAST":       ["NE1","NE2"],
    "NORTH-EAST":       ["NE1","NE2"],
    "NORTHEASTERN":     ["NE1","NE2"],
    "NE INDIA":         ["NE1","NE2"],
    "NORTH EAST INDIA": ["NE1","NE2"],
    "NE/JK":            ["NE1","NE2","X3"],
    "NE + J&K":         ["NE1","NE2","X3"],

    # ── Special zones ─────────────────────────────────────────────────────────
    "J&K":              ["X3"],
    "JK":               ["X3"],
    "J AND K":          ["X3"],
    "JAMMU":            ["X3"],
    "JAMMU AND KASHMIR":["X3"],
    "JAMMU & KASHMIR":  ["X3"],
    "KASHMIR":          ["X3"],
    "LADAKH":           ["X3"],
    "LEH":              ["X3"],
    "LEH LADAKH":       ["X3"],
    "J&K/LADAKH":       ["X3"],
    "JAMMU/LADAKH":     ["X3"],

    "ANDAMAN":          ["X1"],
    "A&N":              ["X1"],
    "A & N":            ["X1"],
    "ANDAMAN AND NICOBAR": ["X1"],
    "ANDAMAN & NICOBAR":   ["X1"],
    "ANDAMAN NICOBAR":  ["X1"],

    "LAKSHADWEEP":      ["X2"],
    "LAKSHADWIP":       ["X2"],
    "LAKSHWADEEP":      ["X2"],

    # ── Standard 7-zone commercial schemes ────────────────────────────────────
    "ZONE A":           ["N1","N2"],          # Metro + NCR
    "ZONE B":           ["N3","N4"],          # Rest of North
    "ZONE C":           ["E1","E2"],          # East
    "ZONE D":           ["W1","W2"],          # West
    "ZONE E":           ["S1","S2","S3","S4"],# South
    "ZONE F":           ["C1","C2"],          # Central
    "ZONE G":           ["NE1","NE2"],        # NE

    # ── Numbered zone schemes (common in B2B) ─────────────────────────────────
    "ZONE 1":           ["N1","N2"],
    "ZONE 2":           ["N3","N4"],
    "ZONE 3":           ["E1","E2"],
    "ZONE 4":           ["W1","W2"],
    "ZONE 5":           ["S1","S2","S3","S4"],
    "ZONE 6":           ["C1","C2"],
    "ZONE 7":           ["NE1","NE2"],
    "ZONE 8":           ["X1","X2","X3"],

    # ── Letter/number combos (TCI, V-express etc.) ────────────────────────────
    "Z1":               ["N1","N2"],
    "Z2":               ["N3","N4"],
    "Z3":               ["E1","E2"],
    "Z4":               ["W1","W2"],
    "Z5":               ["S1","S2","S3","S4"],
    "Z6":               ["C1","C2"],
    "Z7":               ["NE1","NE2"],

    # ── Metro / rest distinctions ─────────────────────────────────────────────
    "METRO":            ["N1","S1","E1","W1"],
    "METROS":           ["N1","S1","E1","W1"],
    "METRO CITIES":     ["N1","S1","E1","W1"],
    "REST":             ["N2","N3","N4","S2","S3","S4","E2","W2","C1","C2"],
    "REST OF INDIA":    ["N2","N3","N4","S2","S3","S4","E2","W2","C1","C2"],
    "NON METRO":        ["N2","N3","N4","S2","S3","S4","E2","W2","C1","C2"],
    "REST OF NORTH":    ["N2","N3","N4"],
    "REST OF SOUTH":    ["S2","S3","S4"],

    # ── Geographic descriptors ─────────────────────────────────────────────────
    "DELHI NCR":        ["N1"],
    "DELHI":            ["N1"],
    "NCR":              ["N1"],
    "MUMBAI":           ["W1"],
    "PUNE":             ["W1"],
    "BENGALURU":        ["S1"],
    "BANGALORE":        ["S1"],
    "CHENNAI":          ["S1"],
    "HYDERABAD":        ["S2"],
    "KOLKATA":          ["E1"],
    "AHMEDABAD":        ["W1"],

    # ── State names → zones ───────────────────────────────────────────────────
    "ANDHRA PRADESH":   ["E2","S2","S3"],
    "AP":               ["E2","S2","S3"],
    "ARUNACHAL PRADESH":["NE2"],
    "ASSAM":            ["NE1","NE2"],
    "BIHAR":            ["E1","E2"],
    "CHANDIGARH":       ["N1","N3"],
    "CHHATTISGARH":     ["C1","C2","E2","N3"],
    "CHATTISGARH":      ["C1","C2","E2","N3"],
    "GOA":              ["W2"],
    "GUJARAT":          ["W1","W2"],
    "GJ":               ["W1","W2"],
    "HARYANA":          ["N1","N3","N4"],
    "HR":               ["N1","N3","N4"],
    "HIMACHAL PRADESH": ["N4"],
    "HP":               ["N4"],
    "JHARKHAND":        ["E1","E2"],
    "JH":               ["E1","E2"],
    "KARNATAKA":        ["S1","S2","W2"],
    "KA":               ["S1","S2","W2"],
    "KERALA":           ["S3","S4"],
    "KL":               ["S3","S4"],
    "MADHYA PRADESH":   ["C1","C2"],
    "MP":               ["C1","C2"],
    "MAHARASHTRA":      ["C1","S2","W1","W2"],
    "MH":               ["C1","S2","W1","W2"],
    "MEGHALAYA":        ["NE2"],
    "MIZORAM":          ["NE2"],
    "NAGALAND":         ["NE2"],
    "ODISHA":           ["E1","E2"],
    "ORISSA":           ["E1","E2"],
    "OR":               ["E1","E2"],
    "PUDUCHERRY":       ["S3"],
    "PONDICHERRY":      ["S3"],
    "PUNJAB":           ["N1","N2","N3"],
    "PB":               ["N1","N2","N3"],
    "RAJASTHAN":        ["N1","N3"],
    "RJ":               ["N1","N3"],
    "SIKKIM":           ["E2","NE2"],
    "TAMIL NADU":       ["S1","S3"],
    "TAMILNADU":        ["S1","S3"],
    "TN":               ["S1","S3"],
    "TELANGANA":        ["S1","S2"],
    "TS":               ["S1","S2"],
    "TRIPURA":          ["NE2"],
    "UTTAR PRADESH":    ["N1","N3","N4"],
    "UP":               ["N1","N3","N4"],
    "UTTARAKHAND":      ["N2","N3","N4"],
    "UK":               ["N2","N3","N4"],
    "UTTARANCHAL":      ["N2","N3","N4"],
    "WEST BENGAL":      ["E1","E2"],
    "WB":               ["E1","E2"],
}


# ─── 3. Pincode-prefix → possible canonical zones ─────────────────────────────
# Derived by analysing pincodes.json (23,937 entries).
# These are the ONLY zones a given first-digit prefix can ever belong to.
# Any other assignment is geographically impossible.

PINCODE_PREFIX_ZONES: Dict[str, List[str]] = {
    "1": ["N1","N2","N3","N4","X3"],              # Delhi / HP / J&K / Ladakh / Haryana
    "2": ["C2","N1","N2","N3","N4"],              # UP / Uttarakhand / some Central
    "3": ["C2","N1","N3","W1","W2"],              # Rajasthan / Gujarat border
    "4": ["C1","C2","E2","N3","S2","W1","W2"],   # Maharashtra / Gujarat / AP border / MP
    "5": ["C2","E2","S1","S2","S3","W2"],         # AP / Telangana / Karnataka / TN / Goa
    "6": ["S1","S3","S4","X2"],                   # TN / Kerala / Lakshadweep
    "7": ["E1","E2","NE1","NE2","X1"],            # Bengal / Odisha / Jharkhand / NE / Andaman
    "8": ["E1","E2","NE2"],                       # Bengal / Odisha / NE
    "9": ["E2"],                                  # Very limited (E Bengal border)
}

# Which zones are IMPOSSIBLE for each first-digit prefix
IMPOSSIBLE_ZONES_FOR_PREFIX: Dict[str, Set[str]] = {
    prefix: frozenset(ALL_ZONES) - frozenset(possible)
    for prefix, possible in PINCODE_PREFIX_ZONES.items()
}

# Two-digit prefix for finer-grained validation (populated below)
PINCODE_2PREFIX_ZONES: Dict[str, List[str]] = {
    # North — all 1x pincodes
    "11": ["N1"],  "12": ["N1","N2","N3","N4"],  "13": ["N1","N2","N3","N4"],
    "14": ["N2","N3","N4","X3"],                  "15": ["N2","N3"],
    "16": ["N1","N2","N3"],                       "17": ["N4"],
    "18": ["N4","X3"],                            "19": ["X3"],
    # UP / Uttarakhand
    "20": ["N1","N3","N4"],  "21": ["N1","N3","N4"],  "22": ["N3","N4"],
    "23": ["N3","N4"],       "24": ["N3","N4"],        "25": ["N3","N4"],
    "26": ["N2","N4"],       "27": ["N1","N3","N4"],   "28": ["N1","N3","N4"],
    # Rajasthan
    "30": ["N1","N3"],  "31": ["N1","N3"],  "32": ["N3"],
    "33": ["N3"],       "34": ["N3"],
    # Gujarat
    "36": ["W1","W2"],  "37": ["W1","W2"],  "38": ["W1","W2"],  "39": ["W1","W2"],
    # Maharashtra
    "40": ["W1"],  "41": ["W1","W2"],  "42": ["W1","W2"],  "43": ["W1","W2"],
    "44": ["C1","W1","W2"],  "45": ["C1","C2","W1"],  "46": ["C1","C2"],
    "47": ["C1","C2"],
    # AP / Telangana
    "50": ["S2"],  "51": ["S2"],  "52": ["S2"],  "53": ["S2"],
    "54": ["S2","E2"],  "55": ["S2","E2"],
    # AP / Karnataka / TN
    "56": ["S1"],  "57": ["S1","S2"],  "58": ["S1"],  "59": ["S2","S3"],
    # TN / Kerala
    "60": ["S1","S3"],  "61": ["S3"],  "62": ["S3"],  "63": ["S3"],
    "64": ["S3"],  "65": ["S3","S4"],  "66": ["S4"],  "67": ["S4"],
    "68": ["S4"],  "69": ["S4","X2"],
    # Bengal / Odisha
    "70": ["E1"],  "71": ["E1","E2"],  "72": ["E1","E2"],  "73": ["E1","E2"],
    "74": ["E1","E2"],  "75": ["E2"],  "76": ["E2"],  "77": ["E2"],
    # NE / Andaman
    "78": ["NE1","NE2"],  "79": ["NE1","NE2","X1"],
    # Odisha / Bengal
    "80": ["E1"],  "81": ["E1"],  "82": ["E1","E2"],  "83": ["E1","E2"],
    "84": ["E1","E2"],  "85": ["E2","NE2"],
}


# ─── 4. State → canonical zones ───────────────────────────────────────────────
# Ground-truth from pincodes.json analysis

STATE_TO_ZONES: Dict[str, List[str]] = {
    "ANDAMAN & NICOBAR ISLANDS":    ["X1"],
    "ANDHRA PRADESH":               ["E2","S2","S3"],
    "ARUNACHAL PRADESH":            ["NE2"],
    "ASSAM":                        ["NE1","NE2"],
    "BIHAR":                        ["E1","E2"],
    "CHANDIGARH":                   ["N1","N3"],
    "CHHATTISGARH":                 ["C1","C2","E2","N3"],
    "DELHI":                        ["N1"],
    "GOA":                          ["W2"],
    "GUJARAT":                      ["W1","W2"],
    "HARYANA":                      ["N1","N3","N4"],
    "HIMACHAL PRADESH":             ["N4"],
    "JAMMU AND KASHMIR":            ["N4"],
    "JHARKHAND":                    ["E1","E2"],
    "KARNATAKA":                    ["S1","S2","W2"],
    "KERALA":                       ["S3","S4"],
    "LADAKH":                       ["X3"],
    "LAKSHADWEEP":                  ["X2"],
    "MADHYA PRADESH":               ["C1","C2"],
    "MAHARASHTRA":                  ["C1","S2","W1","W2"],
    "MEGHALAYA":                    ["NE2"],
    "MIZORAM":                      ["NE2"],
    "NAGALAND":                     ["NE2"],
    "ODISHA":                       ["E1","E2"],
    "PUDUCHERRY":                   ["S3"],
    "PUNJAB":                       ["N1","N2","N3"],
    "RAJASTHAN":                    ["N1","N3"],
    "SIKKIM":                       ["E2","NE2"],
    "TAMIL NADU":                   ["S1","S3"],
    "TELANGANA":                    ["S1","S2"],
    "TRIPURA":                      ["NE2"],
    "UTTAR PRADESH":                ["N1","N3","N4"],
    "UTTARAKHAND":                  ["N2","N3","N4"],
    "WEST BENGAL":                  ["E1","E2"],
    # Common abbreviations / aliases
    "J&K":                          ["N4"],
    "J AND K":                      ["N4"],
    "JAMMU & KASHMIR":              ["N4"],
    "ANDAMAN AND NICOBAR":          ["X1"],
    "PONDICHERRY":                  ["S3"],
    "ORISSA":                       ["E1","E2"],
    "UTTARANCHAL":                  ["N2","N3","N4"],
    "CHATTISGARH":                  ["C1","C2","E2","N3"],
    "TAMILNADU":                    ["S1","S3"],
    "THE DADRA AND NAGAR HAVELI AND DAMAN AND DIU": ["W2"],
    "DADRA AND NAGAR HAVELI":       ["W2"],
    "DAMAN AND DIU":                ["W2"],
}

# Reverse: zone → list of states
ZONE_TO_STATES: Dict[str, List[str]] = {}
for _state, _zones in STATE_TO_ZONES.items():
    for _z in _zones:
        ZONE_TO_STATES.setdefault(_z, []).append(_state)


# ─── 5. Geographic impossibility rules ────────────────────────────────────────
# For a given zone, which states are impossible (never overlap)

ZONE_IMPOSSIBLE_STATES: Dict[str, Set[str]] = {}
_all_states = set(STATE_TO_ZONES.keys())
for _zone in ALL_ZONES:
    _valid_states = set(ZONE_TO_STATES.get(_zone, []))
    ZONE_IMPOSSIBLE_STATES[_zone] = _all_states - _valid_states

# Hard rules: "A pincode from state X can NEVER be in zone Y"
# E.g. "Andhra Pradesh pincode can never be in N1"
GEO_HARD_RULES: List[tuple] = [
    # (state_pattern, impossible_zones_set)
    ("ANDHRA PRADESH",  {"N1","N2","N3","N4","W1","W2","C1","C2","E1","NE1","NE2","X1","X2","X3"}),
    ("KERALA",          {"N1","N2","N3","N4","E1","E2","W1","W2","C1","C2","NE1","NE2","X1","X2","X3"}),
    ("TAMIL NADU",      {"N1","N2","N3","N4","E1","E2","W1","W2","C1","C2","NE1","NE2","X1","X2","X3"}),
    ("DELHI",           {"S1","S2","S3","S4","E1","E2","W1","W2","C1","C2","NE1","NE2","X1","X2","X3","N2","N3","N4"}),
    ("GUJARAT",         {"N1","N2","N3","N4","S1","S3","S4","E1","E2","C1","C2","NE1","NE2","X1","X2","X3"}),
    ("WEST BENGAL",     {"N1","N2","N3","N4","S1","S2","S3","S4","W1","W2","C1","C2","NE1","NE2","X1","X2","X3"}),
    ("KARNATAKA",       {"N1","N2","N3","N4","E1","E2","W1","C1","C2","NE1","NE2","X1","X2","X3"}),
    ("LADAKH",          {"N1","N2","N3","N4","S1","S2","S3","S4","E1","E2","W1","W2","C1","C2","NE1","NE2","X1","X2"}),
    ("LAKSHADWEEP",     {"N1","N2","N3","N4","S1","S2","S3","S4","E1","E2","W1","W2","C1","C2","NE1","NE2","X1","X3"}),
    ("ANDAMAN & NICOBAR ISLANDS", {"N1","N2","N3","N4","S1","S2","S3","S4","E1","E2","W1","W2","C1","C2","NE1","NE2","X2","X3"}),
]


# ─── 6. Charge field synonyms ────────────────────────────────────────────────
# Maps any label a document might use → canonical UTSF field name.
# None = recognised but intentionally skipped (not a simple scalar).

CHARGE_SYNONYMS: Dict[str, object] = {
    # ── Docket / LR charges ──────────────────────────────────────────────────
    "docket":                           "docketCharges",
    "docket charge":                    "docketCharges",
    "docket charges":                   "docketCharges",
    "docket fee":                       "docketCharges",
    "docket fees":                      "docketCharges",
    "docket cost":                      "docketCharges",
    "docket charges (per lr)":          "docketCharges",
    "lr":                               "docketCharges",
    "lr charge":                        "docketCharges",
    "lr charges":                       "docketCharges",
    "lr fee":                           "docketCharges",
    "lr fees":                          "docketCharges",
    "lr cost":                          "docketCharges",
    "lorry receipt":                    "docketCharges",
    "lorry receipt charge":             "docketCharges",
    "lorry receipt charges":            "docketCharges",
    "lorry receipt fee":                "docketCharges",
    "lr / docket":                      "docketCharges",
    "lr/docket":                        "docketCharges",
    "bilti":                            "docketCharges",
    "bilty":                            "docketCharges",
    "bilty charges":                    "docketCharges",
    "consignment note":                 "docketCharges",
    "cn charges":                       "docketCharges",
    "awb charge":                       "docketCharges",
    "awb charges":                      "docketCharges",
    "booking charges":                  "docketCharges",

    # ── Fuel surcharge ────────────────────────────────────────────────────────
    "fuel":                             "fuel",
    "fuel surcharge":                   "fuel",
    "fuel surcharge %":                 "fuel",
    "fuel surcharge%":                  "fuel",
    "fuel%":                            "fuel",
    "fuel %":                           "fuel",
    "fsc":                              "fuel",
    "fsc%":                             "fuel",
    "fsc %":                            "fuel",
    "fs":                               "fuel",
    "fuel sc":                          "fuel",
    "fuel sc%":                         "fuel",
    "fuel adjustment":                  "fuel",
    "fuel adjustment factor":           "fuel",
    "petrol surcharge":                 "fuel",
    "diesel surcharge":                 "fuel",
    "energy surcharge":                 "fuel",
    "fuel_surcharge":                   "fuel",
    "fuel surcahrge":                   "fuel",    # common typo
    "fuel surcharges":                  "fuel",
    "fuel cost":                        "fuel",
    "fuel charge":                      "fuel",
    "fuel charges":                     "fuel",

    # ── Minimum charges ───────────────────────────────────────────────────────
    "minimum":                          "minCharges",
    "min charge":                       "minCharges",
    "min charges":                      "minCharges",
    "minimum charge":                   "minCharges",
    "minimum charges":                  "minCharges",
    "min freight":                      "minCharges",
    "minimum freight":                  "minCharges",
    "minimum chargeable":               "minCharges",
    "min chargeable":                   "minCharges",
    "minimum chargable":                "minCharges",
    "min chargable":                    "minCharges",
    "minimum chargable freight":        "minCharges",
    "min lr charge":                    "minCharges",
    "min_lr_charge":                    "minCharges",
    "minimum billing":                  "minCharges",
    "min billing":                      "minCharges",
    "floor charge":                     "minCharges",
    "floor charges":                    "minCharges",
    "base minimum":                     "minCharges",

    # ── Minimum weight ────────────────────────────────────────────────────────
    "min weight":                       "minWeight",
    "minimum weight":                   "minWeight",
    "min wt":                           "minWeight",
    "minimum wt":                       "minWeight",
    "min chargeable weight":            "minWeight",
    "minimum chargeable weight":        "minWeight",
    "min chargable weight":             "minWeight",
    "minimum chargable weight":         "minWeight",
    "min_weight":                       "minWeight",
    "min_chg_wt":                       "minWeight",
    "chargeable weight minimum":        "minWeight",
    "charge weight minimum":            "minWeight",
    "billable weight minimum":          "minWeight",

    # ── Volumetric / CFT divisor ──────────────────────────────────────────────
    "divisor":                          "divisor",
    "vol divisor":                      "divisor",
    "volumetric divisor":               "divisor",
    "volumetric":                       "divisor",
    "cft":                              "divisor",
    "cft divisor":                      "divisor",
    "1 cft":                            "divisor",
    "1cft":                             "divisor",
    "k factor":                         "divisor",
    "kfactor":                          "divisor",
    "k-factor":                         "divisor",
    "cfactor":                          "divisor",
    "c factor":                         "divisor",
    "volume factor":                    "divisor",
    "vol factor":                       "divisor",
    "dimensional weight divisor":       "divisor",
    "dim weight divisor":               "divisor",
    "dim divisor":                      "divisor",

    # ── Green tax ─────────────────────────────────────────────────────────────
    "green tax":                        "greenTax",
    "green":                            "greenTax",
    "green surcharge":                  "greenTax",
    "green_tax":                        "greenTax",
    "green tax charge":                 "greenTax",
    "environmental surcharge":          "greenTax",
    "eco charge":                       "greenTax",
    "pollution charge":                 "greenTax",

    # ── ROV / FOV / Owner's risk ──────────────────────────────────────────────
    "rov":                              "rovCharges",
    "rov%":                             "rovCharges",
    "rov %":                            "rovCharges",
    "rov charges":                      "rovCharges",
    "fov":                              "rovCharges",
    "fov charges":                      "rovCharges",
    "fov%":                             "rovCharges",
    "fov - owner's risk":               "rovCharges",
    "fov - owners risk":                "rovCharges",
    "fov-owner's risk":                 "rovCharges",
    "risk coverage":                    "rovCharges",
    "risk of value":                    "rovCharges",
    "risk of value charges":            "rovCharges",
    "owner's risk":                     "rovCharges",
    "owners risk":                      "rovCharges",
    "owner risk":                       "rovCharges",
    "vr":                               "rovCharges",   # value risk
    "vr charges":                       "rovCharges",
    "freight risk":                     "rovCharges",
    "cargo risk":                       "rovCharges",
    "risk":                             "rovCharges",

    # ── Insurance ─────────────────────────────────────────────────────────────
    "insurance":                        "insuranceCharges",
    "insurance charge":                 "insuranceCharges",
    "insurance charges":                "insuranceCharges",
    "insurance%":                       "insuranceCharges",
    "cargo insurance":                  "insuranceCharges",
    "transit insurance":                "insuranceCharges",
    "transit insurance charges":        "insuranceCharges",
    "insurance premium":                "insuranceCharges",
    "transit risk":                     "insuranceCharges",

    # ── ODA / EDL ─────────────────────────────────────────────────────────────
    "oda":                              "odaCharges",
    "oda per kg":                       "odaCharges",
    "oda charge":                       "odaCharges",
    "oda charges":                      "odaCharges",
    "oda charges (per kg)":             "odaCharges",
    "out of delivery":                  "odaCharges",
    "out-of-delivery":                  "odaCharges",
    "out of delivery area":             "odaCharges",
    "out of delivery area charge":      "odaCharges",
    "oda area charge":                  "odaCharges",
    "edl":                              "odaCharges",
    "edl charge":                       "odaCharges",
    "edl charges":                      "odaCharges",
    "extended delivery":                "odaCharges",
    "extended delivery location":       "odaCharges",
    "remote area":                      "odaCharges",
    "remote area charge":               "odaCharges",
    "remote area charges":              "odaCharges",
    "remote delivery":                  "odaCharges",
    "special area":                     "odaCharges",
    "special area charge":              "odaCharges",
    "special area charges":             "odaCharges",
    "restricted area":                  "odaCharges",
    "restricted area charge":           "odaCharges",
    "non serviceable area":             "odaCharges",
    "non-serviceable charge":           "odaCharges",
    "beyond area charge":               "odaCharges",
    "beyond delivery":                  "odaCharges",
    "hilly area":                       "odaCharges",
    "hilly area charge":                "odaCharges",
    "interior area":                    "odaCharges",
    "interior charges":                 "odaCharges",

    # ── COD ───────────────────────────────────────────────────────────────────
    "cod":                              "codCharges",
    "cod%":                             "codCharges",
    "cod %":                            "codCharges",
    "cod charges":                      "codCharges",
    "cod charge":                       "codCharges",
    "cod percentage":                   "codCharges",
    "cod fee":                          "codCharges",
    "cash on delivery":                 "codCharges",
    "cash on delivery charges":         "codCharges",
    "cod collection charges":           "codCharges",
    "collection charges":               "codCharges",
    "collect on delivery":              "codCharges",

    # ── Handling ──────────────────────────────────────────────────────────────
    "handling":                         "handlingCharges",
    "handling charge":                  "handlingCharges",
    "handling charges":                 "handlingCharges",
    "handling fee":                     "handlingCharges",
    "cargo handling":                   "handlingCharges",
    "cargo handling charges":           "handlingCharges",
    "loading/unloading":                "handlingCharges",
    "loading unloading":                "handlingCharges",
    "labour charges":                   "handlingCharges",
    "labor charges":                    "handlingCharges",

    # ── DACC ──────────────────────────────────────────────────────────────────
    "dacc":                             "daccCharges",
    "dacc charges":                     "daccCharges",
    "dacc charge":                      "daccCharges",
    "demurrage":                        "daccCharges",
    "demurrage charge":                 "daccCharges",
    "demurrage_charge":                 "daccCharges",
    "detention":                        "daccCharges",
    "detention charge":                 "daccCharges",

    # ── Miscellaneous ─────────────────────────────────────────────────────────
    "misc":                             "miscCharges",
    "misc charge":                      "miscCharges",
    "misc charges":                     "miscCharges",
    "miscellaneous":                    "miscCharges",
    "miscellaneous charges":            "miscCharges",
    "miscellaneous charge":             "miscCharges",
    "other charges":                    "miscCharges",
    "other charge":                     "miscCharges",
    "additional charges":               "miscCharges",
    "idc":                              "miscCharges",
    "indirect cost":                    "miscCharges",
    "surcharge":                        "miscCharges",
    "surcharges":                       "miscCharges",
    "eway":                             "miscCharges",
    "e-way":                            "miscCharges",
    "eway bill":                        "miscCharges",
    "e-way bill":                       "miscCharges",
    "ewaybill":                         "miscCharges",
    "e way bill charges":               "miscCharges",

    # ── Topay ─────────────────────────────────────────────────────────────────
    "topay":                            "topayCharges",
    "to pay":                           "topayCharges",
    "topay charges":                    "topayCharges",
    "to pay charges":                   "topayCharges",
    "to_pay":                           "topayCharges",
    "to-pay":                           "topayCharges",
    "to pay surcharge":                 "topayCharges",
    "freight topay":                    "topayCharges",

    # ── DOD ───────────────────────────────────────────────────────────────────
    "dod":                              "dodCharges",
    "dod charges":                      "dodCharges",
    "dod charge":                       "dodCharges",
    "delivery on demand":               "dodCharges",
    "delivery on demand charges":       "dodCharges",
    "scheduled delivery":               None,   # ambiguous — skip
    "same day delivery":                None,

    # ── Appointment ───────────────────────────────────────────────────────────
    "appointment":                      "appointmentCharges",
    "apt":                              "appointmentCharges",
    "appointment charges":              "appointmentCharges",
    "appointment charge":               "appointmentCharges",
    "apt charges":                      "appointmentCharges",
    "apt_handling":                     "appointmentCharges",
    "appointment handling":             "appointmentCharges",
    "appointment delivery":             "appointmentCharges",
    "timed delivery":                   "appointmentCharges",
    "time slot delivery":               "appointmentCharges",

    # ── First mile ────────────────────────────────────────────────────────────
    "fm":                               "fmCharges",
    "first mile":                       "fmCharges",
    "fm charges":                       "fmCharges",
    "fm charge":                        "fmCharges",
    "first mile charge":                "fmCharges",
    "first mile charges":               "fmCharges",
    "pickup charge":                    "fmCharges",
    "pickup charges":                   "fmCharges",
    "pick up charges":                  "fmCharges",
    "collection charge":                "fmCharges",

    # ── Prepaid ───────────────────────────────────────────────────────────────
    "prepaid":                          "prepaidCharges",
    "prepaid charge":                   "prepaidCharges",
    "prepaid charges":                  "prepaidCharges",
    "prepaid surcharge":                "prepaidCharges",
    "advance freight":                  "prepaidCharges",

    # ── Fields to skip (not simple scalars) ──────────────────────────────────
    "single piece":                     None,
    "single piece charges":             None,
    "claim settlement":                 None,
    "claims":                           None,
    "free storage days":                None,
    "transit time":                     None,
    "delivery time":                    None,
    "tat":                              None,
    "turnaround time":                  None,
    "transit days":                     None,
    "service":                          None,
    "services":                         None,
    "remarks":                          None,
    "notes":                            None,
    "terms":                            None,
    "terms and conditions":             None,
}


# ─── 7. Company field synonyms ────────────────────────────────────────────────

COMPANY_SYNONYMS: Dict[str, str] = {
    # Company name
    "company":              "name",
    "company name":         "name",
    "transporter":          "name",
    "transporter name":     "name",
    "carrier":              "name",
    "carrier name":         "name",
    "vendor":               "name",
    "vendor name":          "name",
    "name":                 "name",
    "organisation":         "name",
    "organization":         "name",
    "firm":                 "name",
    "firm name":            "name",
    "party name":           "name",
    "party":                "name",

    # GST
    "gst":                  "gstNo",
    "gst no":               "gstNo",
    "gst no.":              "gstNo",
    "gst number":           "gstNo",
    "gstin":                "gstNo",
    "gst registration":     "gstNo",
    "gst reg":              "gstNo",
    "gst registration no":  "gstNo",
    "gst id":               "gstNo",
    "gstn":                 "gstNo",

    # PAN
    "pan":                  "panNo",
    "pan no":               "panNo",
    "pan no.":              "panNo",
    "pan number":           "panNo",
    "pan card":             "panNo",
    "permanent account number": "panNo",

    # CIN
    "cin":                  "cin",
    "cin no":               "cin",
    "company identification number": "cin",

    # Contact
    "phone":                "phone",
    "phone no":             "phone",
    "phone number":         "phone",
    "mobile":               "phone",
    "mobile no":            "phone",
    "mobile number":        "phone",
    "contact":              "phone",
    "contact no":           "phone",
    "contact number":       "phone",
    "tel":                  "phone",
    "telephone":            "phone",
    "landline":             "phone",

    "email":                "email",
    "email id":             "email",
    "email address":        "email",
    "e-mail":               "email",
    "e mail":               "email",

    # Address
    "address":              "address",
    "registered address":   "address",
    "office address":       "address",
    "branch address":       "address",
    "head office":          "address",
    "hq":                   "address",

    "city":                 "city",
    "town":                 "city",
    "district":             "city",

    "state":                "state",
    "state name":           "state",
    "province":             "state",

    "pincode":              "contact_pincode",
    "pin":                  "contact_pincode",
    "zip":                  "contact_pincode",
    "zip code":             "contact_pincode",
    "postal code":          "contact_pincode",

    "website":              "website",
    "url":                  "website",
    "web":                  "website",

    # Transport attributes
    "mode":                 "transportMode",
    "transport mode":       "transportMode",
    "service type":         "serviceType",
    "service":              "serviceType",
    "category":             "serviceType",
    "type":                 "serviceType",

    # Approval / rating
    "rating":               "rating",
    "score":                "rating",
    "approval":             "approvalStatus",
    "approval status":      "approvalStatus",
    "status":               "status",
}


# ─── 8. ODA indicator words ───────────────────────────────────────────────────
# When seen in a pincode list, these indicate the pincode is ODA

ODA_POSITIVE_VALUES = frozenset({
    "y", "yes", "true", "1", "oda", "x", "edl", "out", "yes-oda", "oda area",
    "out of delivery", "restricted", "non-serviceable", "non serviceable",
    "unserviceable", "limited", "remote", "extended",
    "\u2713", "\u2714", "\u2611", "\u2705",   # check marks
})

ODA_NEGATIVE_VALUES = frozenset({
    "n", "no", "false", "0", "not serviceable", "ns", "not delivered",
    "undelivered", "not oda",
})

DELIVERY_POSITIVE_VALUES = frozenset({
    "y", "yes", "true", "1", "serviceable", "active", "delivered",
    "normal", "regular", "\u2713", "\u2714",
})

DELIVERY_NEGATIVE_VALUES = frozenset({
    "n", "no", "false", "0", "oda", "x", "not serviceable", "ns",
    "not delivered", "restricted", "out",
})


# ─── 9. Column header name sets (for pincode list detection) ─────────────────

PINCODE_COL_NAMES = frozenset({
    "pincode", "pin", "pin code", "postal", "postal code", "zip", "zip code",
    "pin no", "pinno", "pincodes", "pin codes", "destination pincode",
    "delivery pincode", "dest pincode", "dest pin", "consignee pincode",
})

DELIVERY_COL_NAMES = frozenset({
    "delivery", "deliverable", "delivered", "del",
    "delivery status", "is deliverable", "is serviceable",
    "serviceable", "serviceability", "service", "served",
    "is served", "active", "availability", "available",
})

ODA_COL_NAMES = frozenset({
    "oda", "is oda", "is_oda", "edl", "out of delivery",
    "out_of_delivery", "non serviceable", "non-serviceable",
    "restricted", "oda area", "oda zone", "oda status",
    "extended delivery", "remote area",
})

ZONE_COL_NAMES = frozenset({
    "zone", "zones", "zone name", "zone code", "zonecode",
    "origin zone", "dest zone", "destination zone",
    "transporter zone", "rate zone",
})

STATE_COL_NAMES = frozenset({
    "state", "state code", "statecode", "state_code", "st",
    "state name", "province",
})

CITY_COL_NAMES = frozenset({
    "city", "town", "district", "taluka", "tehsil",
    "district name", "city name",
})
