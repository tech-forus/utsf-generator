"""
LLM Prompts for FC4 Data Extraction
=====================================
All prompts used by the Ollama extractor.
Designed to be precise and return clean JSON.
"""

COMPANY_EXTRACTION_PROMPT = """You are extracting company/transporter details from a logistics document.
Return ONLY valid JSON with these fields (use null for missing):
{
  "name": "full company name",
  "shortName": "abbreviated name or null",
  "code": "vendor/company code or null",
  "gstNo": "GST number (15 chars) or null",
  "panNo": "PAN number (10 chars) or null",
  "phone": "phone number or null",
  "email": "email or null",
  "address": "full address or null",
  "city": "city or null",
  "state": "state or null",
  "pincode": "6-digit pincode or null",
  "website": "website URL or null",
  "transportMode": "road or air or rail or sea",
  "serviceType": "LTL or FTL or PTL or Courier or Express"
}"""


CHARGES_EXTRACTION_PROMPT = """You are extracting freight charges from a logistics rate card.
Return ONLY valid JSON. All numeric values should be numbers (not strings).
{
  "docketCharges": number (flat charge per shipment, e.g. 50),
  "minCharges": number (minimum freight floor, e.g. 150),
  "minWeight": number (min chargeable weight in kg, default 0.5),
  "volumetricDivisor": number (default 5000),
  "greenTax": number (flat green/carbon tax per shipment),
  "daccCharges": number (destination area congestion charge),
  "miscCharges": number (other flat charges),
  "fuel": number (fuel surcharge percentage, e.g. 18 means 18%),
  "rovCharges": {
    "type": "percentage_freight",
    "variable": number (ROV % of freight),
    "fixed": number (minimum ROV Rs)
  },
  "insuranceCharges": {
    "variable": number (insurance % of invoice value),
    "fixed": number (minimum insurance Rs)
  },
  "odaCharges": {
    "type": "per_kg_minimum or weight_band or distance_weight_matrix",
    "perKg": number or null,
    "minimum": number or null,
    "bands": array or null,
    "matrix": array or null
  },
  "handlingCharges": {
    "type": "percentage_freight or per_kg_minimum",
    "variable": number,
    "fixed": number
  },
  "fmCharges": {"variable": number, "fixed": number},
  "appointmentCharges": {"variable": number, "fixed": number},
  "codCharges": {"variable": number, "fixed": number},
  "topayCharges": {"variable": number, "fixed": number},
  "prepaidCharges": {"variable": number, "fixed": number},
  "dodCharges": number or null
}
Only include fields that are explicitly mentioned in the data. Use null for unknown."""


ZONE_MATRIX_EXTRACTION_PROMPT = """You are extracting a freight zone price matrix.
The matrix shows price per kg from origin zone (rows) to destination zone (columns).
Valid zones: N1, N2, N3, N4, S1, S2, S3, S4, E1, E2, W1, W2, C1, C2, NE1, NE2

Return ONLY valid JSON:
{
  "zoneMatrix": {
    "N1": {"N1": 5.5, "N2": 7.0, "S1": 12.0, ...},
    "N2": {"N1": 7.0, "N2": 6.0, ...},
    ...
  }
}
Only include zones that have data. Rates should be per-kg values (typically 4-50 range)."""


SERVICEABILITY_PROMPT = """You are extracting pincode serviceability data for a freight transporter.
The data may contain lists of pincodes, cities, or zones the transporter serves.

Return ONLY valid JSON:
{
  "served_pincodes": [110001, 110002, ...] (array of 6-digit integers if available),
  "oda_pincodes": [110401, ...] (ODA/remote pincodes subset),
  "zone_coverage": {
    "N1": "FULL or PARTIAL or NONE",
    "N2": "FULL or PARTIAL or NONE",
    ...
  },
  "notes": "any important notes about coverage"
}
If only zone-level info available, set zone_coverage but leave served_pincodes empty."""


MERGE_PROMPT = """You are merging multiple extracted data pieces from different files about the same transporter.
Combine them into a single coherent record, preferring more specific/detailed values over vague ones.

Return ONLY valid JSON with this structure:
{
  "company_details": {...},
  "charges": {...},
  "zone_matrix": {...},
  "served_pincodes": [...],
  "oda_pincodes": [...]
}
Resolve conflicts by taking the most complete/specific value."""


ODA_EXTRACTION_PROMPT = """You are extracting ODA (Out of Delivery Area) charge configuration from a logistics document.
ODA charges apply for deliveries to remote/difficult areas.

Return ONLY valid JSON for one of these types:

Type 1 - Simple per kg with minimum:
{"type": "per_kg_minimum", "perKg": 5.0, "minimum": 100.0}

Type 2 - Weight bands (flat charge by weight):
{"type": "weight_band", "bands": [
  {"minKg": 0, "maxKg": 100, "charge": 80},
  {"minKg": 100, "maxKg": 500, "charge": 120},
  {"minKg": 500, "maxKg": null, "charge": 200}
]}

Type 3 - Distance x Weight matrix:
{"type": "distance_weight_matrix", "matrix": [
  {"minKm": 0, "maxKm": 26, "bands": [
    {"minKg": 0, "maxKg": 101, "charge": 35},
    {"minKg": 101, "maxKg": 501, "charge": 45}
  ]},
  {"minKm": 26, "maxKm": 51, "bands": [...]}
]}"""
