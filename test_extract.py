"""
test_extract.py - Test any file against the extract-prices pipeline.

Usage:
    python test_extract.py  "C:/path/to/file.xlsx"
    python test_extract.py  "C:/path/to/file.pdf"

Runs the FULL extraction pipeline (same as Flask /api/extract-prices)
and prints a detailed zone-by-zone breakdown with per-zone reasoning.
Also writes results to logs/extract_test_YYYYMMDD_HHMMSS.log
"""

import sys, os, time, json
from datetime import datetime

# -- Setup paths --------------------------------------------------------------
REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
SRC_DIR   = os.path.join(REPO_ROOT, "utsf-generator", "src")
LOG_DIR   = os.path.join(REPO_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

sys.path.insert(0, SRC_DIR)

ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
log_path = os.path.join(LOG_DIR, f"extract_test_{ts}.log")

# Tee all output to log file
class _Tee:
    def __init__(self, stream, path):
        self._s = stream
        self._f = open(path, "w", encoding="utf-8", buffering=1)
    def write(self, msg):
        self._s.write(msg); self._f.write(msg)
    def flush(self):
        self._s.flush(); self._f.flush()
    def fileno(self): return self._s.fileno()
    @property
    def encoding(self): return getattr(self._s, "encoding", "utf-8")
    @property
    def errors(self): return getattr(self._s, "errors", "replace")

sys.stdout = _Tee(sys.stdout, log_path)
sys.stderr = _Tee(sys.stderr, log_path)

print(f"{'='*70}")
print(f"  FreightCompare extract-prices test  -  {datetime.now().isoformat()}")
print(f"  Log: {log_path}")
print(f"{'='*70}\n")

# -- File argument -------------------------------------------------------------
if len(sys.argv) < 2:
    print("Usage: python test_extract.py <file_path>")
    sys.exit(1)

file_path = sys.argv[1]
if not os.path.isfile(file_path):
    print(f"ERROR: File not found: {file_path}")
    sys.exit(1)

fname = os.path.basename(file_path)
ext   = os.path.splitext(fname)[1].lower()
print(f"File : {file_path}")
print(f"Size : {os.path.getsize(file_path)/1024:.1f} KB")
print(f"Ext  : {ext}")

# -- Auto-classify -------------------------------------------------------------
from web.app import auto_classify_file
category = auto_classify_file(fname)
print(f"Category: {category}  (auto-classified from filename)")
print()

# -- Parse ---------------------------------------------------------------------
t0 = time.time()

import tempfile, shutil
fd, tmp = tempfile.mkstemp(suffix=ext)
os.close(fd)
shutil.copy(file_path, tmp)

try:
    if ext in (".xlsx", ".xls", ".csv", ".tsv"):
        from parsers.excel_parser import ExcelParser
        parser = ExcelParser()
        result = parser.parse(tmp)
        parse_source = "excel"

    elif ext == ".pdf":
        from parsers.pdf_parser import PDFParser
        parser = PDFParser()
        result = parser.parse(tmp)
        parse_source = "pdf"

    else:
        print(f"No parser for {ext}")
        sys.exit(1)
finally:
    try: os.remove(tmp)
    except: pass

elapsed = time.time() - t0
data = result.get("data", {})
zm   = data.get("zone_matrix", {})
charges = data.get("charges", {})
pincodes = data.get("served_pincodes", [])
company  = data.get("company_details", {})

print(f"\n{'-'*70}")
print(f"  PARSE RESULT  ({elapsed:.2f}s via {parse_source})")
print(f"{'-'*70}")

# -- Zone matrix ---------------------------------------------------------------
print(f"\n[ZONE MATRIX]  {len(zm)} origin(s)")
if zm:
    for orig in sorted(zm.keys()):
        dests = zm[orig]
        print(f"  {orig}  ->  {len(dests)} destinations")
        for dest in sorted(dests.keys()):
            print(f"         {dest}: {dests[dest]} Rs/kg")
else:
    print("  (none)")

# -- Rate mode -----------------------------------------------------------------
rate_mode = data.get("_rate_mode", "")
if zm:
    all_rates = [r for dests in zm.values() for r in dests.values() if isinstance(r,(int,float)) and r > 0]
    if all_rates:
        print(f"\n[RATE MODE]  {rate_mode or 'unknown'}")
        print(f"  min={min(all_rates):.2f}  avg={sum(all_rates)/len(all_rates):.2f}  max={max(all_rates):.2f}  Rs/kg")

# -- Charges -------------------------------------------------------------------
print(f"\n[CHARGES]  {len(charges)} field(s)")
for k, v in sorted(charges.items()):
    print(f"  {k}: {v}")

# -- Serviceability ------------------------------------------------------------
print(f"\n[SERVICEABILITY]  {len(pincodes)} served pincodes")
if pincodes:
    # Show zone distribution
    from parsers.oicr_engine import get_oicr_engine
    oicr = get_oicr_engine()
    zdist = oicr.infer_zones_from_pincodes(pincodes)
    total = sum(zdist.values())
    print(f"  Zone distribution ({total} mapped):")
    for z, cnt in sorted(zdist.items(), key=lambda x: -x[1]):
        print(f"    {z}: {cnt} pincodes ({cnt*100//total}%)")

# -- Company -------------------------------------------------------------------
print(f"\n[COMPANY INFO]")
if company:
    for k, v in sorted(company.items()):
        print(f"  {k}: {v}")
else:
    print("  (none)")

# -- Final response JSON (what extract-prices would return) --------------------
from parsers.oicr_engine import _smart_zone_rate
response = {
    "success":           bool(zm),
    "zoneRates":         zm,
    "confidence":        (80 if parse_source == "excel" else 65) if zm else 0,
    "source":            parse_source,
    "zonesFound":        len(zm),
    "rateMode":          rate_mode,
    "message":           f"{len(zm)} origin(s) extracted" if zm else "No zone matrix found",
}
print(f"\n{'-'*70}")
print(f"  RESPONSE SUMMARY")
print(f"{'-'*70}")
print(json.dumps({k: v for k, v in response.items() if k != "zoneRates"}, indent=2))
print(f"\nLog saved to: {log_path}")
print(f"{'='*70}")
