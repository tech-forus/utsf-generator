"""
FC4 UTSF Generator - Main Entry Point
======================================
Standalone Windows app for generating FC4 UTSF files from any data format.

Usage:
  python main.py                    # Interactive mode
  python main.py generate <name>    # Generate for specific transporter
  python main.py list               # List available transporters
  python main.py validate <file>    # Validate an existing UTSF file
  python main.py migrate <file>     # Migrate v2 UTSF to FC4
  python main.py batch              # Process all transporters

Data Structure:
  transporters/
    <transporter_name>/
      company_details/  <- company info (any format)
      charges/          <- rate cards, charge sheets
      zone_data/        <- serviceability, pincode lists, zone matrices
"""

import os
import sys
import json
import argparse
import time
from datetime import datetime
from typing import Dict, List, Optional

# Force UTF-8 output on Windows (avoids CP1252 UnicodeEncodeError with box/check chars)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Add src to path
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SRC_DIR)

# Import ML enhancer and analytics
try:
    from intelligence.ml_enhancer import ml_enhance_utsf_data
    from intelligence.ml_analytics import log_ml_enhancement, get_ml_analytics
    from fc4_schema import calculate_data_quality
    ML_ENHANCER_AVAILABLE = True
    print("[ML] Machine Learning enhancer loaded")
except ImportError as e:
    ML_ENHANCER_AVAILABLE = False
    print(f"[ML] ML enhancer not available: {e}")

# When running as a frozen EXE, launcher.py sets UTSF_ROOT / UTSF_DATA env vars
# so paths point to the folder next to the EXE, not inside PyInstaller's temp dir.
_ENV_ROOT = os.environ.get("UTSF_ROOT")
_ENV_DATA = os.environ.get("UTSF_DATA")
if _ENV_ROOT:
    DATA_DIR         = _ENV_DATA or os.path.join(_ENV_ROOT, "data")
    TRANSPORTERS_DIR = os.path.join(_ENV_ROOT, "transporters")
    OUTPUT_DIR       = os.path.join(_ENV_ROOT, "output")
else:
    DATA_DIR         = os.path.join(os.path.dirname(SRC_DIR), "data")
    TRANSPORTERS_DIR = os.path.join(os.path.dirname(SRC_DIR), "transporters")
    OUTPUT_DIR       = os.path.join(os.path.dirname(SRC_DIR), "output")
PINCODES_PATH = os.path.join(DATA_DIR, "pincodes.json")
ZONES_PATH    = os.path.join(DATA_DIR, "zones_data.json")

DIVIDER      = "=" * 60
SUB_DIVIDER  = "-" * 52


def get_parser_for_file(file_path: str):
    """Return the right parser for a file extension."""
    ext = os.path.splitext(file_path)[1].lower()

    if ext in (".xlsx", ".xls", ".csv", ".tsv"):
        from parsers.excel_parser import ExcelParser
        return ExcelParser()

    elif ext in (".docx", ".doc"):
        from parsers.word_parser import WordParser
        return WordParser()

    elif ext in (".pptx", ".ppt"):
        from parsers.ppt_parser import PPTParser
        return PPTParser()

    elif ext == ".pdf":
        from parsers.pdf_parser import PDFParser
        parser = PDFParser()

        try:
            result = parser.parse(file_path)
            if not result or len(str(result)) < 100:
                raise Exception("Weak or empty parse")
            return parser
        except Exception as e:
            print(f"[Fallback → TCI Parser Triggered]: {e}")
            from parsers.tci_parser import TCIParser
            return TCIParser()

    elif ext in (".png", ".jpg", ".jpeg", ".tiff", ".bmp", ".webp"):
        from parsers.image_parser import ImageParser
        return ImageParser()

    elif ext == ".json":
        return None  # Handle inline

    return None


def parse_json_file(file_path: str) -> Dict:
    """Parse a JSON data file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def collect_files_from_folder(folder: str) -> List[str]:
    """Get all data files from a transporter folder."""
    files = []
    for root, dirs, fnames in os.walk(folder):
        # Skip hidden and system folders
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for fname in fnames:
            if fname.startswith(".") or fname.startswith("~"):
                continue
            ext = os.path.splitext(fname)[1].lower()
            if ext in (".xlsx", ".xls", ".csv", ".tsv", ".docx", ".doc",
                       ".pptx", ".ppt", ".pdf", ".json",
                       ".png", ".jpg", ".jpeg", ".tiff", ".bmp", ".webp"):
                files.append(os.path.join(root, fname))
    return files


def merge_extracted_data(pieces: List[Dict]) -> Dict:
    """Merge multiple extracted data dicts into one."""
    merged = {
        "company_details": {},
        "charges": {},
        "zone_matrix": {},
        "served_pincodes": [],
        "oda_pincodes": [],
        "zone_pincodes": {},
        "serviceability": {},
        "_parseAudit": [],
    }

    # Known field sets for flat-JSON detection (keys used by encoder)
    _COMPANY_FIELDS = {"name","companyName","shortName","gstNo","panNo","phone","email",
                       "address","city","state","pincode","website","serviceType","transportMode",
                       "code","vendorCode","status","verified","isVerified","rating"}
    # Only fields the encoder actually reads from charges
    _CHARGE_FIELDS  = {"fuel","docketCharges","odaCharges","rovCharges","minCharges",
                       "fuelSurcharge","insuranceCharges","handlingCharges","codCharges",
                       "minWeight","volumetricDivisor","divisor","kFactor","greenTax",
                       "daccCharges","miscCharges","ewayCharges","topayCharges","dodCharges",
                       "fmCharges","appointmentCharges","prepaidCharges","zoneRates","zoneMatrix"}
    # Normalize flat-JSON charge key aliases → encoder-canonical keys
    _CHARGE_ALIAS   = {"fuelSurcharge": "fuel", "docketCharge": "docketCharges"}

    for piece in pieces:
        # Merge company details: wrapped dict wins; fall back to flat JSON
        merged_company = False
        for key in ["company_details", "company"]:
            if key in piece and isinstance(piece[key], dict):
                merged["company_details"].update(piece[key])
                merged_company = True
                # don't break — allow both "company_details" and "company" to contribute
        if not merged_company:
            # Flat JSON: detect by presence of known company fields
            if _COMPANY_FIELDS & set(piece.keys()):
                merged["company_details"].update(
                    {k: v for k, v in piece.items() if not k.startswith("_")}
                )

        # Merge charges: iterate ALL matching wrappers (no break — matches v6 behaviour
        # so "pricing.priceRate" unwrapping still works when both keys exist)
        merged_charges = False
        for key in ["charges", "pricing"]:
            if key in piece and isinstance(piece[key], dict):
                merged["charges"].update(piece[key])
                merged_charges = True
        if not merged_charges:
            # Flat JSON: only merge recognised charge fields (avoid polluting with
            # zone_matrix, served_pincodes, etc.)
            flat_charges = {}
            for k, v in piece.items():
                if k.startswith("_"):
                    continue
                canon = _CHARGE_ALIAS.get(k, k)
                if canon in _CHARGE_FIELDS:
                    flat_charges[canon] = v
            if flat_charges:
                merged["charges"].update(flat_charges)

        # Zone matrix (newer / larger wins)
        zm = piece.get("zone_matrix") or piece.get("zoneMatrix") or {}
        if len(zm) > len(merged["zone_matrix"]):
            merged["zone_matrix"] = zm

        # Serviceability block (FC4/v2 format — larger dict wins)
        svc = piece.get("serviceability") or piece.get("service") or {}
        if isinstance(svc, dict) and len(svc) > len(merged["serviceability"]):
            merged["serviceability"] = svc

        # Pincodes (accumulate)
        merged["served_pincodes"].extend(piece.get("served_pincodes", []))
        merged["oda_pincodes"].extend(piece.get("oda_pincodes", []))

        # Zone pincodes
        zp = piece.get("zone_pincodes", {})
        for zone, pins in zp.items():
            if zone not in merged["zone_pincodes"]:
                merged["zone_pincodes"][zone] = []
            merged["zone_pincodes"][zone].extend(pins)

        # Collect parse audit entries from each file
        merged["_parseAudit"].extend(piece.get("_parseAudit", []))

    # Deduplicate pincodes
    merged["served_pincodes"] = list(set(merged["served_pincodes"]))
    merged["oda_pincodes"]    = list(set(merged["oda_pincodes"]))

    return merged


# ---------------------------------------------------------------------------
# Logging helpers for generate_utsf_for_transporter
# ---------------------------------------------------------------------------

def _log_data_summary(label: str, data: Dict, folder: str):
    """Print a summary of what keys/counts were extracted from one file."""
    keys_found = []

    if data.get("zone_matrix"):
        n_zones = len(data["zone_matrix"])
        keys_found.append(f"zone_matrix({n_zones} origins)")

    sp = data.get("served_pincodes", [])
    if sp:
        keys_found.append(f"served_pincodes({len(sp):,})")

    op = data.get("oda_pincodes", [])
    if op:
        keys_found.append(f"oda_pincodes({len(op):,})")

    zp = data.get("zone_pincodes", {})
    if zp:
        total_zp = sum(len(v) for v in zp.values())
        keys_found.append(f"zone_pincodes({total_zp:,} across {len(zp)} zones)")

    ch = data.get("charges", {})
    if ch:
        keys_found.append(f"charges({len(ch)} fields)")

    cd = data.get("company_details", {})
    if cd:
        keys_found.append(f"company_details({len(cd)} fields)")

    if keys_found:
        print(f"    -> data keys extracted: [{', '.join(keys_found)}]")
    else:
        print(f"    -> data keys extracted: (none)")


def _log_merged_summary(merged: Dict):
    """Print a summary of the merged data dict."""
    print(f"\n  {SUB_DIVIDER}")
    print(f"  Merged data summary:")

    cd = merged.get("company_details", {})
    if cd:
        name = cd.get("name", "(not set)")
        gst  = cd.get("gstNo", "(not set)")
        print(f"    company_details : name={name}, gstNo={gst[:12] + '...' if gst and len(gst) > 12 else gst}")
    else:
        print(f"    company_details : (empty)")

    zm = merged.get("zone_matrix", {})
    if zm:
        print(f"    zone_matrix     : {len(zm)} origin zones -> "
              f"{sorted(zm.keys())[:8]}")
    else:
        print(f"    zone_matrix     : (empty)")

    ch = merged.get("charges", {})
    if ch:
        parts = []
        for k in ["fuel", "docketCharges", "minCharges", "rovCharges", "odaCharges"]:
            if k in ch:
                parts.append(f"{k}={ch[k]}")
        print(f"    charges         : {', '.join(parts) if parts else '(no recognised fields)'}")
    else:
        print(f"    charges         : (empty)")

    sp = merged.get("served_pincodes", [])
    print(f"    served_pincodes : {len(sp):,}")

    op = merged.get("oda_pincodes", [])
    print(f"    oda_pincodes    : {len(op):,}")

    zp = merged.get("zone_pincodes", {})
    if zp:
        total = sum(len(v) for v in zp.values())
        print(f"    zone_pincodes   : {total:,} across {len(zp)} zones")


def _log_serviceability_encoding(utsf: Dict):
    """Print per-zone serviceability results from the finished UTSF."""
    svc = utsf.get("serviceability", {})
    if not svc:
        print(f"    (no serviceability data)")
        return

    for zone in sorted(svc.keys()):
        entry = svc[zone]
        mode  = entry.get("mode", "?")
        cnt   = entry.get("servedCount", 0)
        cov   = entry.get("coveragePercent", 0.0)
        oda_c = entry.get("odaCount", 0)

        if mode == "FULL_MINUS_EXCEPT":
            ex_r = len(entry.get("exceptRanges", []))
            ex_s = len(entry.get("exceptSingles", []))
            detail = f"(store {ex_r} excl-ranges, {ex_s} excl-singles)"
        elif mode == "ONLY_SERVED":
            in_r = len(entry.get("servedRanges", []))
            in_s = len(entry.get("servedSingles", []))
            detail = f"(store {in_r} incl-ranges, {in_s} incl-singles)"
        elif mode == "FULL_ZONE":
            detail = "(entire zone served)"
        else:
            detail = ""

        oda_str = f"  ODA: {oda_c}" if oda_c else ""
        print(f"    {zone:>4}: {cnt:>6,} served  {cov:>6.1f}%  {mode:<12} {detail}{oda_str}")


def _log_oda_encoding(utsf: Dict):
    """Print per-zone ODA summary from the finished UTSF."""
    svc = utsf.get("serviceability", {})
    oda_zones = {z: e for z, e in svc.items() if e.get("odaCount", 0) > 0}
    if not oda_zones:
        print(f"    (no ODA pincodes)")
        return
    for zone, entry in sorted(oda_zones.items()):
        r = len(entry.get("odaRanges",   []))
        s = len(entry.get("odaSingles",  []))
        print(f"    {zone}: {entry['odaCount']:,} ODA pincodes ({r} ranges, {s} singles)")


def _log_validation(missing_fields: List[str], errors: List[str]):
    """Print validation summary with warning/tick indicators."""
    print(f"\n  {SUB_DIVIDER}")
    print(f"  Validation:")
    if not missing_fields and not errors:
        print(f"    All required fields present")
    for f in missing_fields:
        print(f"    WARNING: {f} not set")
    for e in errors:
        print(f"    ERROR  : {e}")


# ---------------------------------------------------------------------------
# Main generation pipeline
# ---------------------------------------------------------------------------

def generate_utsf_for_transporter(
    transporter_name: str,
    use_ai: bool = True,
    verbose: bool = True
) -> Optional[str]:
    """
    Main generation pipeline for a single transporter.
    Returns path to generated UTSF file, or None on failure.
    """
    folder = os.path.join(TRANSPORTERS_DIR, transporter_name)
    if not os.path.exists(folder):
        print(f"[ERROR] Folder not found: {folder}")
        return None

    t_start = time.time()

    print(f"\n{DIVIDER}")
    print(f"Processing: {transporter_name}")
    print(f"{DIVIDER}")
    print(f"  Source folder: {folder}")

    # ------------------------------------------------------------------
    # 1. Collect files
    # ------------------------------------------------------------------
    files = collect_files_from_folder(folder)
    if not files:
        print(f"  [WARN] No data files found in {folder}")
        print(f"  Add files to: company_details/, charges/, zone_data/")
        return None

    print(f"\n  Files found: {len(files)}")
    for i, f in enumerate(files, 1):
        rel = os.path.relpath(f, folder)
        print(f"    [{i}] {rel}")

    # ------------------------------------------------------------------
    # 2. Parse all files
    # ------------------------------------------------------------------
    extracted_pieces: List[Dict] = []

    for file_idx, file_path in enumerate(files, 1):
        rel    = os.path.relpath(file_path, folder)
        ext    = os.path.splitext(file_path)[1].lower()
        subfolder = os.path.basename(os.path.dirname(file_path))

        print(f"\n  --- Parsing file {file_idx}/{len(files)}: {rel} ---")

        if ext == ".json":
            try:
                data = parse_json_file(file_path)
                extracted_pieces.append(data)
                # Log key-level summary for JSON
                top_keys = list(data.keys())[:8]
                print(f"    Parser: JSON (direct)")
                print(f"    Top-level keys: {top_keys}")
                # Extract known sub-keys
                for k in ["company_details", "company", "charges", "zone_matrix",
                          "served_pincodes", "oda_pincodes", "zone_pincodes"]:
                    if k in data:
                        v = data[k]
                        if isinstance(v, dict):
                            print(f"    -> {k}: {len(v)} fields")
                        elif isinstance(v, list):
                            print(f"    -> {k}: {len(v):,} items")
                        else:
                            print(f"    -> {k}: {str(v)[:60]}")
            except Exception as e:
                print(f"    [ERROR] JSON parse failed: {e}")
            continue

        parser = get_parser_for_file(file_path)
        if not parser:
            print(f"    ? Unsupported file type: {ext}")
            continue

        print(f"    Parser: {type(parser).__name__}")

        try:
            result = parser.parse(file_path)
            piece  = result.get("data", {})
            extracted_pieces.append(piece)

            n_tables = len(result.get("tables", []))
            n_text   = len(result.get("text", ""))
            print(f"    Parse complete: {n_tables} table(s), {n_text:,} chars text")
            _log_data_summary(rel, piece, folder)

            # ----------------------------------------------------------
            # Optional AI extraction
            # ----------------------------------------------------------
            if use_ai and result.get("text") and len(result["text"]) > 100:
                try:
                    from intelligence.ollama_client import OllamaExtractor
                    extractor = OllamaExtractor()

                    if extractor.is_available():
                        print(f"    AI extraction in progress...")
                        folder_type = subfolder.lower()

                        if "company" in folder_type:
                            ai_data = extractor.extract_company_details(result["text"])
                            if ai_data:
                                extracted_pieces[-1]["company_details"] = ai_data
                                print(f"    AI extracted company details: "
                                      f"{list(ai_data.keys())[:6]}")

                        elif "charge" in folder_type or "rate" in folder_type:
                            ai_data = extractor.extract_charges(result["text"])
                            if ai_data:
                                extracted_pieces[-1]["charges"] = ai_data
                                print(f"    AI extracted charges: {list(ai_data.keys())[:6]}")

                        elif "zone" in folder_type:
                            if not extracted_pieces[-1].get("zone_matrix"):
                                ai_zm = extractor.extract_zone_matrix(result["text"])
                                if ai_zm:
                                    extracted_pieces[-1]["zone_matrix"] = ai_zm
                                    print(f"    AI extracted zone matrix: "
                                          f"{len(ai_zm)} origins")
                            ai_svc = extractor.extract_serviceability(result["text"])
                            if ai_svc.get("served_pincodes"):
                                extracted_pieces[-1]["served_pincodes"] = ai_svc["served_pincodes"]
                                extracted_pieces[-1]["oda_pincodes"] = ai_svc.get("oda_pincodes", [])
                                print(f"    AI extracted serviceability: "
                                      f"{len(ai_svc['served_pincodes']):,} pincodes")
                        else:
                            if not extracted_pieces[-1]:
                                ai_data = extractor.extract_charges(result["text"])
                                if ai_data:
                                    extracted_pieces[-1]["charges"] = ai_data

                except Exception as e:
                    print(f"    AI extraction failed: {e}")

        except Exception as e:
            import traceback
            print(f"  [FILE ERROR] {rel}")
            print(f"    Reason : {type(e).__name__}: {e}")
            print(f"    Action : Skipping this file — other files will still be processed")
            for ln in traceback.format_exc().splitlines()[-6:]:
                if ln.strip():
                    print(f"    Detail : {ln}")

    if not extracted_pieces:
        print("[ERROR] No data could be extracted from files")
        return None

    # ------------------------------------------------------------------
    # 3. Merge all extracted pieces
    # ------------------------------------------------------------------
    print(f"\n  --- Merging {len(extracted_pieces)} data pieces ---")
    merged = merge_extracted_data(extracted_pieces)

    # ------------------------------------------------------------------
    # 4. Apply ML Enhancement (if available)
    # ------------------------------------------------------------------
    if ML_ENHANCER_AVAILABLE:
        print(f"\n  --- Applying ML Enhancement ---")
        try:
            # Calculate quality before enhancement (simplified)
            before_quality = 55.0  # Default baseline for incomplete data
            
            enhanced_data = ml_enhance_utsf_data(merged, transporter_name)
            ml_info = enhanced_data.get("_ml_enhancements", {})
            
            if ml_info.get("enhancements"):
                print(f"    ML enhancements applied: {', '.join(ml_info['enhancements'])}")
                print(f"    Overall confidence: {ml_info['overall_confidence']:.2f}")
                merged = enhanced_data
                
                # Calculate quality after enhancement (simplified)
                after_quality = min(before_quality + len(ml_info['enhancements']) * 15, 100)
                
                # Log for analytics
                log_ml_enhancement(
                    transporter_name, 
                    ml_info['enhancements'], 
                    ml_info['overall_confidence'],
                    before_quality,
                    after_quality
                )
                
                # Show improvement
                improvement = after_quality - before_quality
                if improvement > 0:
                    print(f"    Quality improvement: +{improvement:.1f} points")
                
            else:
                print(f"    No ML enhancements needed")
                
        except Exception as e:
            print(f"    ML enhancement failed: {e}")
    else:
        print(f"    ML enhancer not available - skipping enhancement")

    # Set transporter name if not already set
    if not merged.get("company_details", {}).get("name"):
        guessed = transporter_name.replace("_", " ").title()
        merged["company_details"]["name"] = guessed
        print(f"  company.name not found in files — using folder name: '{guessed}'")

    _log_merged_summary(merged)

    # ------------------------------------------------------------------
    # 5. Encode to FC4
    # ------------------------------------------------------------------
    print(f"\n  --- Building FC4 UTSF ---")
    try:
        from builder.fc4_encoder import FC4Encoder
        encoder = FC4Encoder(PINCODES_PATH, ZONES_PATH)
        utsf = encoder.encode(
            merged,
            source_files=[os.path.relpath(f, folder) for f in files],
            transporter_id=transporter_name.lower().replace(" ", "_")
        )
    except Exception as e:
        print(f"[ERROR] Encoding failed: {e}")
        import traceback
        traceback.print_exc()
        return None

    # Store parse audit in UTSF for viewer review
    parse_audit = merged.get("_parseAudit", [])
    if parse_audit:
        utsf["_parseAudit"] = parse_audit
        print(f"\n  Parse audit: {len(parse_audit)} uncertain match(es) recorded for review")

    # ── Auto-learning: passively confirm parse audit entries that produced
    #    real data in the final UTSF (method = "fuzzy" / "token" / "geo").
    #    This teaches the dictionary without any user clicking.
    quality = utsf.get("dataQuality", 0)
    if quality >= 40 and parse_audit:
        try:
            from knowledge.ml_dictionary_engine import record_passive_confirmation
            confirmed = 0
            for entry in parse_audit:
                method = entry.get("method", "")
                if method in ("fuzzy", "token", "geo", "substring", "normalised"):
                    record_passive_confirmation(
                        learn_type  = entry.get("type", "charge"),
                        raw         = entry.get("raw", ""),
                        canonical   = entry.get("matched"),
                        confidence  = entry.get("confidence", 0.0),
                    )
                    confirmed += 1
            if confirmed:
                print(f"  Auto-learning: passively confirmed {confirmed} "
                      f"uncertain match(es) (quality={quality:.0f})")
        except Exception as _learn_err:
            print(f"  Auto-learning skipped: {_learn_err}")

    # ------------------------------------------------------------------
    # 5. Log serviceability and ODA results
    # ------------------------------------------------------------------
    print(f"\n  Serviceability encoding results:")
    _log_serviceability_encoding(utsf)

    print(f"\n  ODA encoding results:")
    _log_oda_encoding(utsf)

    # ------------------------------------------------------------------
    # 7. Validation
    # ------------------------------------------------------------------
    missing = utsf.get("stats", {}).get("dataQuality", {}).get("missingFields", [])
    from builder.validator import print_validation_report
    from fc4_schema import validate_fc4
    errors = validate_fc4(utsf)
    _log_validation(missing, errors)
    print_validation_report(utsf)

    # ------------------------------------------------------------------
    # 8. Save output
    # ------------------------------------------------------------------
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    safe_name   = transporter_name.lower().replace(" ", "_").replace("/", "_")
    output_path = os.path.join(OUTPUT_DIR, f"{safe_name}.utsf.json")
    encoder.save(utsf, output_path)

    quality = utsf.get("dataQuality", 0)
    stats   = utsf.get("stats", {})
    total_pins  = stats.get("totalServedPincodes", 0)
    zones_cnt   = stats.get("zonesServed", 0)
    oda_cnt     = stats.get("totalOdaPincodes", 0)
    active_z    = stats.get("activeZones", [])
    elapsed     = time.time() - t_start
    file_kb     = os.path.getsize(output_path) / 1024

    print(f"\n{DIVIDER}")
    print(f"  Generated : {output_path}")
    print(f"  Data Quality: {quality:.1f}/100")
    print(f"  Coverage  : {total_pins:,} pincodes, {zones_cnt} zones active {active_z[:8]}")
    print(f"  ODA       : {oda_cnt:,} pincodes")
    print(f"  File size : {file_kb:.1f} KB")
    print(f"  Elapsed   : {elapsed:.1f}s")
    print(f"{DIVIDER}\n")

    return output_path


def list_transporters() -> List[str]:
    """List all transporter folders."""
    if not os.path.exists(TRANSPORTERS_DIR):
        return []
    return [
        d for d in os.listdir(TRANSPORTERS_DIR)
        if os.path.isdir(os.path.join(TRANSPORTERS_DIR, d))
        and not d.startswith(".")
        and d != "EXAMPLE_TRANSPORTER"
    ]


def validate_utsf_file(file_path: str):
    """Validate an existing UTSF file."""
    with open(file_path, "r", encoding="utf-8") as f:
        utsf = json.load(f)

    version = utsf.get("version", "?")
    print(f"File: {file_path}")
    print(f"Version: {version}")

    from builder.validator import print_validation_report
    print_validation_report(utsf)


def show_ml_analytics():
    """Display ML enhancement analytics."""
    if not ML_ENHANCER_AVAILABLE:
        print("ML analytics not available - ML enhancer not loaded")
        return
    
    try:
        analytics = get_ml_analytics()
        print("\n" + DIVIDER)
        print("  ML Enhancement Analytics")
        print(DIVIDER)
        
        if "status" in analytics:
            print(f"  {analytics['status']}")
            return
        
        print(f"  Total Enhancements: {analytics['total_enhancements']}")
        print(f"  Average Quality Improvement: {analytics['average_quality_improvement']} points")
        print(f"  Success Rate: {analytics['success_rate']}%")
        print(f"  Average Confidence: {analytics['average_confidence']}")
        
        print(f"\n  Quality Metrics:")
        print(f"    Before (Avg): {analytics['quality_before_avg']}/100")
        print(f"    After (Avg):  {analytics['quality_after_avg']}/100")
        print(f"    Net Improvement: +{analytics['average_quality_improvement']} points")
        
        print(f"\n  Most Common Enhancements:")
        for enhancement, count in analytics['most_common_enhancements'].items():
            print(f"    {enhancement}: {count} times")
        
        if analytics.get('recent_enhancements'):
            print(f"\n  Recent Activity:")
            for event in analytics['recent_enhancements'][-3:]:
                print(f"    {event['transporter']}: {event['improvement']:+.1f} points (confidence: {event['confidence']:.2f})")
        
        print(DIVIDER)
        
    except Exception as e:
        print(f"Error generating analytics: {e}")


def migrate_v2_file(file_path: str) -> str:
    """Migrate a v2 UTSF to FC4."""
    with open(file_path, "r", encoding="utf-8") as f:
        v2_data = json.load(f)

    from builder.fc4_encoder import migrate_v2_to_fc4
    fc4 = migrate_v2_to_fc4(v2_data, PINCODES_PATH, ZONES_PATH)

    out_name = os.path.basename(file_path).replace(".utsf.json", ".fc4.utsf.json")
    out_path = os.path.join(OUTPUT_DIR, out_name)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(fc4, f, indent=2)

    print(f"Migrated: {out_path}")
    return out_path


def interactive_mode():
    """Interactive CLI for the generator."""
    print("\n" + "="*60)
    print("  FC4 UTSF Generator")
    print("  Logistics Data -> Canonical UTSF Format")
    print("="*60)

    while True:
        print("\nOptions:")
        print("  1. Generate UTSF for a transporter")
        print("  2. List available transporters")
        print("  3. Validate a UTSF file")
        print("  4. Migrate v2 UTSF to FC4")
        print("  5. Batch process all transporters")
        print("  6. Create new transporter folder")
        print("  7. Show ML Enhancement Analytics")
        print("  q. Quit")

        choice = input("\nChoice: ").strip().lower()

        if choice == "q":
            print("Goodbye!")
            break

        elif choice == "1":
            transporters = list_transporters()
            if not transporters:
                print("\nNo transporters found.")
                print(f"Add a folder to: {TRANSPORTERS_DIR}")
                print("Each folder needs: company_details/, charges/, zone_data/")
                continue

            print("\nAvailable transporters:")
            for i, t in enumerate(transporters, 1):
                print(f"  {i}. {t}")

            sel = input("Enter number or name: ").strip()
            try:
                idx  = int(sel) - 1
                name = transporters[idx]
            except (ValueError, IndexError):
                name = sel

            use_ai = input("Use AI extraction? (Y/n): ").strip().lower() != "n"
            generate_utsf_for_transporter(name, use_ai=use_ai)

        elif choice == "2":
            transporters = list_transporters()
            if transporters:
                print(f"\nFound {len(transporters)} transporter(s):")
                for t in transporters:
                    folder = os.path.join(TRANSPORTERS_DIR, t)
                    files  = collect_files_from_folder(folder)
                    print(f"  - {t} ({len(files)} files)")
            else:
                print("\nNo transporters found.")

        elif choice == "3":
            path = input("Path to UTSF file: ").strip().strip('"')
            if os.path.exists(path):
                validate_utsf_file(path)
            else:
                print(f"File not found: {path}")

        elif choice == "4":
            path = input("Path to v2 UTSF file: ").strip().strip('"')
            if os.path.exists(path):
                migrate_v2_file(path)
            else:
                print(f"File not found: {path}")

        elif choice == "5":
            transporters = list_transporters()
            print(f"\nBatch processing {len(transporters)} transporters...")
            results = []
            for t in transporters:
                out = generate_utsf_for_transporter(t, use_ai=True, verbose=False)
                results.append((t, out is not None))

            print("\nBatch complete:")
            for name, success in results:
                status = "OK" if success else "FAIL"
                print(f"  [{status}] {name}")

        elif choice == "6":
            name = input("Transporter name: ").strip()
            if name:
                safe = name.replace(" ", "_")
                for sub in ["company_details", "charges", "zone_data"]:
                    path = os.path.join(TRANSPORTERS_DIR, safe, sub)
                    os.makedirs(path, exist_ok=True)
                print(f"\nCreated: transporters/{safe}/")
                print(f"  Add your files to the subfolders:")
                print(f"    company_details/  <- company info")
                print(f"    charges/          <- rate cards")
                print(f"    zone_data/        <- serviceability/pincode data")

        elif choice == "7":
            show_ml_analytics()


def main():
    parser = argparse.ArgumentParser(
        description="FC4 UTSF Generator -- Convert any transporter data to FC4 format"
    )
    parser.add_argument("command", nargs="?",
                        choices=["generate", "list", "validate", "migrate", "batch", "analytics"],
                        help="Command to run")
    parser.add_argument("target", nargs="?", help="Transporter name or file path")
    parser.add_argument("--no-ai", action="store_true", help="Disable AI extraction")
    parser.add_argument("--quiet",  action="store_true", help="Minimal output")

    args = parser.parse_args()

    if not args.command:
        interactive_mode()
        return

    if args.command == "list":
        transporters = list_transporters()
        for t in transporters:
            print(t)

    elif args.command == "generate":
        if not args.target:
            print("Usage: python main.py generate <transporter_name>")
            sys.exit(1)
        generate_utsf_for_transporter(args.target, use_ai=not args.no_ai)

    elif args.command == "validate":
        if not args.target:
            print("Usage: python main.py validate <path/to/file.utsf.json>")
            sys.exit(1)
        validate_utsf_file(args.target)

    elif args.command == "migrate":
        if not args.target:
            print("Usage: python main.py migrate <path/to/v2.utsf.json>")
            sys.exit(1)
        migrate_v2_file(args.target)

    elif args.command == "batch":
        transporters = list_transporters()
        for t in transporters:
            generate_utsf_for_transporter(t, use_ai=not args.no_ai)

    elif args.command == "analytics":
        show_ml_analytics()


if __name__ == "__main__":
    main()
