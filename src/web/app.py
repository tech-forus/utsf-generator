"""
FC4 UTSF Generator — Web UI
Flask app serving the web interface on localhost:5000
"""

import os
import sys
import json
import subprocess
import tempfile
from flask import (
    Flask, render_template, request, redirect, url_for,
    Response, stream_with_context, jsonify, send_file, abort
)
from werkzeug.utils import secure_filename
from flask_cors import CORS

# ─── Path setup ───────────────────────────────────────────────────────────────
WEB_DIR       = os.path.dirname(os.path.abspath(__file__))
SRC_DIR       = os.path.dirname(WEB_DIR)
ROOT_DIR      = os.path.dirname(SRC_DIR)
MAIN_PY       = os.path.join(SRC_DIR, "main.py")
TRANSPORTERS  = os.path.join(ROOT_DIR, "transporters")
OUTPUT_DIR    = os.path.join(ROOT_DIR, "output")
DATA_DIR      = os.path.join(ROOT_DIR, "data")
# Writable knowledge dir — overridden by configure_paths() in frozen EXE mode
# so that learned_dict.py / learning_data.json are stored next to the EXE,
# not inside the read-only PyInstaller temp bundle.
KNOWLEDGE_DIR = os.path.join(SRC_DIR, "knowledge")

sys.path.insert(0, SRC_DIR)

# ─── Railway volume override ──────────────────────────────────────────────────
# When UTSF_ROOT is set (Railway deployment with a persistent volume),
# mutable dirs (transporters, output, knowledge) point there so files
# survive redeploys.  Read-only reference data (pincodes, zones) stays
# bundled inside the image.
_UTSF_ROOT = os.environ.get("UTSF_ROOT")
if _UTSF_ROOT:
    TRANSPORTERS  = os.path.join(_UTSF_ROOT, "transporters")
    OUTPUT_DIR    = os.path.join(_UTSF_ROOT, "output")
    KNOWLEDGE_DIR = os.path.join(_UTSF_ROOT, "knowledge")
    os.makedirs(TRANSPORTERS,  exist_ok=True)
    os.makedirs(OUTPUT_DIR,    exist_ok=True)
    os.makedirs(KNOWLEDGE_DIR, exist_ok=True)


def configure_paths(root_dir: str, bundle_dir: str = None):
    """Override path constants — called by launcher when running as EXE."""
    global TRANSPORTERS, OUTPUT_DIR, DATA_DIR, MAIN_PY, KNOWLEDGE_DIR, _ID_COUNTER_FILE
    TRANSPORTERS  = os.path.join(root_dir, "transporters")
    OUTPUT_DIR    = os.path.join(root_dir, "output")
    DATA_DIR      = bundle_dir or os.path.join(root_dir, "data")
    MAIN_PY       = os.path.join(bundle_dir or root_dir, "main.py") if bundle_dir else MAIN_PY
    # Mutable knowledge files (learned_dict.py, learning_data.json) live in
    # ROOT_DIR/knowledge/ so they persist between EXE sessions.
    KNOWLEDGE_DIR = os.path.join(root_dir, "knowledge")
    _ID_COUNTER_FILE = os.path.join(TRANSPORTERS, ".id_counter")
    os.makedirs(TRANSPORTERS, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(KNOWLEDGE_DIR, exist_ok=True)

app = Flask(__name__, template_folder="templates")
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB

# ─── CORS ─────────────────────────────────────────────────────────────────────────────
# Allows browser requests from freightcompare.ai (Vercel frontend).
# Override via UTSF_ALLOWED_ORIGIN env var if the domain ever changes.
# Locally (no env var set) defaults to * so standalone use still works.
_ALLOWED_ORIGIN = os.environ.get("UTSF_ALLOWED_ORIGIN", "*")
CORS(app, origins=[_ALLOWED_ORIGIN], supports_credentials=False)

# ─── API key guard ─────────────────────────────────────────────────────────────────────
# Protects all endpoints when deployed on Railway.
# Set UTSF_API_KEY env var; Node backend sends it as:  X-API-Key: <value>
# If env var is absent (local standalone use) the guard is skipped entirely.
_API_KEY = os.environ.get("UTSF_API_KEY")

@app.before_request
def _check_api_key():
    if not _API_KEY:
        return  # local mode -- no key configured, allow everything
    if request.method == "OPTIONS":
        return  # let CORS preflight through
    if request.path == "/api/status":
        return  # health check endpoint — always public
    if request.headers.get("X-API-Key") != _API_KEY:
        return jsonify({"error": "Unauthorized"}), 401

# Track active generation runs to prevent duplicate starts
_active_generations: set = set()

# ─── Version ──────────────────────────────────────────────────────────────────
APP_VERSION      = "v9.0"
APP_BUILD_DATE   = "2026-04-10"
OICR_ENGINE      = "OICR v9.0"
UTSF_SCHEMA      = "UTSF v2.1"

# Inject version into every template context
@app.context_processor
def inject_version():
    return {
        "app_version":    APP_VERSION,
        "app_build_date": APP_BUILD_DATE,
        "oicr_engine":    OICR_ENGINE,
        "utsf_schema":    UTSF_SCHEMA,
    }

# ─── Constants ────────────────────────────────────────────────────────────────
SUBFOLDERS = ["company_details", "charges", "zone_data"]
ALLOWED_EXT = {".xlsx", ".xls", ".csv", ".tsv",
               ".docx", ".doc", ".pptx", ".ppt",
               ".pdf", ".json",
               ".png", ".jpg", ".jpeg", ".tiff", ".bmp", ".webp"}
SUBFOLDER_ICONS = {
    "company_details": "🏢",
    "charges": "💰",
    "zone_data": "🗺️"
}
SUBFOLDER_HINTS = {
    "company_details": "Company name, GST, PAN, contact info",
    "charges": "Rate cards, docket charges, fuel %, ODA",
    "zone_data": "Zone price matrix, pincode lists, serviceability"
}

# ─── Auto-sort keywords ──────────────────────────────────────────────────────
# Each subfolder has filename keywords (higher = stronger signal)
SORT_KEYWORDS = {
    "company_details": [
        "company", "vendor", "transporter", "profile", "info", "details",
        "contact", "gst", "pan", "cin", "registration", "address", "about",
        "overview", "kyc", "onboard",
    ],
    "charges": [
        "rate", "rates", "charge", "charges", "price", "pricing", "tariff",
        "fuel", "docket", "oda", "rov", "insurance", "handling", "freight",
        "surcharge", "fee", "fees", "cost", "invoice", "billing",
    ],
    "zone_data": [
        "zone", "zones", "pincode", "pincodes", "serviceability", "service",
        "coverage", "matrix", "area", "delivery", "served", "network",
        "lane", "lanes", "route", "region", "reach",
    ],
}

def auto_classify_file(filename: str) -> str:
    """
    Auto-classify a file into company_details / charges / zone_data
    based on filename keywords. Returns the best-match subfolder.
    """
    name_lower = os.path.splitext(filename)[0].lower()
    # Replace common separators with spaces
    name_clean = name_lower.replace("_", " ").replace("-", " ").replace(".", " ")
    words = set(name_clean.split())

    scores = {sub: 0 for sub in SUBFOLDERS}
    for sub, keywords in SORT_KEYWORDS.items():
        for kw in keywords:
            if kw in name_clean:
                # Exact substring match
                scores[sub] += 2
            for w in words:
                if kw in w or w in kw:
                    scores[sub] += 1

    best = max(scores, key=lambda s: scores[s])
    # If no signal at all, use file extension as tiebreaker
    if scores[best] == 0:
        ext = os.path.splitext(filename)[1].lower()
        if ext in (".png", ".jpg", ".jpeg"):
            return "company_details"  # Logo / stamp photos
        if ext in (".pptx", ".ppt"):
            return "charges"   # Presentation = rate card deck
        return "charges"  # Most common default

    return best


# ─── Helpers ──────────────────────────────────────────────────────────────────

def allowed_file(filename: str) -> bool:
    return os.path.splitext(filename)[1].lower() in ALLOWED_EXT


def get_transporter_files(name: str) -> dict:
    base = os.path.join(TRANSPORTERS, name)
    result = {}
    for sub in SUBFOLDERS:
        path = os.path.join(base, sub)
        if os.path.isdir(path):
            files = []
            for fname in sorted(os.listdir(path)):
                if fname.startswith(".") or fname.startswith("~"):
                    continue
                fpath = os.path.join(path, fname)
                size = os.path.getsize(fpath)
                ext = os.path.splitext(fname)[1].lower()
                files.append({
                    "name": fname,
                    "size": _human_size(size),
                    "ext": ext.lstrip(".")
                })
            result[sub] = files
        else:
            result[sub] = []
    return result


def get_output_meta(name: str) -> dict | None:
    safe = _safe_name(name)
    path = os.path.join(OUTPUT_DIR, f"{safe}.utsf.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            utsf = json.load(f)
        stats = utsf.get("stats", {})
        company = utsf.get("company", utsf.get("meta", {}))
        return {
            "quality": utsf.get("dataQuality", 0),
            "pincodes": stats.get("totalServedPincodes", stats.get("totalPincodes", 0)),
            "zones": stats.get("zonesServed", 0),
            "company_name": company.get("name") or company.get("companyName") or name,
            "generated_at": utsf.get("generatedAt", ""),
            "format": utsf.get("format", f"v{utsf.get('version','?')}"),
            "active_zones": stats.get("activeZones", []),
        }
    except Exception:
        return None


def list_transporters() -> list[str]:
    if not os.path.exists(TRANSPORTERS):
        return []
    return sorted([
        d for d in os.listdir(TRANSPORTERS)
        if os.path.isdir(os.path.join(TRANSPORTERS, d))
        and not d.startswith(".")
        and d != "EXAMPLE_TRANSPORTER"
    ])


def _safe_name(name: str) -> str:
    return name.lower().replace(" ", "_").replace("/", "_").replace("\\", "_")


# ─── Transporter ID system ────────────────────────────────────────────────────
_ID_COUNTER_FILE = None   # set by configure_paths

def _get_id_counter_file():
    global _ID_COUNTER_FILE
    if _ID_COUNTER_FILE is None:
        _ID_COUNTER_FILE = os.path.join(TRANSPORTERS, ".id_counter")
    return _ID_COUNTER_FILE


def _next_transporter_id() -> str:
    """Atomically increment and return the next transporter ID like TRP-00042."""
    counter_file = _get_id_counter_file()
    os.makedirs(TRANSPORTERS, exist_ok=True)
    try:
        n = int(open(counter_file).read().strip()) if os.path.exists(counter_file) else 0
    except (ValueError, OSError):
        n = 0
    n += 1
    with open(counter_file, "w") as f:
        f.write(str(n))
    return f"TRP-{n:05d}"


def get_transporter_id(name: str) -> str:
    """Return the stored ID for a transporter, or empty string if not set."""
    id_file = os.path.join(TRANSPORTERS, name, ".transporter_id")
    try:
        return open(id_file).read().strip()
    except OSError:
        return ""


def _human_size(size: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.0f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def _quality_color(score: float) -> str:
    if score >= 75: return "green"
    if score >= 50: return "yellow"
    return "red"


# ─── Routes: Dashboard ────────────────────────────────────────────────────────

@app.get("/")
def dashboard():
    names = list_transporters()
    transporters = []
    for name in names:
        files = get_transporter_files(name)
        counts = {sub: len(files[sub]) for sub in SUBFOLDERS}
        meta = get_output_meta(name)
        transporters.append({
            "name": name,
            "tid": get_transporter_id(name),
            "counts": counts,
            "total_files": sum(counts.values()),
            "output": meta,
            "is_generating": name in _active_generations,
        })
    return render_template("dashboard.html", transporters=transporters)


@app.post("/transporter/create")
def create_transporter():
    name = request.form.get("name", "").strip()
    if not name:
        return redirect(url_for("dashboard"))
    safe = _safe_name(name).replace(" ", "_")
    folder = os.path.join(TRANSPORTERS, safe)
    for sub in SUBFOLDERS:
        os.makedirs(os.path.join(folder, sub), exist_ok=True)
    # Assign a unique ID if not already assigned
    id_file = os.path.join(folder, ".transporter_id")
    if not os.path.exists(id_file):
        tid = _next_transporter_id()
        with open(id_file, "w") as f:
            f.write(tid)
    return redirect(url_for("transporter_detail", name=safe))


# ─── Routes: Transporter Detail ───────────────────────────────────────────────

@app.get("/transporter/<name>")
def transporter_detail(name: str):
    folder = os.path.join(TRANSPORTERS, name)
    if not os.path.isdir(folder):
        abort(404)
    files = get_transporter_files(name)
    meta = get_output_meta(name)
    is_generating = name in _active_generations
    tid = get_transporter_id(name)
    return render_template(
        "transporter.html",
        name=name,
        tid=tid,
        files=files,
        subfolders=SUBFOLDERS,
        subfolder_icons=SUBFOLDER_ICONS,
        subfolder_hints=SUBFOLDER_HINTS,
        output=meta,
        is_generating=is_generating,
    )


@app.post("/transporter/<name>/upload")
def upload_files(name: str):
    """
    Smart upload: auto-classifies files into the right subfolder.
    Optional: pass subfolder= to override auto-classification.
    Returns sorted result so UI can show where each file landed.
    """
    override_subfolder = request.form.get("subfolder")

    saved = []    # [{name, subfolder, original_name}]
    skipped = []  # [filename]

    for f in request.files.getlist("files"):
        if not f.filename:
            continue
        if not allowed_file(f.filename):
            skipped.append(f.filename)
            continue

        filename = secure_filename(f.filename)
        if override_subfolder and override_subfolder in SUBFOLDERS:
            subfolder = override_subfolder
        else:
            subfolder = auto_classify_file(f.filename)

        dest_dir = os.path.join(TRANSPORTERS, name, subfolder)
        os.makedirs(dest_dir, exist_ok=True)
        f.save(os.path.join(dest_dir, filename))
        saved.append({"name": filename, "subfolder": subfolder})

    return jsonify({"ok": True, "saved": saved, "skipped": skipped})


@app.delete("/transporter/<name>/file")
def delete_file(name: str):
    data = request.get_json() or {}
    subfolder = data.get("subfolder")
    filename = data.get("filename")
    if subfolder not in SUBFOLDERS or not filename:
        return jsonify({"ok": False, "error": "Bad request"}), 400
    path = os.path.join(TRANSPORTERS, name, subfolder, secure_filename(filename))
    if os.path.isfile(path):
        os.remove(path)
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "File not found"}), 404


@app.post("/transporter/<name>/move-file")
def move_file(name: str):
    """Atomically move a file between subfolders (used by drag-to-move UI)."""
    data = request.get_json() or {}
    from_sub = data.get("from")
    to_sub = data.get("to")
    filename = data.get("filename")
    if from_sub not in SUBFOLDERS or to_sub not in SUBFOLDERS or not filename:
        return jsonify({"ok": False, "error": "Bad request"}), 400
    safe_fn = secure_filename(filename)
    src = os.path.join(TRANSPORTERS, name, from_sub, safe_fn)
    dst_dir = os.path.join(TRANSPORTERS, name, to_sub)
    dst = os.path.join(dst_dir, safe_fn)
    if not os.path.isfile(src):
        return jsonify({"ok": False, "error": "Source file not found"}), 404
    os.makedirs(dst_dir, exist_ok=True)
    os.replace(src, dst)  # atomic on same filesystem
    return jsonify({"ok": True, "from": from_sub, "to": to_sub})


# ─── Routes: Generation (SSE) ─────────────────────────────────────────────────

@app.get("/transporter/<name>/generate")
def generate_stream(name: str):
    if name in _active_generations:
        def already():
            yield f"data: {json.dumps('[WARN] Already generating for ' + name)}\n\n"
            yield f"data: {json.dumps({'__done__': True, 'exitCode': 1})}\n\n"
        return Response(stream_with_context(already()), mimetype="text/event-stream")

    def event_stream():
        _active_generations.add(name)
        exit_code = 0
        try:
            IS_FROZEN = getattr(sys, "frozen", False)

            if IS_FROZEN:
                # Frozen EXE: sys.executable IS the EXE — spawning it as a subprocess
                # just starts another web server and ignores all arguments.
                # Run generation in-process on a background thread instead.
                import queue as _queue
                import threading as _threading

                q = _queue.Queue()

                class _Writer:
                    encoding = "utf-8"
                    errors   = "replace"
                    def write(self, s):
                        for line in s.splitlines():
                            if line.strip():
                                q.put(line)
                    def flush(self): pass

                def _run():
                    writer = _Writer()
                    old_out, old_err = sys.stdout, sys.stderr
                    sys.stdout = writer
                    sys.stderr = writer
                    try:
                        import main as _main
                        _main.generate_utsf_for_transporter(name, use_ai=True)
                    except Exception as exc:
                        import traceback as _tb
                        q.put(f"[ERROR] {exc}")
                        for ln in _tb.format_exc().splitlines():
                            if ln.strip():
                                q.put(f"  {ln}")
                    finally:
                        sys.stdout = old_out
                        sys.stderr = old_err
                        q.put(None)  # sentinel — generation done

                _threading.Thread(target=_run, daemon=True).start()

                while True:
                    try:
                        item = q.get(timeout=300)
                    except Exception:
                        break
                    if item is None:
                        break
                    yield f"data: {json.dumps(item)}\n\n"

            else:
                # Normal Python: subprocess approach (stdout line-by-line → SSE)
                cmd = [sys.executable, "-u", MAIN_PY, "generate", name]
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    cwd=ROOT_DIR,
                    bufsize=1,
                    env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"},
                )
                for line in iter(proc.stdout.readline, ""):
                    payload = line.rstrip("\r\n")
                    if payload:
                        yield f"data: {json.dumps(payload)}\n\n"
                proc.wait()
                exit_code = proc.returncode

        except Exception as e:
            yield f"data: {json.dumps(f'[ERROR] {e}')}\n\n"
            exit_code = 1
        finally:
            _active_generations.discard(name)

        yield f"data: {json.dumps({'__done__': True, 'exitCode': exit_code})}\n\n"

    return Response(
        stream_with_context(event_stream()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ─── Routes: Output Viewer ────────────────────────────────────────────────────

@app.get("/output/<name>")
def view_output(name: str):
    safe = _safe_name(name)
    path = os.path.join(OUTPUT_DIR, f"{safe}.utsf.json")
    if not os.path.exists(path):
        # Try exact name too
        path2 = os.path.join(OUTPUT_DIR, f"{name}.utsf.json")
        if os.path.exists(path2):
            path = path2
        else:
            abort(404)

    with open(path, "r", encoding="utf-8") as f:
        utsf = json.load(f)

    from builder.validator import full_validate
    is_valid, errors, warnings = full_validate(utsf)

    company = utsf.get("company", utsf.get("meta", {}))
    pricing = utsf.get("pricing", {})
    zm = pricing.get("zoneMatrix") or pricing.get("zoneRates") or {}
    stats = utsf.get("stats", {})
    svc = utsf.get("serviceability", {})

    # Build sorted zone list for matrix header
    matrix_zones = sorted(zm.keys()) if zm else []

    # Charges summary list for badges (supports both v2.1 priceRate and FC4 direct keys)
    pr = pricing.get("priceRate", {})
    enabled_charges = []
    _charge_map = [
        ("FUEL",        pr.get("fuel") or pricing.get("fuel")),
        ("ROV",         pr.get("rovCharges") or pricing.get("rov")),
        ("INSURANCE",   pr.get("insuranceCharges") or pricing.get("insurance")),
        ("ODA",         pr.get("odaCharges") or pricing.get("oda")),
        ("HANDLING",    pr.get("handlingCharges") or pricing.get("handling")),
        ("FM",          pr.get("fmCharges") or pricing.get("fm")),
        ("APPOINTMENT", pr.get("appointmentCharges") or pricing.get("appointment")),
        ("COD",         pr.get("codCharges") or pricing.get("cod")),
        ("TOPAY",       pr.get("topayCharges") or pricing.get("topay")),
        ("PREPAID",     pr.get("prepaidCharges") or pricing.get("prepaid")),
        ("DOD",         pr.get("dodCharges") or pricing.get("dod")),
    ]
    for label, val in _charge_map:
        if val is None:
            continue
        if isinstance(val, (int, float)) and float(val) > 0:
            enabled_charges.append(label)
        elif isinstance(val, dict):
            if val.get("enabled") or val.get("v", 0) != 0 or val.get("f", 0) != 0 \
               or val.get("bands") or val.get("matrix") or val.get("type"):
                enabled_charges.append(label)

    # Parse audit: uncertain matches for review (dedupe by raw+matched)
    raw_audit = utsf.get("_parseAudit", [])
    seen = set()
    parse_audit = []
    for entry in raw_audit:
        key = (entry.get("type"), entry.get("raw"), str(entry.get("matched")))
        if key not in seen:
            seen.add(key)
            parse_audit.append(entry)

    return render_template(
        "viewer.html",
        name=name,
        utsf=utsf,
        company=company,
        pricing=pricing,
        stats=stats,
        svc=svc,
        zone_matrix=zm,
        matrix_zones=matrix_zones,
        is_valid=is_valid,
        errors=errors,
        warnings=warnings,
        enabled_charges=enabled_charges,
        quality_color=_quality_color(utsf.get("dataQuality", 0)),
        parse_audit=parse_audit,
    )


@app.get("/output/<name>/download")
def download_output(name: str):
    safe = _safe_name(name)
    path = os.path.join(OUTPUT_DIR, f"{safe}.utsf.json")
    if not os.path.exists(path):
        abort(404)
    return send_file(path, as_attachment=True,
                     download_name=f"{safe}.utsf.json",
                     mimetype="application/json")


@app.delete("/output/<name>")
def delete_output(name: str):
    """Delete a generated UTSF file."""
    safe = _safe_name(name)
    path = os.path.join(OUTPUT_DIR, f"{safe}.utsf.json")
    if not os.path.exists(path):
        # Try exact name too
        path2 = os.path.join(OUTPUT_DIR, f"{name}.utsf.json")
        if os.path.exists(path2):
            path = path2
        else:
            return jsonify({"ok": False, "error": "File not found"}), 404
    try:
        os.remove(path)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.delete("/transporter/<name>")
def delete_transporter(name: str):
    """Delete a transporter folder and all its contents."""
    safe = _safe_name(name)
    # Security: don't allow deleting outside TRANSPORTERS
    path = os.path.join(TRANSPORTERS, safe)
    
    if not os.path.exists(path):
        return jsonify({"ok": False, "error": "Transporter not found"}), 404
    
    try:
        import shutil
        shutil.rmtree(path)
        # Also try to delete associated UTSF
        utsf_path = os.path.join(OUTPUT_DIR, f"{safe}.utsf.json")
        if os.path.exists(utsf_path):
            os.remove(utsf_path)
            
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ─── Routes: Migrate ──────────────────────────────────────────────────────────

@app.get("/migrate")
def migrate_page():
    return render_template("migrate.html")


@app.post("/migrate")
def migrate_upload():
    f = request.files.get("file")
    if not f or not f.filename:
        return render_template("migrate.html", error="No file uploaded")
    if not f.filename.endswith(".json"):
        return render_template("migrate.html", error="Please upload a .json UTSF v2 file")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(suffix="_v2.utsf.json", dir=OUTPUT_DIR)
        os.close(fd)
        f.save(tmp_path)

        from main import migrate_v2_file
        out_path = migrate_v2_file(tmp_path)
        out_name = os.path.basename(out_path).replace(".fc4.utsf.json", "").replace(".utsf.json", "")
        return redirect(url_for("view_output", name=out_name))
    except Exception as e:
        return render_template("migrate.html", error=str(e))
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


# ─── Routes: Learn / Refine ──────────────────────────────────────────────────

@app.post("/api/learn")
def api_learn():
    """
    User confirms or corrects a field mapping.
    Uses the ML dictionary engine: tracks frequency, auto-promotes
    high-confidence corrections to learned_dict.py after N confirmations
    (like phone autocorrect learning from repeated use).

    Body (confirm):  {"type": "charge"|"zone", "raw": "...", "canonical": "..."}
    Body (correct):  {"type": "charge"|"zone", "raw": "...", "canonical": "...",
                      "wrong_canonical": "..."}   ← optional, for explicit corrections
    """
    data = request.get_json() or {}
    learn_type      = data.get("type")
    raw             = (data.get("raw") or "").strip()
    canonical       = data.get("canonical")
    wrong_canonical = data.get("wrong_canonical")  # set when user corrects a bad match

    if not learn_type or not raw or canonical is None:
        return jsonify({"ok": False, "error": "type, raw, canonical required"}), 400

    try:
        sys.path.insert(0, SRC_DIR)
        from knowledge.ml_dictionary_engine import record_confirmation, record_correction

        if wrong_canonical is not None:
            result = record_correction(learn_type, raw, wrong_canonical, canonical)
        else:
            result = record_confirmation(learn_type, raw, canonical)

        return jsonify({"ok": True, "result": result})
    except Exception as e:
        # Fallback to direct write if engine fails
        try:
            _save_learned_entry(learn_type, raw, canonical)
        except Exception:
            pass
        return jsonify({"ok": True, "fallback": True, "error": str(e)})


def _save_learned_entry(learn_type: str, raw: str, canonical):
    """
    Direct write fallback — atomically inserts into learned_dict.py.
    Uses KNOWLEDGE_DIR so it works in both dev mode and frozen EXE mode.
    """
    import re as _re2

    learned_path = os.path.join(KNOWLEDGE_DIR, "learned_dict.py")
    # In frozen EXE mode the writable learned_dict.py may not exist yet — create it
    if not os.path.exists(learned_path):
        try:
            os.makedirs(KNOWLEDGE_DIR, exist_ok=True)
            with open(learned_path, "w", encoding="utf-8") as _f:
                _f.write(
                    '"""User-learned corrections (auto-managed)."""\n\n'
                    'LEARNED_CHARGES = {}\n\nLEARNED_ZONES = {}\n'
                )
        except OSError:
            return

    with open(learned_path, "r", encoding="utf-8") as f:
        content = f.read()

    if learn_type == "charge":
        key   = raw.lower()
        entry = f'    {key!r}: {canonical!r},'
        target_dict = "LEARNED_CHARGES"
    elif learn_type == "zone":
        key   = raw.upper()
        zones = canonical if isinstance(canonical, list) else [canonical]
        entry = f'    {key!r}: {zones!r},'
        target_dict = "LEARNED_ZONES"
    else:
        return

    m = _re2.search(rf'({_re2.escape(target_dict)}\s*=\s*\{{)(.*?)(\}}\s*\n)',
                    content, _re2.DOTALL)
    if m:
        new_content = content[:m.start(3)] + "\n" + entry + "\n" + content[m.start(3):]
    else:
        new_content = content + f"\n# learned\n{target_dict} = {{}}\n{target_dict}.update({{{entry}}})\n"

    tmp = learned_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(new_content)
    os.replace(tmp, learned_path)
    print(f"[Learn] Saved {learn_type}: {raw!r} → {canonical!r}")


@app.get("/api/learn-stats")
def api_learn_stats():
    """Return ML dictionary learning statistics."""
    try:
        from knowledge.ml_dictionary_engine import get_stats
        return jsonify({"ok": True, "stats": get_stats()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ─── Routes: Compare ──────────────────────────────────────────────────────────

@app.get("/compare")
def compare_page():
    names = list_transporters()
    outputs = []
    for name in names:
        meta = get_output_meta(name)
        if meta:
            outputs.append({"name": name, **meta})
    return render_template("compare.html", outputs=outputs)


@app.get("/api/compare")
def api_compare():
    """
    Compare two UTSF files.
    Query params: a=<name>&b=<name>
    Returns a structured diff.
    """
    a_name = request.args.get("a", "")
    b_name = request.args.get("b", "")

    def load(name):
        safe = _safe_name(name)
        path = os.path.join(OUTPUT_DIR, f"{safe}.utsf.json")
        if not os.path.exists(path):
            path = os.path.join(OUTPUT_DIR, f"{name}.utsf.json")
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    a = load(a_name)
    b = load(b_name)
    if not a or not b:
        return jsonify({"ok": False, "error": "One or both files not found"}), 404

    diff = _build_utsf_diff(a, b)
    return jsonify({"ok": True, "diff": diff, "a_name": a_name, "b_name": b_name})


def _build_utsf_diff(a: dict, b: dict) -> dict:
    """Build a structured diff between two UTSFs."""
    diff = {
        "quality":    {"a": a.get("dataQuality", 0), "b": b.get("dataQuality", 0)},
        "zones":      {},
        "charges":    {},
        "matrix":     {},
        "summary":    [],
    }

    # ── Quality delta ──────────────────────────────────────────────────────────
    qa, qb = diff["quality"]["a"], diff["quality"]["b"]
    if qa != qb:
        diff["summary"].append(
            f"Quality: {qa:.0f} → {qb:.0f} ({'+' if qb>qa else ''}{qb-qa:.0f} pts)"
        )

    # ── Serviceability zones ───────────────────────────────────────────────────
    svc_a = a.get("serviceability", {})
    svc_b = b.get("serviceability", {})
    all_zones_seen = sorted(set(list(svc_a.keys()) + list(svc_b.keys())))

    for z in all_zones_seen:
        za = svc_a.get(z, {})
        zb = svc_b.get(z, {})
        mode_a = za.get("mode", "NOT_SERVED")
        mode_b = zb.get("mode", "NOT_SERVED")
        count_a = za.get("servedCount", 0)
        count_b = zb.get("servedCount", 0)
        cov_a = za.get("coveragePercent", 0)
        cov_b = zb.get("coveragePercent", 0)

        diff["zones"][z] = {
            "mode_a": mode_a, "mode_b": mode_b,
            "count_a": count_a, "count_b": count_b,
            "cov_a": round(cov_a, 1), "cov_b": round(cov_b, 1),
            "mode_changed": mode_a != mode_b,
            "count_delta": count_b - count_a,
        }

    # ── Zone matrix rates ──────────────────────────────────────────────────────
    def get_matrix(utsf):
        p = utsf.get("pricing", {})
        return p.get("zoneMatrix") or p.get("zoneRates") or {}

    zm_a, zm_b = get_matrix(a), get_matrix(b)
    all_origins = sorted(set(list(zm_a.keys()) + list(zm_b.keys())))

    for orig in all_origins:
        dests_a = zm_a.get(orig, {})
        dests_b = zm_b.get(orig, {})
        all_dests = sorted(set(list(dests_a.keys()) + list(dests_b.keys())))
        for dest in all_dests:
            rate_a = dests_a.get(dest)
            rate_b = dests_b.get(dest)
            key = f"{orig}→{dest}"
            if rate_a != rate_b:
                diff["matrix"][key] = {
                    "a": rate_a, "b": rate_b,
                    "delta": round((rate_b or 0) - (rate_a or 0), 2),
                }

    # ── Charges ────────────────────────────────────────────────────────────────
    def get_pr(utsf):
        p = utsf.get("pricing", {})
        return p.get("priceRate") or p.get("base") or {}

    pr_a, pr_b = get_pr(a), get_pr(b)

    def get_fuel(utsf):
        p = utsf.get("pricing", {})
        f = p.get("fuel")
        if isinstance(f, dict): return f.get("value", 0)
        pr = p.get("priceRate", {})
        return pr.get("fuel", 0) if pr else 0

    charge_fields = [
        ("fuel",          get_fuel(a), get_fuel(b)),
        ("docketCharges", pr_a.get("docketCharges"), pr_b.get("docketCharges")),
        ("minCharges",    pr_a.get("minCharges"),    pr_b.get("minCharges")),
        ("minWeight",     pr_a.get("minWeight"),      pr_b.get("minWeight")),
        ("greenTax",      pr_a.get("greenTax"),       pr_b.get("greenTax")),
    ]
    for field, va, vb in charge_fields:
        if va != vb:
            diff["charges"][field] = {"a": va, "b": vb}

    # ── Summary ───────────────────────────────────────────────────────────────
    zones_added   = [z for z, d in diff["zones"].items()
                     if d["mode_a"] == "NOT_SERVED" and d["mode_b"] != "NOT_SERVED"]
    zones_removed = [z for z, d in diff["zones"].items()
                     if d["mode_a"] != "NOT_SERVED" and d["mode_b"] == "NOT_SERVED"]
    if zones_added:   diff["summary"].append(f"Zones added: {zones_added}")
    if zones_removed: diff["summary"].append(f"Zones removed: {zones_removed}")
    if diff["matrix"]: diff["summary"].append(f"{len(diff['matrix'])} rate changes")
    if diff["charges"]: diff["summary"].append(f"{len(diff['charges'])} charge changes")
    if not diff["summary"]: diff["summary"].append("No significant differences found")

    return diff


# ─── Routes: Price Extraction ─────────────────────────────────────────────────

@app.post("/api/extract-prices")
def api_extract_prices():
    """
    Extract zone rate matrix from an uploaded price sheet.
    Accepts any file format: Excel, PDF, image, Word, CSV.
    Returns: { zoneRates: { originZone: { destZone: rate } }, confidence: 0-100, source: str }
    Called by the AddVendor zone price matrix step when the user uploads a rate card.
    """
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"error": "No file uploaded"}), 400

    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in ALLOWED_EXT:
        return jsonify({"error": f"Unsupported file type: {ext}"}), 400

    import tempfile

    tmp_path = None
    try:
        # Save to a temp file so parsers can read it
        suffix = ext if ext else ".bin"
        fd, tmp_path = tempfile.mkstemp(suffix=suffix)
        os.close(fd)
        f.save(tmp_path)

        # ── Run through our parser stack ──────────────────────────────────────
        sys.path.insert(0, SRC_DIR)

        zone_rates = {}
        confidence = 0
        parse_source = "unknown"
        text_for_ai = ""

        if ext in (".xlsx", ".xls", ".csv", ".tsv"):
            # Excel/CSV: ExcelParser._auto_detect() extracts zone matrices directly
            from parsers.excel_parser import ExcelParser
            parser = ExcelParser()
            result = parser.parse(tmp_path)
            zone_matrix = result.get("data", {}).get("zone_matrix") or {}
            if zone_matrix:
                zone_rates = zone_matrix
                confidence = 80
            text_for_ai = result.get("text", "")
            parse_source = "excel"

        elif ext == ".pdf":
            from parsers.pdf_parser import PDFParser
            parser = PDFParser()
            result = parser.parse(tmp_path)
            text_for_ai = result.get("text", "")
            parse_source = "pdf"

        elif ext in (".png", ".jpg", ".jpeg", ".tiff", ".bmp", ".webp"):
            from parsers.image_parser import ImageParser
            parser = ImageParser()
            result = parser.parse(tmp_path)
            text_for_ai = result.get("text", "")
            parse_source = "image_ocr"

        elif ext in (".docx", ".doc"):
            from parsers.word_parser import WordParser
            parser = WordParser()
            result = parser.parse(tmp_path)
            text_for_ai = result.get("text", "")
            parse_source = "word"

        else:
            return jsonify({"error": f"Parser not available for {ext}"}), 422

        # ── AI fallback: use Ollama to extract zone matrix from text ──────────
        if not zone_rates and text_for_ai:
            try:
                from intelligence.ollama_client import OllamaClient
                client = OllamaClient()
                if client.is_available():
                    ai_result = client.extract_zone_matrix(text_for_ai[:6000])
                    if ai_result and isinstance(ai_result, dict):
                        zone_rates = ai_result
                        confidence = 45
            except Exception as ai_err:
                print(f"[extract-prices] AI fallback failed: {ai_err}")

        return jsonify({
            "zoneRates": zone_rates,
            "confidence": confidence,
            "source": parse_source,
            "zonesFound": len(zone_rates),
            "message": (
                f"Extracted {len(zone_rates)} origin zones from {parse_source}"
                if zone_rates else
                "Could not extract zone rates from this file — try a cleaner Excel rate card"
            ),
        })

    except Exception as exc:
        import traceback
        print(f"[extract-prices] Error: {exc}\n{traceback.format_exc()}")
        return jsonify({"error": str(exc), "zoneRates": {}, "confidence": 0}), 500
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


# ─── Routes: API ──────────────────────────────────────────────────────────────

@app.get("/api/status")
def api_status():
    try:
        from intelligence.ollama_client import get_available_models
        models = get_available_models()
        ollama_ok = len(models) > 0
    except Exception:
        models = []
        ollama_ok = False

    transporter_count = len(list_transporters())
    output_count = 0
    if os.path.isdir(OUTPUT_DIR):
        output_count = len([f for f in os.listdir(OUTPUT_DIR) if f.endswith(".utsf.json")])

    return jsonify({
        "ollama":        ollama_ok,
        "models":        models,
        "transporters":  transporter_count,
        "outputs":       output_count,
        "active":        list(_active_generations),
        "app_version":   APP_VERSION,
        "oicr_engine":   OICR_ENGINE,
        "utsf_schema":   UTSF_SCHEMA,
    })


# ─── Routes: Input Data ───────────────────────────────────────────────────────

def _get_input_data_dir() -> str:
    """Return the 'input data' folder next to the transporters/ folder."""
    return os.path.join(ROOT_DIR, "input data")


@app.get("/input-data")
def input_data_list():
    """List all transporter folders found in the 'input data' directory."""
    input_dir = _get_input_data_dir()
    folders = []
    if os.path.isdir(input_dir):
        for name in sorted(os.listdir(input_dir)):
            full = os.path.join(input_dir, name)
            if not os.path.isdir(full) or name.startswith("."):
                continue
            files = [
                f for f in os.listdir(full)
                if not f.startswith(".") and not f.startswith("~")
            ]
            folders.append({"name": name, "files": files, "count": len(files)})
    return jsonify({"ok": True, "folders": folders, "input_dir": input_dir})


@app.post("/input-data/import/<folder_name>")
def import_input_data(folder_name: str):
    """
    Import files from 'input data/<folder_name>' into the transporters/ structure.
    Auto-classifies each file into company_details / charges / zone_data.
    Creates the transporter folder if needed, then user can generate UTSF.
    """
    import shutil

    input_dir = _get_input_data_dir()
    src_folder = os.path.join(input_dir, folder_name)
    if not os.path.isdir(src_folder):
        return jsonify({"ok": False, "error": f"Folder not found: {folder_name}"}), 404

    safe = _safe_name(folder_name)
    dest_base = os.path.join(TRANSPORTERS, safe)
    for sub in SUBFOLDERS:
        os.makedirs(os.path.join(dest_base, sub), exist_ok=True)

    imported = []
    skipped = []
    for fname in os.listdir(src_folder):
        if fname.startswith(".") or fname.startswith("~"):
            continue
        src_path = os.path.join(src_folder, fname)
        if not os.path.isfile(src_path):
            continue
        ext = os.path.splitext(fname)[1].lower()
        if ext not in ALLOWED_EXT:
            skipped.append(fname)
            continue

        subfolder = auto_classify_file(fname)
        dest_path = os.path.join(dest_base, subfolder, fname)
        shutil.copy2(src_path, dest_path)
        imported.append({"name": fname, "subfolder": subfolder})

    return jsonify({
        "ok":       True,
        "transporter": safe,
        "imported": imported,
        "skipped":  skipped,
    })


# ─── Template filters ─────────────────────────────────────────────────────────

@app.template_filter("quality_color")
def quality_color_filter(score):
    return _quality_color(float(score or 0))


@app.template_filter("mode_color")
def mode_color_filter(mode):
    colors = {
        "FULL_ZONE": "green",
        "EXCLUDING": "blue",
        "INCLUDING": "yellow",
        "NOT_SERVED": "gray",
        "FULL_MINUS_EXCEPT": "blue",
        "ONLY_SERVED": "yellow",
    }
    return colors.get(mode, "gray")


# ─── Entry point ──────────────────────────────────────────────────────────────

def _startup_check():
    """Print startup status to console."""
    print("\n" + "="*55)
    print(f"  UTSF Generator {APP_VERSION} — {APP_BUILD_DATE}")
    print(f"  Engine: {OICR_ENGINE}  Schema: {UTSF_SCHEMA}")
    print("="*55)

    # Data files
    for fname in ["pincodes.json", "zones_data.json"]:
        path = os.path.join(DATA_DIR, fname)
        if os.path.exists(path):
            size = os.path.getsize(path) // 1024
            print(f"  [OK] {fname} ({size} KB)")
        else:
            print(f"  [!!] MISSING: {fname} - generation will fail!")

    # Ollama
    try:
        from intelligence.ollama_client import get_available_models
        models = get_available_models()
        if models:
            print(f"  [OK] Ollama: {models[0]} + {len(models)-1} more")
        else:
            print("  [--] Ollama: running but no models installed")
            print("       Run: ollama pull qwen2.5-coder:3b")
    except Exception:
        print("  [--] Ollama: not running (AI extraction disabled)")

    # Optional packages
    for pkg, label in [("pdfplumber", "PDF"), ("pytesseract", "Image OCR"), ("openpyxl", "Excel")]:
        try:
            __import__(pkg)
            print(f"  [OK] {label} support")
        except ImportError:
            print(f"  [--] {label} support not available")

    transporters = list_transporters()
    print(f"\n  Transporters: {len(transporters)}")
    print(f"  URL: http://localhost:5000")
    print("="*50 + "\n")


if __name__ == "__main__":
    import webbrowser, threading
    _startup_check()
    threading.Timer(1.5, lambda: webbrowser.open("http://localhost:5000")).start()
    app.run(debug=False, host="127.0.0.1", port=5000, threaded=True)
