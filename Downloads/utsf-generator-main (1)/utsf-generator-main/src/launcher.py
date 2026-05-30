"""
UTSF Generator — Launcher
Starts the web UI server and opens the browser automatically.
Used as the entry point for the standalone EXE.
"""
import sys
import os
import threading
import webbrowser
import time
import socket

# ── Path resolution (handles both frozen EXE and normal Python) ───────────────

# When frozen by PyInstaller, _MEIPASS is the temp dir with bundled files
IS_FROZEN = getattr(sys, "frozen", False)

if IS_FROZEN:
    # Running as compiled EXE
    BUNDLE_DIR = sys._MEIPASS          # bundled src/ files live here
    ROOT_DIR   = os.path.dirname(sys.executable)  # EXE location = project root
else:
    # Running as normal Python script
    BUNDLE_DIR = os.path.dirname(os.path.abspath(__file__))
    ROOT_DIR   = os.path.dirname(BUNDLE_DIR)

# Add bundle dir to path so imports work
sys.path.insert(0, BUNDLE_DIR)

# Tell the web app where the project root is (transporters/, output/, data/)
os.environ["UTSF_ROOT"] = ROOT_DIR
os.environ["UTSF_DATA"] = os.path.join(BUNDLE_DIR, "data") if IS_FROZEN else os.path.join(ROOT_DIR, "data")

# ── Port finding ──────────────────────────────────────────────────────────────

def find_free_port(preferred: int = 5000) -> int:
    """Find an available port, starting from preferred."""
    for port in range(preferred, preferred + 20):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", port))
                return port
        except OSError:
            continue
    return preferred  # Fallback — Flask will error if this fails too

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    port = find_free_port(5000)
    url  = f"http://127.0.0.1:{port}"

    # ── 3. Fresh Start Management ─────────────────────────────────────────────
    # If a .fresh_start file exists, OR if --fresh is in sys.argv, clear transient data
    if "--fresh" in sys.argv or os.path.exists(os.path.join(ROOT_DIR, ".fresh_start")):
        print(f"  [Session] Starting afresh — clearing session data...")
        # Clear output
        out_dir = os.path.join(ROOT_DIR, "output")
        if os.path.isdir(out_dir):
            for f in os.listdir(out_dir):
                if f.endswith(".json"):
                    try: os.remove(os.path.join(out_dir, f))
                    except: pass
        # Clear 'randomly named' or test transporters
        trans_dir = os.path.join(ROOT_DIR, "transporters")
        if os.path.isdir(trans_dir):
            import shutil
            for d in os.listdir(trans_dir):
                # Don't delete canonical ones or company_details
                if d in ["tci_freight", "v_express", "delhivery", "ekart_logistics"]:
                    continue
                # If directory name looks like a hash or is very short/weird
                if len(d) > 8 or "." in d or "," in d:
                    try: shutil.rmtree(os.path.join(trans_dir, d))
                    except: pass
        
    # Force UTF-8 output
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    print(f"\n  UTSF Generator v9")
    print(f"  Starting server at {url}")
    print(f"  Your browser will open automatically.")
    print(f"  Close this window to stop the server.\n")

    # Open browser after a short delay (gives Flask time to start)
    def _open():
        time.sleep(1.8)
        webbrowser.open(url)

    threading.Thread(target=_open, daemon=True).start()

    # Import and run Flask app
    from web.app import app, configure_paths
    configure_paths(ROOT_DIR, BUNDLE_DIR)
    app.run(host="127.0.0.1", port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
