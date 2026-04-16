#!/usr/bin/env python3
"""
UTSF Generator v9 — Web Launcher
==================================
Run this instead of the old EXE. Starts the full web interface.

  python run.py          → opens http://localhost:5000
  python run.py --port 8080  → custom port
"""

import sys
import os

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR  = os.path.join(ROOT_DIR, "src")

# Add src to Python path (so imports like "from web.app import ..." work)
sys.path.insert(0, SRC_DIR)

# Force UTF-8 on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Tell the app where data lives (same env vars as the old EXE)
os.environ["UTSF_ROOT"] = ROOT_DIR
os.environ["UTSF_DATA"] = os.path.join(ROOT_DIR, "data")

if __name__ == "__main__":
    from launcher import main
    main()
