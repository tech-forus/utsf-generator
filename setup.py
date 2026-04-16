"""
UTSF Generator v9 — First-Time Setup
=====================================
Run this once to install all dependencies and prepare the workspace.
Called automatically by RUN_WEB.bat on first launch.
"""
import sys
import os
import subprocess
import shutil

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
FLAG_FILE = os.path.join(ROOT_DIR, "setup_done.flag")


def run(cmd, label):
    print(f"  Installing {label}...", end=" ", flush=True)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print("OK")
        return True
    else:
        print("FAILED")
        print(result.stderr[-500:] if result.stderr else "(no output)")
        return False


def main():
    print("=" * 60)
    print("  UTSF Generator v9 — Setup")
    print("=" * 60)
    print()

    pip = [sys.executable, "-m", "pip", "install", "--quiet", "--upgrade"]

    packages = [
        (["flask"],                               "Flask (web server)"),
        (["pdfplumber"],                          "pdfplumber (PDF tables)"),
        (["PyPDF2"],                              "PyPDF2 (PDF fallback)"),
        (["pandas", "openpyxl", "xlrd"],          "Excel support"),
        (["python-docx"],                         "Word document support"),
        (["python-pptx"],                         "PowerPoint support"),
        (["pillow"],                              "Image processing"),
        (["pytesseract"],                         "OCR engine wrapper"),
        (["opencv-python-headless"],              "OpenCV (image OCR)"),
        (["numpy"],                               "NumPy"),
        (["werkzeug"],                            "Werkzeug"),
    ]

    failed = []
    for pkg_list, label in packages:
        ok = run(pip + pkg_list, label)
        if not ok:
            failed.append(label)

    print()

    # Check Tesseract OCR
    tesseract_ok = shutil.which("tesseract") is not None
    if tesseract_ok:
        print("  Tesseract OCR: Found — image PDFs will OCR correctly")
    else:
        print("  Tesseract OCR: NOT FOUND (image-heavy PDFs may extract less data)")
        print("  To enable: download from https://github.com/UB-Mannheim/tesseract/wiki")
        print("  and install to C:\\Program Files\\Tesseract-OCR\\")
        print("  (App works fine without it — text PDFs and Excel are fully supported)")

    print()

    # Create required folders
    for folder in ["transporters", "output", "knowledge"]:
        path = os.path.join(ROOT_DIR, folder)
        os.makedirs(path, exist_ok=True)

    if failed:
        print(f"  WARNING: {len(failed)} package(s) failed to install:")
        for f in failed:
            print(f"    - {f}")
        print("  The app will still work but some file types may be unsupported.")
        print()

    # Write flag file
    with open(FLAG_FILE, "w") as f:
        f.write("setup_done\n")

    print("  Setup complete!")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
