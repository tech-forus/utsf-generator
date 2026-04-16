@echo off
setlocal EnableDelayedExpansion
title UTSF Generator v9
cd /d "%~dp0"

echo.
echo  ============================================================
echo   UTSF Generator v9  ^|  Transporter Data to UTSF v2.1
echo  ============================================================
echo.

:: ── Check Python ─────────────────────────────────────────────────────────────
python --version >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Python not found.
    echo.
    echo  Please install Python 3.9 or newer from:
    echo    https://www.python.org/downloads/
    echo.
    echo  During install, tick "Add Python to PATH" !
    echo.
    pause
    exit /b 1
)

:: ── First-time setup ─────────────────────────────────────────────────────────
if not exist "setup_done.flag" (
    echo  First run detected — running setup ^(this takes ~1 minute^)...
    echo.
    python setup.py
    if errorlevel 1 (
        echo.
        echo  [ERROR] Setup failed. See errors above.
        echo  Fix them and double-click RUN_WEB.bat again.
        pause
        exit /b 1
    )
    echo.
)

:: ── Launch app ────────────────────────────────────────────────────────────────
echo  Launching web interface at http://localhost:5000
echo  ^(Close this window to stop the server^)
echo.
python run.py
if errorlevel 1 (
    echo.
    echo  [ERROR] App failed to start. See errors above.
    pause
)
