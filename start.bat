@echo off
title PHANTOM v1.0

echo.
echo  ========================================
echo   PHANTOM v1.0 - OKX Trading Bot
echo  ========================================
echo.

REM Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed or not in PATH
    echo Install Python 3.10+ from https://python.org
    echo Make sure to check "Add Python to PATH" during installation
    pause
    exit /b 1
)
echo  [OK] Python found

REM Create venv if not exists
if not exist "venv\" (
    echo  [SETUP] Creating virtual environment...
    python -m venv venv
    if errorlevel 1 (
        echo [ERROR] Failed to create venv
        pause
        exit /b 1
    )
)

REM Activate venv
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo [ERROR] Failed to activate venv
    pause
    exit /b 1
)
echo  [OK] venv activated

REM Install dependencies
echo  [SETUP] Installing dependencies (may take 3-5 minutes first time)...
python -m pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet
echo  [OK] Dependencies ready

REM Check .env
if not exist ".env" (
    echo  [WARNING] .env file not found
)

echo.
echo  ========================================
echo   Starting PHANTOM...
echo   Dashboard: http://localhost:8000
echo   Health:    http://localhost:8000/api/health
echo  ========================================
echo.

REM Open browser after 5 seconds
start /b cmd /c "timeout /t 5 /nobreak >nul && start http://localhost:8000"

REM Run the bot
python bot_server.py

echo.
echo  Bot stopped. Press any key to close.
pause >nul
