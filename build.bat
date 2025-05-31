@echo off
setlocal enabledelayedexpansion
set PYTHONUTF8=1

:: === Step 1: Get version from pyproject.toml ===
FOR /F "tokens=2 delims== " %%F IN ('findstr /r "^version *= *\"[0-9\.]\+\"" pyproject.toml') DO (
    SET "PROJECT_VERSION=%%~F"
)
:: 去除引号
SET "PROJECT_VERSION=%PROJECT_VERSION:"=%"

if /I "%PROJECT_VERSION%"=="" (
    echo [ERROR] Failed to extract version from pyproject.toml.
    exit /b 1
)
echo [INFO] Project version detected: v%PROJECT_VERSION%
echo [INFO] Project version detected: v%PROJECT_VERSION%

:: === Step 4: Check if version exists on PyPI ===
echo [INFO] Checking PyPI for existing version...
python -c "import requests, sys; resp = requests.get(f'https://pypi.org/pypi/logixbase/json'); versions = resp.json().get('releases', {}); sys.exit(0 if '%PROJECT_VERSION%' not in versions else 1)"
if %errorlevel% equ 1 (
    echo [INFO] Version v%PROJECT_VERSION% already exists on PyPI. Skipping build and upload.
    goto :end
)

:: === Step 5: Clean previous builds ===
echo [INFO] Cleaning old build files...
if exist dist rmdir /s /q dist
if exist build rmdir /s /q build
for /d %%i in (*.egg-info) do (
    rmdir /s /q "%%i"
)
echo.

:: === Step 6: Build project ===
echo [INFO] Building the project...
python -m build
if %errorlevel% neq 0 (
    echo [ERROR] Build failed!
    exit /b %errorlevel%
)
echo.

:: === Step 7: Upload to PyPI ===
echo [INFO] Uploading to PyPI...

twine upload dist/*
if %errorlevel% neq 0 (
    echo [ERROR] PyPI upload failed!
    exit /b %errorlevel%
)
echo.

:end
echo [SUCCESS] Completed: uploaded to PyPI (if new).
pause
endlocal
