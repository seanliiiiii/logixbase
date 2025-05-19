@echo off
setlocal enabledelayedexpansion
set PYTHONUTF8=1

:: === Step 1: Get version from get_version.py ===
FOR /F "delims=" %%F IN ('python get_version.py') DO (
    SET "PROJECT_VERSION=%%F"
)
if /I "%PROJECT_VERSION%"=="VERSION_NOT_FOUND" (
    echo [ERROR] Failed to extract version.
    exit /b 1
)
echo [INFO] Project version detected: v%PROJECT_VERSION%

:: === Step 2: Git commit & push to master ===
echo [INFO] Committing changes to Git...
git add .
git commit -m "Auto build and release: v%PROJECT_VERSION%"
git push origin HEAD:master
if %errorlevel% neq 0 (
    echo [ERROR] Git push to master failed!
    exit /b %errorlevel%
)
echo.

:: === Step 3: Create version branch (vX.Y.Z) ===
echo [INFO] Creating version branch v%PROJECT_VERSION%...
git branch v%PROJECT_VERSION% 2>nul
git checkout v%PROJECT_VERSION%
git push origin v%PROJECT_VERSION%
if %errorlevel% neq 0 (
    echo [WARNING] Failed to push version branch. It may already exist.
)
git checkout master
echo.

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
echo [SUCCESS] Completed: Git pushed to master and version branch, and uploaded to PyPI (if new).
pause
endlocal
