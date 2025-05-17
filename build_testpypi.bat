@echo off
set PYTHONUTF8=1

echo Cleaning old build files for TestPyPI...
if exist dist rmdir /s /q dist
if exist build rmdir /s /q build
for /d %%i in (*.egg-info) do (
    rmdir /s /q "%%i"
)
echo.

echo Building the project for TestPyPI...
python setup.py sdist bdist_wheel
if %errorlevel% neq 0 (
    echo Build for TestPyPI failed!
    exit /b %errorlevel%
)
echo.

echo Uploading to TestPyPI...
echo IMPORTANT:
echo When prompted for "Enter your username:", type: __token__
echo When prompted for "Enter your password:", paste your TestPyPI API token (including the 'pypi-' prefix).
echo ---
twine upload --verbose --repository-url https://test.pypi.org/legacy/ dist/*
if %errorlevel% neq 0 (
    echo TestPyPI upload failed! See verbose output above.
    exit /b %errorlevel%
)
echo.

echo Successfully built and uploaded to TestPyPI.
echo You can check your package at: https://test.pypi.org/project/logixbase/ 