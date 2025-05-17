@echo off
set PYTHONUTF8=1
echo Cleaning old build files...
if exist dist rmdir /s /q dist
if exist build rmdir /s /q build
for /d %%i in (*.egg-info) do (
    rmdir /s /q "%%i"
)
echo.

echo Building the project...
python setup.py sdist bdist_wheel
if %errorlevel% neq 0 (
    echo Build failed!
    exit /b %errorlevel%
)
echo.

echo Uploading to PyPI...
twine upload dist/*
if %errorlevel% neq 0 (
    echo PyPI upload failed!
    exit /b %errorlevel%
)
echo.

echo Pushing to GitHub...
git add .
git commit -m "Auto build and publish"

echo Pushing to main branch...
git push git@github.com:seanliiiiii/logixbase.git HEAD:main
if %errorlevel% neq 0 (
    echo GitHub push to main branch failed!
    exit /b %errorlevel%
)
echo.

echo Getting project version...
FOR /F "tokens=* USEBACKQ" %%F IN (`python -c "import re; f=open('logixbase/__init__.py', 'r', encoding='utf-8'); print(re.search(r\"__version__\s*=\s*['\"]([^'\"]*)['\"]\", f.read()).group(1)); f.close()"`) DO (
    SET "PROJECT_VERSION=%%F"
)

if not defined PROJECT_VERSION (
    echo Failed to get project version.
    exit /b 1
)
echo Project version: %PROJECT_VERSION%
echo.

echo Creating and pushing version branch v%PROJECT_VERSION%...
git checkout -b v%PROJECT_VERSION%
if %errorlevel% neq 0 (
    echo Failed to create version branch v%PROJECT_VERSION%!
    echo It might already exist. Attempting to push existing local branch.
)
git push git@github.com:seanliiiiii/logixbase.git v%PROJECT_VERSION%
if %errorlevel% neq 0 (
    echo GitHub push to version branch v%PROJECT_VERSION% failed!
    exit /b %errorlevel%
)
echo.

echo Switching back to main branch...
git checkout main
echo.

echo Successfully built, published to PyPI, and pushed to GitHub (main and version branch v%PROJECT_VERSION%). 