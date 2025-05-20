```batch
@echo off
setlocal EnableDelayedExpansion
chcp 65001>nul

REM ------------------------------------------------------------------------
REM Release Script: version-based branch creation and Git merge
REM Save this file as "release.bat" (NOT git.bat) in your repository root.
REM It must not shadow the 'git' command. Double-click to run.
REM Assumes:
REM   • Git repo is at this level (script parent folder).
REM   • Project package is in subfolder named same as this folder.
REM Requirements: Windows, Git.exe in PATH.
REM ------------------------------------------------------------------------

REM --- Change to script directory ---
pushd "%~dp0"
set "SCRIPT_DIR=%CD%"
echo Script directory: %SCRIPT_DIR%

REM --- Derive project/package folder name ---
for %%F in ("%SCRIPT_DIR%") do set "PROJECT_NAME=%%~nF"
echo Project name: %PROJECT_NAME%

REM --- Locate __init__.py and extract __version__ ---
set "PACKAGE_DIR=%SCRIPT_DIR%\%PROJECT_NAME%"
set "INIT_FILE=%PACKAGE_DIR%\__init__.py"
if not exist "%INIT_FILE%" (
    echo ERROR: __init__.py not found in %PACKAGE_DIR%
    popd
    pause
    exit /b 1
)
for /f "tokens=2 delims==" %%V in ('findstr /R "__version__ *= *" "%INIT_FILE%"') do set "VER_RAW=%%V"
rem Strip surrounding quotes (double and single) and spaces
set "VER=!VER_RAW:"=!"
set "VER=!VER:'=!"
set "VER=!VER: =!"
echo Version: !VER!

REM --- Git operations using git.exe to avoid recursion ---
echo Committing local changes...
git.exe add .
git.exe commit -m "Release version !VER!" || echo No changes to commit.

echo Determining current branch...
for /f "tokens=*" %%B in ('git.exe rev-parse --abbrev-ref HEAD') do set "CURRENT=%%B"
echo Current branch: !CURRENT!

echo Switching to master...
git.exe checkout master

echo Pulling latest master...
git.exe pull origin master

echo Merging !CURRENT! into master...
if /I NOT "!CURRENT!"=="master" (
    git.exe merge "!CURRENT!" || echo Merge skipped.
)

echo Pushing master to origin...
git.exe push origin master

echo Creating and pushing version branch v!VER!...
git.exe checkout -b v!VER!
git.exe push -u origin v!VER!

popd
echo Done.
pause
endlocal
```
