@echo off
setlocal EnableDelayedExpansion
chcp 65001>nul

REM ------------------------------------------------------------------------
REM gitpush.bat —— 一键提交 & 推送（支持空远程仓库）
REM ------------------------------------------------------------------------

REM 1. 进入脚本目录
pushd "%~dp0"
echo [INFO] Script directory: %CD%
echo.

REM 2. 初始化 Git 仓库（如无 .git\HEAD）
if not exist ".git\HEAD" (
  echo [INFO] No Git repo found. git init...
  git init
  echo.
)

REM 3. 配置 origin（如未配置）
git config --get remote.origin.url >nul 2>&1
if errorlevel 1 (
  echo [INFO] No remote 'origin'. Reading repository from pyproject.toml...
  set "REPO_URL="
  for /f "tokens=3" %%R in ('findstr /R "^repository *= *\".*\"" pyproject.toml') do (
    set "REPO_URL=%%~R"
  )
  if "!REPO_URL!"=="" (
    echo [ERROR] Cannot find repository in pyproject.toml!
    popd & pause & exit /b 1
  )
  echo [INFO] git remote add origin !REPO_URL!
  git remote add origin "!REPO_URL!"
  echo.
)

REM 4. 记录当前分支
for /f "tokens=*" %%B in ('git rev-parse --abbrev-ref HEAD') do set "CURRENT=%%B"
echo [INFO] Current branch: !CURRENT!
echo.

REM 5. 识别主分支：master 或 main，均不存在则用 CURRENT
set "PRIMARY="
git rev-parse --verify master >nul 2>&1 && set "PRIMARY=master"
git rev-parse --verify main   >nul 2>&1 && set "PRIMARY=main"
if "!PRIMARY!"=="" set "PRIMARY=!CURRENT!"
echo [INFO] Primary branch: !PRIMARY!
echo.

REM 6. 取版本号
set "VER="
for /f "tokens=2 delims== " %%V in ('findstr /R "^version *= *\"[0-9]\+\.[0-9]\+\.[0-9]\+\"" pyproject.toml') do (
  set "VER_RAW=%%~V"
)
set "VER=!VER_RAW:"=!"
if "!VER!"=="" (
  echo [ERROR] Failed to extract version!
  popd & pause & exit /b 1
)
echo [INFO] Version: !VER!
echo.

REM 7. 提交所有本地改动
echo [INFO] git add -A...
git add -A
echo [INFO] git commit...
git commit -m "chore(release): auto-commit all changes for v!VER!" || echo [INFO] No changes to commit.
echo.

REM 8. 切到主分支
echo [INFO] git checkout !PRIMARY!...
git checkout !PRIMARY! || (echo [ERROR] Checkout !PRIMARY! failed & popd & pause & exit /b 1)
echo.

REM 9. 检查远程是否有主分支
set "HAS_REMOTE="
for /f %%H in ('git ls-remote --heads origin !PRIMARY!') do set "HAS_REMOTE=1"

if defined HAS_REMOTE (
  echo [INFO] Remote branch '!PRIMARY!' exists → pulling updates...
  git pull origin !PRIMARY! || (echo [ERROR] Pull failed & popd & pause & exit /b 1)

  REM 合并开发分支
  if /I not "!CURRENT!"=="!PRIMARY!" (
    echo [INFO] git merge !CURRENT!...
    git merge "!CURRENT!" || (echo [ERROR] Merge conflict & popd & pause & exit /b 1)
  )

  echo [INFO] git push origin !PRIMARY!...
  git push origin !PRIMARY! || (echo [ERROR] Push failed & popd & pause & exit /b 1)
) else (
  echo [INFO] Remote branch '!PRIMARY!' not found → initial push...
  echo [INFO] git push -u origin !PRIMARY!...
  git push -u origin !PRIMARY! || (echo [ERROR] Initial push failed & popd & pause & exit /b 1)
)
echo.

REM 10. 创建／重建版本分支 v<VER>
set "TAGBR=v!VER!"
git branch --list !TAGBR! >nul 2>&1 && (
  echo [INFO] Deleting local branch '!TAGBR!'...
  git branch -D !TAGBR!
)
echo [INFO] git checkout -b !TAGBR!...
git checkout -b !TAGBR! || (echo [ERROR] Create branch failed & popd & pause & exit /b 1)
echo [INFO] git push -u origin !TAGBR!...
git push -u origin !TAGBR! || (echo [ERROR] Push tag branch failed & popd & pause & exit /b 1)
echo.

REM 11. 切回开发分支
echo [INFO] git checkout !CURRENT!...
git checkout "!CURRENT!" || (echo [ERROR] Checkout !CURRENT! failed & popd & pause & exit /b 1)
echo.

popd
echo [INFO] 完成！
pause
endlocal
