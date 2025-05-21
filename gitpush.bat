@echo off
setlocal EnableDelayedExpansion
chcp 65001>nul

REM ------------------------------------------------------------------------
REM gitpush.bat —— 一键提交 & 推送（确保本地改动不丢失、不被覆盖）
REM ------------------------------------------------------------------------

REM 1. 进入脚本所在目录
pushd "%~dp0"
set "SCRIPT_DIR=%CD%"
echo [INFO] Script directory: %SCRIPT_DIR%

REM 2. 获取项目名（当前文件夹名）
for %%F in ("%SCRIPT_DIR%") do set "PROJECT_NAME=%%~nF"
echo [INFO] Project name: %PROJECT_NAME%

REM 3. 从 pyproject.toml 中提取 version
set "VER="
for /f "tokens=2 delims== " %%V in ('findstr /R "^version *= *\"[0-9]\+\.[0-9]\+\.[0-9]\+\"" pyproject.toml') do (
    set "VER_RAW=%%~V"
)
set "VER=!VER_RAW:"=!"
if "!VER!"=="" (
    echo [ERROR] 无法从 pyproject.toml 提取版本号！
    popd & pause & exit /b 1
)
echo [INFO] Version: !VER!

REM 4. 自动识别主分支名称（master 或 main）
set "PRIMARY=master"
git rev-parse --verify master >nul 2>&1 || set "PRIMARY=main"
echo [INFO] Primary branch: %PRIMARY%

REM 5. 记录当前开发分支
for /f "tokens=*" %%B in ('git rev-parse --abbrev-ref HEAD') do set "CURRENT=%%B"
echo [INFO] Current branch: %CURRENT%

REM 6. **一次性提交所有改动**（respect .gitignore）
echo [INFO] Staging ALL changes (add -A, respects .gitignore)...
git add -A

echo [INFO] Committing local changes...
git commit -m "chore(release): auto-commit all changes for v!VER!" || echo [INFO] No changes to commit.

REM 到此，工作区已干净，不会再丢失本地改动

REM 7. 切换到主分支并拉取最新
echo [INFO] Checking out %PRIMARY%...
git checkout %PRIMARY%  || (echo [ERROR] Checkout %PRIMARY% failed! & popd & pause & exit /b 1)

echo [INFO] Pulling origin/%PRIMARY%...
git pull origin %PRIMARY% || (echo [ERROR] Pull failed! & popd & pause & exit /b 1)

REM 8. 合并当前分支到主分支
if /I NOT "%CURRENT%"=="%PRIMARY%" (
    echo [INFO] Merging %CURRENT% into %PRIMARY%...
    git merge "%CURRENT%" || (
        echo [ERROR] Merge conflict! 请手动解决后重试。
        popd & pause & exit /b 1
    )
)

REM 9. 推送主分支
echo [INFO] Pushing %PRIMARY%...
git push origin %PRIMARY% || (
    echo [ERROR] Push %PRIMARY% 失败! & popd & pause & exit /b 1
)

REM 10. 创建或重建版本分支 v<VER>
set "TAGBR=v!VER!"
git branch --list %TAGBR% >nul 2>&1 && (
    echo [INFO] Deleting existing local branch %TAGBR%...
    git branch -D %TAGBR%
)
echo [INFO] Creating branch %TAGBR% from %PRIMARY%...
git checkout -b %TAGBR% || (
    echo [ERROR] 无法创建分支 %TAGBR%! & popd & pause & exit /b 1
)

echo [INFO] Pushing branch %TAGBR%...
git push -u origin %TAGBR% || (
    echo [ERROR] Push %TAGBR% 失败! & popd & pause & exit /b 1
)

REM 11. 切回原来开发分支
echo [INFO] Returning to branch %CURRENT%...
git checkout "%CURRENT%" || (
    echo [ERROR] Checkout %CURRENT% 失败! & popd & pause & exit /b 1
)

popd
echo [INFO] 完成！
pause
endlocal
