@echo off

REM 获取脚本所在目录（dashboard 目录）
set SCRIPT_DIR=%~dp0

REM 切换到 dashboard 目录，确保相对路径正确
pushd "%SCRIPT_DIR%"

REM 启动后端 FastAPI 服务（使用 uvicorn，参数正确传递）
echo 启动后端服务中...
start cmd /k "cd /d %SCRIPT_DIR% && uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000"

REM 启动前端 Vite 服务
cd front
if not exist node_modules (
    echo 正在安装前端依赖...
    npm install
)
echo 启动前端服务中...
call npm run dev

popd
pause
