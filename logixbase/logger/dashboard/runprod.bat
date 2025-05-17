@echo off
setlocal enabledelayedexpansion

REM === 设定端口 ===
set PORT=10000

REM === 获取本机局域网 IP（仅取第一个非 127 开头的） ===
for /f "tokens=2 delims=:" %%f in ('ipconfig ^| findstr /C:"IPv4"') do (
    set ip=%%f
    set ip=!ip: =!
    if not "!ip!"=="127.0.0.1" (
        set LOCAL_IP=!ip!
        goto :found_ip
    )
)
:found_ip

REM === 确保进入项目根目录 ===
set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"

REM === 检查构建是否存在 ===
if not exist "front\dist\index.html" (
    echo ❌ [ERROR] 未找到前端构建文件 front/dist/index.html
    echo 👉 请先执行前端构建：
    echo     cd front
    echo     npm install
    echo     npm run build
    pause
    exit /b 1
)

REM === 启动 FastAPI 服务 ===
echo ✅ 启动日志服务中（监听地址：0.0.0.0:%PORT%）...
echo ----------------------------------------------------------
echo 🖥️  本机访问地址： http://localhost:%PORT%
echo 🌐  局域网访问地址： http://%LOCAL_IP%:%PORT%
echo 🔁  API 示例地址： http://localhost:%PORT%/api/projects
echo ----------------------------------------------------------

REM === 自动打开浏览器访问本机地址 ===
start "" http://localhost:%PORT%

REM === 运行 FastAPI（监听所有 IP）===
python main.py --host 0.0.0.0 --port %PORT%

pause
