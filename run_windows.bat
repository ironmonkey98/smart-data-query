@echo off
chcp 65001 >nul
:: ── smart-data-query Web 服务启动脚本（Windows）──────────────────────────

set PROJECT_DIR=%~dp0
cd /d "%PROJECT_DIR%"

:: ── 1. 检查 Python ─────────────────────────────────────────────────────────
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] 未找到 python，请先安装 Python 3.9+
    pause
    exit /b 1
)

:: ── 2. 安装依赖 ────────────────────────────────────────────────────────────
echo [INFO] 检查/安装依赖...
python -m pip install -q -r requirements-server.txt

:: ── 3. 检查 ANTHROPIC_AUTH_TOKEN ───────────────────────────────────────────
if exist ".env" (
    for /f "usebackq tokens=1,* delims==" %%A in (".env") do (
        if not "%%A"=="" if not "%%A:~0,1%"=="#" set "%%A=%%B"
    )
)

if "%ANTHROPIC_AUTH_TOKEN%"=="" if "%ANTHROPIC_API_KEY%"=="" (
    echo [WARN] 未设置 ANTHROPIC_AUTH_TOKEN，对话功能将不可用
    echo        请在 .env 文件中设置 ANTHROPIC_AUTH_TOKEN=sk-...
)

:: ── 4. 检查停车场数据库 ────────────────────────────────────────────────────
if not exist "data\sample_parking_ops.db" (
    echo [INFO] 未找到停车场数据库，正在从 Excel 重建...
    python scripts\build_parking_ops_from_excels.py
) else (
    echo [INFO] 停车场数据库就绪
)

:: ── 5. 清理端口占用（8000）────────────────────────────────────────────────
set PORT=8000
for /f "tokens=5" %%P in ('netstat -ano ^| findstr ":%PORT% " ^| findstr "LISTENING"') do (
    echo [INFO] 端口 %PORT% 被占用，正在释放...
    taskkill /PID %%P /F >nul 2>&1
)

:: ── 6. 启动服务 ────────────────────────────────────────────────────────────
echo.
echo   启动 smart-data-query Web 服务
echo   地址：http://localhost:%PORT%
echo   按 Ctrl+C 停止
echo.

python "%PROJECT_DIR%server.py"
pause
