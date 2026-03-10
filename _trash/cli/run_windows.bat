@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul

set "PROJECT_DIR=%~dp0"
cd /d "%PROJECT_DIR%"

echo [INFO] smart-data-query Windows 启动脚本
echo [INFO] 项目目录: %PROJECT_DIR%

set "PYTHON_CMD="
where py >nul 2>nul
if %errorlevel%==0 (
    set "PYTHON_CMD=py -3"
) else (
    where python >nul 2>nul
    if %errorlevel%==0 (
        set "PYTHON_CMD=python"
    )
)

if "%PYTHON_CMD%"=="" (
    echo [ERROR] 未找到 Python。请先安装 Python 3.10+ 并勾选 Add Python to PATH。
    pause
    exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
    echo [INFO] 首次运行，正在创建虚拟环境...
    call %PYTHON_CMD% -m venv ".venv"
    if errorlevel 1 (
        echo [ERROR] 创建虚拟环境失败。
        pause
        exit /b 1
    )
)

set "VENV_PYTHON=%PROJECT_DIR%.venv\Scripts\python.exe"

echo [INFO] 安装/检查依赖...
call "%VENV_PYTHON%" -m pip install --upgrade pip >nul
call "%VENV_PYTHON%" -m pip install -r "requirements-server.txt" pandas openpyxl
if errorlevel 1 (
    echo [ERROR] 依赖安装失败。
    pause
    exit /b 1
)

if not exist ".env" (
    echo [WARN] 未检测到 .env 文件。
    echo [WARN] 如需完整 AI 对话能力，请按 README_WINDOWS.md 创建 .env。
)

if not exist "data\sample_parking_ops.db" (
    if exist "data\5个停车场数据\车场基础数据.xlsx" (
        echo [INFO] 未找到 data\sample_parking_ops.db，正在根据 Excel 重建停车数据库...
        call "%VENV_PYTHON%" "scripts\build_parking_ops_from_excels.py"
        if errorlevel 1 (
            echo [ERROR] 从 Excel 构建 sample_parking_ops.db 失败。
            pause
            exit /b 1
        )
    ) else (
        echo [ERROR] 未找到 data\sample_parking_ops.db，也未找到原始 Excel 数据目录。
        echo [ERROR] 请确认打包内容完整。
        pause
        exit /b 1
    )
)

echo [INFO] 启动 Web 服务...
echo [INFO] 浏览器地址: http://127.0.0.1:8000
start "" "http://127.0.0.1:8000"
call "%VENV_PYTHON%" -m uvicorn server:app --host 127.0.0.1 --port 8000

endlocal
