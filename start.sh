#!/usr/bin/env bash
# ── smart-data-query Web 服务启动脚本 ─────────────────────────────────────
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

# ── 1. 检查 Python ─────────────────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
  echo "[ERROR] 未找到 python3，请先安装 Python 3.9+"
  exit 1
fi

# ── 2. 安装依赖 ────────────────────────────────────────────────────────────
echo "[INFO] 检查/安装依赖..."
python3 -m pip install -q -r requirements-server.txt --break-system-packages 2>/dev/null \
  || python3 -m pip install -q -r requirements-server.txt

# ── 3. 检查 ANTHROPIC_AUTH_TOKEN ───────────────────────────────────────────
#    优先读 .env，其次读环境变量
if [ -f "$PROJECT_DIR/.env" ]; then
  export $(grep -v '^#' "$PROJECT_DIR/.env" | xargs) 2>/dev/null || true
fi

if [ -z "$ANTHROPIC_AUTH_TOKEN" ] && [ -z "$ANTHROPIC_API_KEY" ]; then
  echo "[WARN] 未设置 ANTHROPIC_AUTH_TOKEN，对话功能将不可用"
  echo "       请在 .env 文件中设置，或执行："
  echo "       export ANTHROPIC_AUTH_TOKEN=sk-..."
fi

# ── 4. 生成模拟数据（首次运行）────────────────────────────────────────────
SALES_FILE="$PROJECT_DIR/data/sample_sales.csv"
SALES_LINES=$(wc -l < "$SALES_FILE" 2>/dev/null || echo 0)
if [ "$SALES_LINES" -lt 100 ]; then
  echo "[INFO] 检测到数据量不足，重新生成模拟数据..."
  python3 "$PROJECT_DIR/data/gen_mock_data.py"
else
  echo "[INFO] 数据文件就绪（sales: ${SALES_LINES} 行）"
fi

# ── 5. 清理上次运行的端口占用 ──────────────────────────────────────────────
PORT=${PORT:-8000}
OCCUPIED=$(lsof -ti:"$PORT" 2>/dev/null || true)
if [ -n "$OCCUPIED" ]; then
  echo "[INFO] 端口 $PORT 被占用，正在释放..."
  kill -9 $OCCUPIED 2>/dev/null || true
  sleep 1
fi

# ── 6. 启动服务 ────────────────────────────────────────────────────────────
echo ""
echo "  启动 smart-data-query Web 服务"
echo "  地址：http://localhost:$PORT"
echo "  按 Ctrl+C 停止"
echo ""

exec python3 "$PROJECT_DIR/server.py"
