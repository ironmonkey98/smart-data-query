"""
FastAPI Web 服务：Agent 模式 —— Claude 自主写 SQL、分析推理、决策澄清。
工具集：list_data_sources / execute_sql / reflect / save_insight
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator

import anthropic
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

load_dotenv(dotenv_path=Path(__file__).parent / ".env")

SCRIPTS_DIR = Path(__file__).parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from connect_db import execute_raw_sql_on_sqlite, execute_raw_sql_on_csv  # noqa: E402

# ─── 路径常量 ─────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent

# 启动时读取 schema / glossary，注入 System Prompt
SCHEMA_TEXT   = (BASE_DIR / "references" / "db-schema.md").read_text(encoding="utf-8")
GLOSSARY_TEXT = (BASE_DIR / "references" / "term-glossary.md").read_text(encoding="utf-8")


def _query_data_date_range() -> str:
    """启动时查询数据库实际日期范围，返回注入 System Prompt 的说明文字。
    防止 Claude 用 date('now') 查到空结果后反复重试导致 429。"""
    try:
        db_path = str(BASE_DIR / "data" / "sample_parking_ops.db")
        rows = execute_raw_sql_on_sqlite(
            db_path,
            "SELECT date(MIN(paid_at)) AS min_date, date(MAX(paid_at)) AS max_date "
            "FROM parking_payment_records"
        )
        if rows and rows[0].get("max_date"):
            min_d = rows[0]["min_date"]
            max_d = rows[0]["max_date"]
            return (
                "\n\n## 数据库日期范围（重要）\n\n"
                "数据库中停车经营数据的实际日期范围为 " + min_d + " ~ " + max_d + "。\n"
                "- 禁止在 SQL 中使用 date('now') 或系统当前日期，当前日期已超出数据范围。\n"
                "- 当用户说今天/昨天/本周/本月/最近N天时，以 " + max_d + " 作为今天的基准推算日期。\n"
                "- 如果某个查询返回 0 行，优先考虑日期不在 " + min_d + "~" + max_d + " 范围内，"
                "直接告知用户并调整到有效范围，不要反复重试相似查询。"
            )
    except Exception:
        pass
    return ""


DATA_DATE_RANGE = _query_data_date_range()

# ─── 数据源配置 ──────────────────────────────────────────────────────────────
DATA_SOURCES: dict[str, dict] = {
    "parking_ops": {
        "source_type": "sqlite",
        "source_path": str(BASE_DIR / "data" / "sample_parking_ops.db"),
        "description": (
            "停车场经营数据（SQLite）。三张表：\n"
            "  • parking_lots (lot_id, parking_lot_name, total_spaces)\n"
            "  • parking_payment_records (payment_id, lot_id, initiated_at, paid_at, "
            "license_plate, entry_at, receivable_amount, actual_amount, payment_result, "
            "payment_method, refund_amount, payment_source, invoice_flag)\n"
            "  • parking_passage_records (passage_id, lot_id, license_plate, vehicle_type, "
            "entry_at, entry_gate, exit_at, exit_gate, stay_minutes, receivable_amount, "
            "actual_amount, notes)"
        ),
    },
}

# ─── Session 管理 ─────────────────────────────────────────────────────────────
@dataclass
class SessionData:
    messages: list[dict] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    last_active: float = field(default_factory=time.time)

sessions: dict[str, SessionData] = {}
SESSION_TTL_SECONDS = 2 * 3600


async def _cleanup_sessions_loop() -> None:
    while True:
        await asyncio.sleep(3600)
        now = time.time()
        expired = [sid for sid, s in sessions.items() if now - s.last_active > SESSION_TTL_SECONDS]
        for sid in expired:
            del sessions[sid]


# ─── Claude Tools 定义 ───────────────────────────────────────────────────────
TOOLS: list[dict] = [
    {
        "name": "list_data_sources",
        "description": "列出所有可用数据源及其表结构说明。当你不确定该用哪个数据源时调用。",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "execute_sql",
        "description": (
            "执行你编写的 SELECT SQL，返回查询结果。\n"
            "• parking_ops 数据源：SQLite，表名为 parking_lots / parking_payment_records / parking_passage_records\n"
            "只允许 SELECT 语句。结果最多返回 500 行给你分析。"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source_name": {
                    "type": "string",
                    "enum": list(DATA_SOURCES.keys()),
                    "description": "数据源名称",
                },
                "sql": {
                    "type": "string",
                    "description": "你编写的 SELECT SQL，结尾不需要分号",
                },
            },
            "required": ["source_name", "sql"],
        },
    },
    {
        "name": "reflect",
        "description": (
            "在执行复杂多步分析前，先输出分析计划。\n"
            "以下情况必须先调用 reflect：\n"
            "  \u2022 涉及 2 个以上数据源\n"
            "  \u2022 需要对比多个时间段（同比/环比）\n"
            "  \u2022 问题包含\u300c综合\u300d\u300c全面分析\u300d\u300c报告\u300d等词\n"
            "分析计划最多列 3 个查询步骤；每个步骤尽量一次 SQL 取全所需字段，"
            "不要把可以合并的查询拆成多条。"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "plan": {
                    "type": "string",
                    "description": "用中文列出本次分析步骤：查哪些数据源、用什么时间范围、对比逻辑",
                }
            },
            "required": ["plan"],
        },
    },
    {
        "name": "save_insight",
        "description": (
            "将重要分析发现保存到记忆，供后续对话引用。\n"
            "以下情况主动调用：\n"
            "  \u2022 发现明确数据异常（如支付失败率持续偏高）\n"
            "  \u2022 用户说\u300c记住这个\u300d\u300c下次提醒我\u300d\n"
            "  \u2022 连续追问同一主题后确认的结论"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "topic":   {"type": "string", "description": "主题标签，如 'B停车场支付失败'"},
                "insight": {"type": "string", "description": "核心发现，一两句话"},
            },
            "required": ["topic", "insight"],
        },
    },
]

# ─── System Prompt ────────────────────────────────────────────────────────────
SYSTEM_PROMPT = f"""你是智能数据分析助理，通过自主编写 SQL 查询数据，基于真实数据给出业务分析结论。

## 数据表结构（停车经营 SQLite）

{SCHEMA_TEXT}{DATA_DATE_RANGE}

## 业务术语对照

{GLOSSARY_TEXT}

> 当前系统只接入停车经营数据（parking_ops），不支持其他数据源。

## 澄清原则（最重要）

当用户问题存在以下任何一种情况，**必须直接用文字询问用户，不得调用工具，不得猜测**：
- 时间范围不明确（没有"最近N天/本周/本月/今年"等）
- 分析目标不明确（"看看情况"而没说看什么指标）
- 对象范围不明确（哪个车场/哪个区域/哪个产品）

澄清时直接说明需要哪些信息，格式简洁，例：
"请告诉我：①想看哪个时间段？②关注哪个车场还是全部？"

## SQL 编写规范

- 只写 SELECT，禁止 INSERT / UPDATE / DELETE / DROP / CREATE
- 时间过滤用字符串比较，如 `WHERE date(paid_at) >= '2025-01-01'`
- sales 表数值字段（paid_amount）已是数字，可直接做 SUM / AVG
- parking_ops 支付失败判断：`payment_result LIKE '%失败%' OR payment_result = '支付成功,通知车场失败'`
- 免费放行判断：`vehicle_type = '临时车' AND COALESCE(receivable_amount,0) = 0 AND COALESCE(actual_amount,0) = 0`
- 利用率估算：`SUM(stay_minutes) / (total_spaces * 1440.0)`
- 默认加 LIMIT 500 防止结果过大
- 多表 JOIN 必须写 ON 条件，禁止隐式笛卡尔积
  标准写法：`parking_payment_records p JOIN parking_lots l ON l.lot_id = p.lot_id`

## 工作流程

1. **问题模糊** → 直接文字澄清，不调工具
2. **复杂多步问题** → 先 reflect 列计划，再逐步 execute_sql
3. **执行查询** → execute_sql（自己写 SQL）
4. **解读数据** → 基于返回的 rows 给出结论，先结论后原因
5. **发现重要异常** → save_insight 记录
6. **每次回答后** → 主动提出 1-2 个追问建议"""


# ─── Tool 处理函数 ───────────────────────────────────────────────────────────

def _handle_list_data_sources(_input: dict) -> dict:
    return {"sources": {name: {"description": cfg["description"]} for name, cfg in DATA_SOURCES.items()}}


def _handle_execute_sql(input_: dict, session_id: str) -> dict:
    source_name = input_.get("source_name", "")
    sql         = input_.get("sql", "").strip()

    if source_name not in DATA_SOURCES:
        return {"error": f"未知数据源: {source_name}"}

    # 安全：只允许只读查询（SELECT / WITH...SELECT CTE / 注释开头）
    # 去掉行注释和块注释后取首个非空词判断
    import re as _re
    sql_stripped = _re.sub(r"--[^\n]*", "", sql)           # 去掉 -- 行注释
    sql_stripped = _re.sub(r"/\*.*?\*/", "", sql_stripped, flags=_re.DOTALL)  # 去掉 /* */ 块注释
    first_keyword = (sql_stripped.split() or [""])[0].upper()
    if first_keyword not in ("SELECT", "WITH"):
        return {"error": "只允许 SELECT 查询，禁止 INSERT / UPDATE / DELETE / DROP 等操作"}

    cfg = DATA_SOURCES[source_name]
    try:
        if cfg["source_type"] == "sqlite":
            rows = execute_raw_sql_on_sqlite(cfg["source_path"], sql)
        elif cfg["source_type"] == "csv":
            rows = execute_raw_sql_on_csv(cfg["source_path"], sql, table_name=source_name)
        else:
            return {"error": f"不支持的数据源类型: {cfg['source_type']}"}
    except Exception as exc:
        return {"error": f"SQL 执行失败: {exc}"}

    return {
        "row_count": len(rows),
        "rows":      rows[:500],
    }


def _handle_reflect(input_: dict) -> dict:
    return {"plan": input_.get("plan", ""), "status": "ok"}


def _handle_save_insight(input_: dict) -> dict:
    memory_dir = BASE_DIR / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    record = {
        "ts":      datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "topic":   input_.get("topic", ""),
        "insight": input_.get("insight", ""),
    }
    with (memory_dir / "insights.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
    return {"status": "saved", "topic": record["topic"]}


def _load_memory_context() -> str:
    """读取最近 10 条 insights，拼入 System Prompt 尾部。"""
    path = BASE_DIR / "memory" / "insights.jsonl"
    if not path.exists():
        return ""
    lines = path.read_text(encoding="utf-8").strip().splitlines()[-10:]
    items = [json.loads(line) for line in lines if line.strip()]
    if not items:
        return ""
    body = "\n".join(f"- [{i['ts'][:10]}] {i['topic']}：{i['insight']}" for i in items)
    return f"\n\n## 历史记忆（最近发现）\n{body}"


# ─── FastAPI App ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(_cleanup_sessions_loop())
    yield


app = FastAPI(title="smart-data-query web", lifespan=lifespan)

STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)


@app.get("/")
async def serve_index() -> HTMLResponse:
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return HTMLResponse(index_path.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>index.html not found</h1>", status_code=404)


@app.get("/api/data-sources")
async def get_data_sources() -> dict:
    return {name: {"description": cfg["description"]} for name, cfg in DATA_SOURCES.items()}


class ChatRequest(BaseModel):
    session_id: str
    message: str


@app.post("/api/chat")
async def chat(req: ChatRequest) -> StreamingResponse:
    return StreamingResponse(
        _chat_stream(req.session_id, req.message),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ─── SSE 工具 ─────────────────────────────────────────────────────────────────

def _sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

# 429 指数退避延迟（秒）：第1次等2s，第2次4s，第3次8s
_RETRY_DELAYS = [2, 4, 8]


# ─── SSE 流式核心：Tool Use 循环 ──────────────────────────────────────────────

async def _chat_stream(session_id: str, user_message: str) -> AsyncGenerator[str, None]:
    if session_id not in sessions:
        sessions[session_id] = SessionData()
    session = sessions[session_id]
    session.last_active = time.time()
    session.messages.append({"role": "user", "content": user_message})

    api_key  = os.environ.get("ANTHROPIC_AUTH_TOKEN") or os.environ.get("ANTHROPIC_API_KEY", "")
    base_url = os.environ.get("ANTHROPIC_BASE_URL")
    model    = os.environ.get("ANTHROPIC_MODEL", "claude-opus-4-6")

    if not api_key:
        yield _sse({"type": "error", "message": "未设置 ANTHROPIC_AUTH_TOKEN 或 ANTHROPIC_API_KEY"})
        yield _sse({"type": "done"})
        return

    client_kwargs: dict = {"api_key": api_key}
    if base_url:
        client_kwargs["base_url"] = base_url
    client = anthropic.Anthropic(**client_kwargs)

    # 动态拼接历史记忆
    system = SYSTEM_PROMPT + _load_memory_context()

    max_tool_rounds = 8   # 复杂分析可能需要多轮 SQL 查询
    tool_round = 0

    try:
        while tool_round <= max_tool_rounds:

            # ── 带指数退避的 API 调用（处理 429 限流）────────────────────────
            tool_uses: list[dict] = []
            final_message = None
            last_rate_exc: anthropic.RateLimitError | None = None

            for attempt, wait_secs in enumerate([0] + _RETRY_DELAYS):
                if wait_secs:
                    yield _sse({
                        "type":    "retrying",
                        "wait":    wait_secs,
                        "attempt": attempt,          # 第几次重试（1-based）
                        "max":     len(_RETRY_DELAYS),
                    })
                    await asyncio.sleep(wait_secs)

                tool_uses = []
                last_rate_exc = None
                try:
                    with client.messages.stream(
                        model=model,
                        max_tokens=4096,
                        system=system,
                        tools=TOOLS,
                        messages=session.messages,
                    ) as stream:
                        for event in stream:
                            if event.type == "content_block_start":
                                if event.content_block.type == "tool_use":
                                    label_map = {
                                        "list_data_sources": "QUERYING DATA SOURCES",
                                        "execute_sql":       "EXECUTING SQL",
                                        "reflect":           "PLANNING ANALYSIS",
                                        "save_insight":      "SAVING INSIGHT",
                                    }
                                    yield _sse({
                                        "type":        "tool_use",
                                        "tool_name":   event.content_block.name,
                                        "tool_use_id": event.content_block.id,
                                        "label":       label_map.get(event.content_block.name, "PROCESSING"),
                                    })
                                    tool_uses.append({
                                        "type":  "tool_use",
                                        "id":    event.content_block.id,
                                        "name":  event.content_block.name,
                                        "input": {},
                                    })
                                elif event.content_block.type == "text":
                                    pass

                            elif event.type == "content_block_delta":
                                if event.delta.type == "text_delta":
                                    yield _sse({"type": "text_delta", "content": event.delta.text})
                                elif event.delta.type == "input_json_delta" and tool_uses:
                                    last = tool_uses[-1]
                                    last["_raw_input"] = last.get("_raw_input", "") + event.delta.partial_json

                            elif event.type == "content_block_stop":
                                if tool_uses and "_raw_input" in tool_uses[-1]:
                                    last = tool_uses[-1]
                                    try:
                                        last["input"] = json.loads(last.pop("_raw_input"))
                                    except json.JSONDecodeError:
                                        last["input"] = {}

                        final_message = stream.get_final_message()
                    break  # API 调用成功，退出重试循环

                except anthropic.RateLimitError as exc:
                    last_rate_exc = exc
                    # 还有重试次数则继续，否则在循环结束后抛出
                    continue

            if last_rate_exc is not None:
                raise last_rate_exc  # 重试全部耗尽，向外层抛出

            # 记录 assistant 回复
            assistant_content = []
            for block in final_message.content:
                if block.type == "text":
                    assistant_content.append({"type": "text", "text": block.text})
                elif block.type == "tool_use":
                    assistant_content.append({
                        "type":  "tool_use",
                        "id":    block.id,
                        "name":  block.name,
                        "input": block.input,
                    })
            session.messages.append({"role": "assistant", "content": assistant_content})

            if final_message.stop_reason != "tool_use" or not tool_uses:
                break

            tool_round += 1
            if tool_round > max_tool_rounds:
                yield _sse({"type": "error", "message": "工具调用次数超过上限"})
                # 补占位 tool_result，避免 session.messages 中 tool_use 无响应导致下次 400
                session.messages.append({
                    "role": "user",
                    "content": [
                        {
                            "type":        "tool_result",
                            "tool_use_id": tu["id"],
                            "content":     "工具调用已达上限，本次分析中止。",
                        }
                        for tu in tool_uses
                    ],
                })
                break

            # 执行工具
            tool_results = []
            for tu in tool_uses:
                tool_name  = tu["name"]
                tool_input = tu["input"]

                if tool_name == "list_data_sources":
                    result    = _handle_list_data_sources(tool_input)
                    chart_svg = ""

                elif tool_name == "execute_sql":
                    result    = await asyncio.to_thread(_handle_execute_sql, tool_input, session_id)
                    chart_svg = ""

                elif tool_name == "reflect":
                    result    = _handle_reflect(tool_input)
                    chart_svg = ""
                    yield _sse({"type": "reflect", "plan": result.get("plan", "")})

                elif tool_name == "save_insight":
                    result    = _handle_save_insight(tool_input)
                    chart_svg = ""

                else:
                    result    = {"error": f"未知工具: {tool_name}"}
                    chart_svg = ""

                # 发给前端
                yield _sse({
                    "type":        "tool_result",
                    "tool_use_id": tu["id"],
                    "tool_name":   tool_name,
                    "row_count":   result.get("row_count", 0),
                    "error":       result.get("error"),
                    "chart_svg":   chart_svg,
                    "plan":        result.get("plan"),
                    "sources":     result.get("sources"),
                })

                # 给 Claude 的结果（不含 SVG）
                claude_result = {k: v for k, v in result.items() if not k.startswith("_")}
                tool_results.append({
                    "type":        "tool_result",
                    "tool_use_id": tu["id"],
                    "content":     json.dumps(claude_result, ensure_ascii=False),
                })

            session.messages.append({"role": "user", "content": tool_results})

    except anthropic.APIStatusError as exc:
        yield _sse({"type": "error", "message": f"API 错误: {exc.status_code} {exc.message}"})
    except Exception as exc:
        yield _sse({"type": "error", "message": f"服务器错误: {exc}"})

    yield _sse({"type": "done"})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
