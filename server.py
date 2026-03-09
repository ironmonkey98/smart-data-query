"""
FastAPI Web 服务：将 smart_query 能力以 Claude Tool Use 形式暴露，配合 SSE 流式输出。
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import AsyncGenerator

import anthropic
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# 用 server.py 所在目录的绝对路径加载 .env，避免工作目录不同导致找不到
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

# 将 scripts 目录加入 sys.path，以便直接 import smart_query
SCRIPTS_DIR = Path(__file__).parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from smart_query import run_query  # noqa: E402  # scripts/smart_query.py

# ─── 数据源配置 ──────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent

DATA_SOURCES = {
    "sales": {
        "source_type": "csv",
        "source_path": str(BASE_DIR / "data" / "sample_sales.csv"),
        "schema_path": str(BASE_DIR / "references" / "db-schema.md"),
        "glossary_path": str(BASE_DIR / "references" / "term-glossary.md"),
        "description": "销售数据（成交额、区域、时间趋势等）",
    },
    "parking_ops": {
        "source_type": "csv",
        "source_path": str(BASE_DIR / "data" / "sample_parking_ops.csv"),
        "schema_path": str(BASE_DIR / "references" / "db-schema.md"),
        "glossary_path": str(BASE_DIR / "references" / "term-glossary.md"),
        "description": "停车场经营数据（收入、流量、异常诊断等）",
    },
}

# ─── Session 管理 ─────────────────────────────────────────────────────────────
@dataclass
class SessionData:
    messages: list[dict] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    last_active: float = field(default_factory=time.time)

# session_id → SessionData
sessions: dict[str, SessionData] = {}

SESSION_TTL_SECONDS = 2 * 3600  # 2 小时不活跃后清理


async def _cleanup_sessions_loop() -> None:
    """后台任务：每小时清理超时 session。"""
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
        "description": "列出所有可用的数据源及其说明，帮助判断该用哪个数据源回答问题。",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "query_data",
        "description": (
            "对指定数据源执行自然语言数据查询，返回摘要统计和 SVG 折线图。"
            "调用前请确认用户提供了时间范围（如最近7天/30天）。"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source_name": {
                    "type": "string",
                    "enum": list(DATA_SOURCES.keys()),
                    "description": "数据源名称，sales=销售数据，parking_ops=停车场数据",
                },
                "question": {
                    "type": "string",
                    "description": "用中文描述的自然语言查询问题",
                },
            },
            "required": ["source_name", "question"],
        },
    },
]

SYSTEM_PROMPT = """你是智能数据分析助理，专门分析销售数据和停车场经营数据。

## 数据源选择
- 停车场相关问题（收入、车流、异常开闸、支付失败）→ parking_ops
- 销售/成交额/区域/产品对比问题 → sales
- 问题不明确时先调用 list_data_sources

## 查询前置要求
- 必须有明确时间范围（如最近7天/30天），否则先向用户确认
- 图表已单独展示给用户，无需在文字中描述图表细节

## 追问改写规则（重要）
用户的追问往往省略了上文提到的区域、指标、时间范围。调用 query_data 前，
必须将追问改写为包含完整要素的独立查询句。

改写示例：
  用户上文问过"最近30天华南成交额趋势"，追问"从哪天开始下滑的？"
  → 改写为："最近30天华南每日成交额明细，按日期列出"
  再调用 query_data(source_name="sales", question="改写后的完整问题")

## 直接分析规则（优先于重新查询）
若 tool_result 中包含 rows_sample（原始数据行），且用户追问属于以下类型，
应直接对 rows_sample 进行计算，而不是重新调用 query_data：
  - 拐点/转折日期："从哪天开始下滑/上涨"
  - 极值定位："哪天最高/最低"
  - 变化幅度："下滑了多少%"、"最大跌幅是多少"
  - 环比计算："上周同比是多少"

计算方法（以拐点检测为例）：
  对 rows_sample 按日期排序后，对目标指标做滑动均值对比，
  连续3日均值下穿前期均值的首日即为拐点日。

## 输出规范
- 回答用中文，先结论后原因
- 每次分析后主动提出 1-2 个追问建议"""

# ─── Tool 处理函数 ───────────────────────────────────────────────────────────

def _handle_list_data_sources(_input: dict) -> dict:
    """直接返回 DATA_SOURCES 描述，不调用 smart_query。"""
    sources = {
        name: {"description": cfg["description"]}
        for name, cfg in DATA_SOURCES.items()
    }
    return {"sources": sources}


def _handle_query_data(input_: dict, session_id: str) -> dict:
    """桥接同步 run_query，结果含 summary 列表和 chart_svg 字符串。"""
    source_name = input_["source_name"]
    question = input_["question"]

    if source_name not in DATA_SOURCES:
        return {"error": f"未知数据源: {source_name}"}

    cfg = DATA_SOURCES[source_name]
    # 每个 session × source 独立输出目录，避免并发冲突
    output_dir = str(BASE_DIR / "run-output" / source_name / session_id)

    try:
        payload = run_query(
            question=question,
            source_type=cfg["source_type"],
            source=cfg["source_path"],
            schema=cfg["schema_path"],
            glossary=cfg["glossary_path"],
            output_dir=output_dir,
        )
    except Exception as exc:
        return {"error": str(exc)}

    # 读取生成的 SVG
    chart_path = Path(output_dir) / "chart.svg"
    chart_svg = chart_path.read_text(encoding="utf-8") if chart_path.exists() else ""

    # 构造传给 Claude 的精简结果（不含 SVG，节省 token）
    result_for_claude: dict = {
        "needs_clarification": payload.get("needs_clarification", False),
        "clarifying_question": payload.get("clarifying_question"),
        "summary": [],
        "row_count": 0,
    }

    if payload.get("needs_clarification"):
        pass  # 只传 clarifying_question 即可
    elif "result" in payload:
        # 普通查询路径
        result_for_claude["summary"] = payload.get("summary", [])
        result_for_claude["row_count"] = payload["result"].get("row_count", 0)
        result_for_claude["rows_sample"] = payload.get("rows_sample", [])
    elif "analysis" in payload:
        # 停车场路径
        analysis = payload["analysis"]
        result_for_claude["summary"] = [
            payload.get("executive_summary", ""),
            payload.get("narrative", ""),
        ]
        result_for_claude["row_count"] = analysis.get("row_count", 0)
        result_for_claude["rows_sample"] = payload.get("rows_sample", [])

    # chart_svg 存在 result 里，供 SSE 发给前端
    return {**result_for_claude, "_chart_svg": chart_svg}


# ─── FastAPI App ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(_cleanup_sessions_loop())
    yield


app = FastAPI(title="smart-data-query web", lifespan=lifespan)


# 静态文件（index.html）
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)


@app.get("/")
async def serve_index() -> HTMLResponse:
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return HTMLResponse(index_path.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>index.html not found</h1>", status_code=404)


class DataSourcesResponse(BaseModel):
    sources: dict


@app.get("/api/data-sources")
async def get_data_sources() -> dict:
    return {
        name: {"description": cfg["description"]}
        for name, cfg in DATA_SOURCES.items()
    }


class ChatRequest(BaseModel):
    session_id: str
    message: str


@app.post("/api/chat")
async def chat(req: ChatRequest) -> StreamingResponse:
    return StreamingResponse(
        _chat_stream(req.session_id, req.message),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # 禁用 nginx 缓冲
        },
    )


# ─── SSE 流式核心逻辑 ─────────────────────────────────────────────────────────

def _sse(data: dict) -> str:
    """格式化一条 SSE 消息。"""
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


async def _chat_stream(session_id: str, user_message: str) -> AsyncGenerator[str, None]:
    """Tool Use 循环 + SSE 流式输出。"""
    # 取或创建 session
    if session_id not in sessions:
        sessions[session_id] = SessionData()
    session = sessions[session_id]
    session.last_active = time.time()

    # 追加用户消息
    session.messages.append({"role": "user", "content": user_message})

    # 优先读取 Kimi / 兼容 Anthropic 的环境变量
    api_key  = os.environ.get("ANTHROPIC_AUTH_TOKEN") or os.environ.get("ANTHROPIC_API_KEY", "")
    base_url = os.environ.get("ANTHROPIC_BASE_URL")   # None 则使用 SDK 默认
    model    = os.environ.get("ANTHROPIC_MODEL", "claude-opus-4-6")

    if not api_key:
        yield _sse({"type": "error", "message": "未设置 ANTHROPIC_AUTH_TOKEN 或 ANTHROPIC_API_KEY 环境变量"})
        yield _sse({"type": "done"})
        return

    client_kwargs = {"api_key": api_key}
    if base_url:
        client_kwargs["base_url"] = base_url
    client = anthropic.Anthropic(**client_kwargs)

    max_tool_rounds = 5
    tool_round = 0
    # 存储本轮 Claude 回复（含 tool_use blocks），用于构造 messages
    pending_assistant_content: list = []

    try:
        while tool_round <= max_tool_rounds:
            # ── 调用 Claude API（流式）──────────────────────────────────────
            with client.messages.stream(
                model=model,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=TOOLS,
                messages=session.messages,
            ) as stream:
                tool_uses: list[dict] = []          # 收集本轮所有 tool_use blocks
                current_text = ""                   # 当前文字 block 累积

                for event in stream:
                    if event.type == "content_block_start":
                        if event.content_block.type == "tool_use":
                            # 通知前端开始调用工具
                            yield _sse({
                                "type": "tool_use",
                                "tool_name": event.content_block.name,
                                "tool_use_id": event.content_block.id,
                            })
                            tool_uses.append({
                                "type": "tool_use",
                                "id": event.content_block.id,
                                "name": event.content_block.name,
                                "input": {},
                            })
                        elif event.content_block.type == "text":
                            current_text = ""

                    elif event.type == "content_block_delta":
                        if event.delta.type == "text_delta":
                            current_text += event.delta.text
                            yield _sse({"type": "text_delta", "content": event.delta.text})
                        elif event.delta.type == "input_json_delta":
                            # 累积 tool input JSON
                            if tool_uses:
                                last = tool_uses[-1]
                                last["_raw_input"] = last.get("_raw_input", "") + event.delta.partial_json

                    elif event.type == "content_block_stop":
                        # 解析累积的 tool input JSON
                        if tool_uses and "_raw_input" in tool_uses[-1]:
                            last = tool_uses[-1]
                            try:
                                last["input"] = json.loads(last.pop("_raw_input"))
                            except json.JSONDecodeError:
                                last["input"] = {}

                    elif event.type == "message_stop":
                        pass  # 处理在循环外

                # 获取完整 message 对象（含 stop_reason）
                final_message = stream.get_final_message()

            # ── 记录助手回复 ──────────────────────────────────────────────
            # 将本轮助手回复加入 messages（使用完整 content blocks）
            assistant_content = []
            for block in final_message.content:
                if block.type == "text":
                    assistant_content.append({"type": "text", "text": block.text})
                elif block.type == "tool_use":
                    assistant_content.append({
                        "type": "tool_use",
                        "id": block.id,
                        "name": block.name,
                        "input": block.input,
                    })

            session.messages.append({"role": "assistant", "content": assistant_content})

            # ── 判断是否需要继续 tool_use 循环 ───────────────────────────
            if final_message.stop_reason != "tool_use" or not tool_uses:
                break  # Claude 已完成，退出循环

            tool_round += 1
            if tool_round > max_tool_rounds:
                yield _sse({"type": "error", "message": "工具调用次数超过上限"})
                break

            # ── 执行工具调用 ──────────────────────────────────────────────
            tool_results = []
            for tu in tool_uses:
                tool_name = tu["name"]
                tool_input = tu["input"]

                # 在线程池中执行同步工具（run_query 是同步阻塞的）
                if tool_name == "list_data_sources":
                    result = _handle_list_data_sources(tool_input)
                    chart_svg = ""
                elif tool_name == "query_data":
                    result = await asyncio.to_thread(
                        _handle_query_data, tool_input, session_id
                    )
                    chart_svg = result.pop("_chart_svg", "")
                else:
                    result = {"error": f"未知工具: {tool_name}"}
                    chart_svg = ""

                # 发给前端的 tool_result 事件（含 SVG）
                yield _sse({
                    "type": "tool_result",
                    "tool_use_id": tu["id"],
                    "summary": result.get("summary", []),
                    "row_count": result.get("row_count", 0),
                    "needs_clarification": result.get("needs_clarification", False),
                    "clarifying_question": result.get("clarifying_question"),
                    "chart_svg": chart_svg,
                    "sources": result.get("sources"),  # list_data_sources 返回
                    "error": result.get("error"),
                })

                # 传给 Claude 的 tool_result（不含 SVG）
                claude_result = {k: v for k, v in result.items() if k != "_chart_svg"}
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu["id"],
                    "content": json.dumps(claude_result, ensure_ascii=False),
                })

            # 将 tool_results 加入 messages，触发下一轮 Claude 推理
            session.messages.append({"role": "user", "content": tool_results})

    except anthropic.APIStatusError as exc:
        yield _sse({"type": "error", "message": f"API 错误: {exc.status_code} {exc.message}"})
    except Exception as exc:
        yield _sse({"type": "error", "message": f"服务器错误: {exc}"})

    yield _sse({"type": "done"})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
