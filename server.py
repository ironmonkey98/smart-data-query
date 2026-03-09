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
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import AsyncGenerator

import anthropic
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# 用 server.py 所在目录的绝对路径加载 .env，避免工作目录不同导致找不到
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

# 将 scripts 目录加入 sys.path，以便直接 import smart_query
SCRIPTS_DIR = Path(__file__).parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from smart_query import run_query  # noqa: E402  # scripts/smart_query.py
from parking_skill_runtime import (  # noqa: E402
    build_final_text,
    build_runtime_think_detail,
    build_tool_result,
    decide_next_action,
    detect_parking_task,
    execute_skill,
    select_skill_name,
    should_handle_with_runtime,
    summarize_check,
)

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
        "source_type": "sqlite",
        "source_path": str(BASE_DIR / "data" / "sample_parking_ops.db"),
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
    {
        "name": "reflect",
        "description": "在执行复杂多步分析前，先输出分析计划和待查清单。适用于：涉及多个数据源、需要对比多个时间段、或用户问题需要分解子任务时。",
        "input_schema": {
            "type": "object",
            "properties": {
                "plan": {
                    "type": "string",
                    "description": "用中文描述本次分析计划，列出将要执行的步骤",
                }
            },
            "required": ["plan"],
        },
    },
    {
        "name": "compare_periods",
        "description": "对同一数据源的两个时间段进行对比分析，返回环比/同比差值。适用于：本月vs上月、本周vs上周、今年vs去年。",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_name": {
                    "type": "string",
                    "enum": list(DATA_SOURCES.keys()),
                    "description": "数据源名称",
                },
                "question": {
                    "type": "string",
                    "description": "分析主题，如'各区域成交额'",
                },
                "period_a": {
                    "type": "string",
                    "description": "当前周期，如'最近30天'",
                },
                "period_b": {
                    "type": "string",
                    "description": "对比周期，如'前30天'或'上个月'",
                },
            },
            "required": ["source_name", "question", "period_a", "period_b"],
        },
    },
    {
        "name": "save_insight",
        "description": "将重要分析发现保存到记忆中，供后续对话引用。适用于：发现数据异常、确认某区域长期趋势、用户明确要求记住某个结论。",
        "input_schema": {
            "type": "object",
            "properties": {
                "topic": {
                    "type": "string",
                    "description": "主题标签，如'B停车场故障'",
                },
                "insight": {
                    "type": "string",
                    "description": "核心发现，一两句话",
                },
            },
            "required": ["topic", "insight"],
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
- 对于“生成最近7天停车经营周报/给管理层看/停车经营报告”这类单一停车经营报表需求，
  直接调用 query_data(source_name="parking_ops", question="用户原问题") 生成完整管理层报表，不要拆成多个子查询。

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
- 每次分析后主动提出 1-2 个追问建议

## 何时使用 reflect 工具
以下情况必须先调用 reflect，再执行查询：
- 用户问题涉及 2 个以上数据源
- 需要对比多个时间段（如同比、环比）
- 问题包含"综合"、"全面分析"、"报告"等词，且需要跨数据源或多步骤拆解
reflect 的 plan 字段要列出：将查询哪些数据源、用什么时间范围、对比逻辑是什么。

## 何时使用 save_insight
以下情况主动调用 save_insight：
- 发现明确的数据异常（如支付失败率超过5%）
- 用户表达"记住这个"、"下次提醒我"
- 连续3次追问同一主题后确认的结论"""

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
        return _build_query_error_payload(exc, source_name)

    # 读取生成的 SVG
    chart_path = Path(output_dir) / "chart.svg"
    chart_svg = chart_path.read_text(encoding="utf-8") if chart_path.exists() else ""

    # 构造传给 Claude 的精简结果（不含 SVG，节省 token）
    result_for_claude: dict = {
        "needs_clarification": payload.get("needs_clarification", False),
        "clarifying_question": payload.get("clarifying_question"),
        "summary": [],
        "row_count": 0,
        "default_time_note": payload.get("default_time_note"),
    }

    if payload.get("needs_clarification"):
        pass  # 只传 clarifying_question 即可
    elif "result" in payload:
        # 普通查询路径
        result_for_claude["summary"] = payload.get("summary", [])
        result_for_claude["row_count"] = payload["result"].get("row_count", 0)
        result_for_claude["rows_sample"] = payload["result"].get("rows", [])
    elif "analysis" in payload:
        # 停车场路径
        analysis = payload["analysis"]
        rows_sample = payload.get("rows_sample", [])
        result_for_claude["summary"] = [
            payload.get("executive_summary", ""),
            payload.get("narrative", ""),
        ]
        result_for_claude["row_count"] = _resolve_query_row_count(payload)
        result_for_claude["rows_sample"] = rows_sample
        if analysis.get("analysis_type") == "management_report":
            report_id = _persist_report_payload(
                payload=payload,
                question=question,
                session_id=session_id,
                chart_svg=chart_svg,
            )
            result_for_claude["report_id"] = report_id
            result_for_claude["report_url"] = f"/report/{report_id}"

    # chart_svg 存在 result 里，供 SSE 发给前端
    return {**result_for_claude, "_chart_svg": chart_svg}


def _handle_reflect(input_: dict) -> dict:
    """直接返回分析计划，不调用任何数据接口。"""
    return {"plan": input_.get("plan", ""), "status": "ok"}


def _handle_compare_periods(input_: dict, session_id: str) -> dict:
    """对同一数据源的两个时间段分别查询，合并计算 delta 和 pct_change。"""
    source_name = input_["source_name"]
    question = input_["question"]
    period_a = input_["period_a"]
    period_b = input_["period_b"]

    if source_name not in DATA_SOURCES:
        return {"error": f"未知数据源: {source_name}"}
    if source_name != "sales":
        return {
            "error": "compare_periods 当前仅支持 sales 数据源，请直接使用 query_data 处理停车经营报表。"
        }

    question_a = f"{period_a} {question}"
    question_b = f"{period_b} {question}"

    result_a = _handle_query_data(
        {"source_name": source_name, "question": question_a},
        session_id + "/period_a",
    )
    result_b = _handle_query_data(
        {"source_name": source_name, "question": question_b},
        session_id + "/period_b",
    )

    if result_a.get("error"):
        return {
            "error": f"period_a 查询失败: {result_a['error']}",
            "error_hint": result_a.get("error_hint"),
        }
    if result_b.get("error"):
        return {
            "error": f"period_b 查询失败: {result_b['error']}",
            "error_hint": result_b.get("error_hint"),
        }

    rows_a = result_a.get("rows_sample", [])
    rows_b = result_b.get("rows_sample", [])
    comparison_rows = []
    if rows_a and rows_b:
        sample_keys = list(rows_a[0].keys())
        numeric_keys = [k for k, v in rows_a[0].items() if isinstance(v, (int, float))]
        index_keys = [k for k in sample_keys if k not in numeric_keys]

        def make_key(row: dict) -> tuple:
            return tuple(row.get(k) for k in index_keys)

        b_lookup = {make_key(row): row for row in rows_b}

        for row_a in rows_a:
            key = make_key(row_a)
            row_b = b_lookup.get(key)
            merged = {**row_a, "_period": period_a}
            if row_b:
                for numeric_key in numeric_keys:
                    val_a = row_a.get(numeric_key, 0) or 0
                    val_b = row_b.get(numeric_key, 0) or 0
                    merged[f"{numeric_key}_prev"] = val_b
                    merged[f"{numeric_key}_delta"] = round(val_a - val_b, 4)
                    merged[f"{numeric_key}_pct"] = (
                        round((val_a - val_b) / val_b * 100, 2) if val_b != 0 else None
                    )
            comparison_rows.append(merged)

    return {
        "period_a": period_a,
        "period_b": period_b,
        "rows_sample": comparison_rows,
        "summary": result_a.get("summary", []),
        "row_count": len(comparison_rows),
        "_chart_svg": result_a.pop("_chart_svg", ""),
    }


def _build_query_error_payload(exc: Exception, source_name: str) -> dict:
    if isinstance(exc, FileNotFoundError):
        missing_path = str(exc.filename or "")
        if source_name == "sales":
            return {
                "error": "销售示例数据暂不可用。",
                "error_hint": (
                    "缺少 data/sample_sales.csv。"
                    " 请恢复该文件，或重新生成销售样例数据后再执行区域销售对比。"
                ),
                "error_detail": missing_path,
            }
        if source_name == "parking_ops":
            return {
                "error": "停车经营数据暂不可用。",
                "error_hint": (
                    "缺少停车经营数据文件。"
                    " 请先运行 scripts/build_parking_ops_from_excels.py 重新生成数据源。"
                ),
                "error_detail": missing_path,
            }
    return {"error": str(exc)}


def _handle_save_insight(input_: dict) -> dict:
    """将重要发现追加写入 memory/insights.jsonl。"""
    memory_dir = BASE_DIR / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    insight_path = memory_dir / "insights.jsonl"

    record = {
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "topic": input_.get("topic", ""),
        "insight": input_.get("insight", ""),
    }
    with insight_path.open("a", encoding="utf-8") as file_obj:
        file_obj.write(json.dumps(record, ensure_ascii=False) + "\n")

    return {"status": "saved", "topic": record["topic"]}


def _load_memory_context() -> str:
    """读取最近10条 insights，格式化为 System Prompt 附录。"""
    path = BASE_DIR / "memory" / "insights.jsonl"
    if not path.exists():
        return ""
    lines = path.read_text(encoding="utf-8").strip().splitlines()[-10:]
    items = [json.loads(line) for line in lines if line.strip()]
    if not items:
        return ""
    body = "\n".join(
        f"- [{item['ts'][:10]}] {item['topic']}：{item['insight']}" for item in items
    )
    return f"\n\n## 历史记忆（最近发现）\n{body}"


def _persist_report_payload(payload: dict, question: str, session_id: str, chart_svg: str) -> str:
    """将管理层报表写入文件，供独立报表页读取。"""
    report_id = f"{_slugify_session_id(session_id)}-{uuid.uuid4().hex[:8]}"
    report_dir = BASE_DIR / "run-output" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{report_id}.json"
    analysis = payload["analysis"]
    report_type = analysis.get("report_type", "weekly")
    report_payload = {
        "report_id": report_id,
        "analysis_type": analysis["analysis_type"],
        "report_type": report_type,
        "title": "停车经营管理层日报" if report_type == "daily" else "停车经营管理层简报",
        "question": question,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "semantic_plan": payload.get("task", {}).get("semantic_plan", {}),
        "overview": analysis.get("overview", {}),
        "executive_summary": payload.get("executive_summary", []),
        "narrative": payload.get("narrative", {}),
        "focus_lots": analysis.get("focus_lots", []),
        "priority_actions": analysis.get("priority_actions", []),
        "modules": analysis.get("modules", {}),
        "chart_svg": chart_svg,
    }
    report_path.write_text(
        json.dumps(report_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return report_id


def _load_report_payload(report_id: str) -> dict:
    report_path = BASE_DIR / "run-output" / "reports" / f"{report_id}.json"
    if not report_path.exists():
        raise HTTPException(status_code=404, detail="报表不存在")
    return json.loads(report_path.read_text(encoding="utf-8"))


def _slugify_session_id(session_id: str) -> str:
    sanitized = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in session_id)
    return sanitized.strip("-") or "report"


def _is_direct_management_report_request(user_message: str) -> bool:
    report_keywords = ("周报", "日报", "管理层", "经营报告", "经营简报")
    parking_keywords = ("停车", "车场", "场子")
    operator_keywords = ("经营", "情况", "老板", "今日", "今天")
    return (
        (any(keyword in user_message for keyword in parking_keywords) and any(keyword in user_message for keyword in report_keywords))
        or (any(keyword in user_message for keyword in operator_keywords) and any(keyword in user_message for keyword in ("日报", "今天", "今日", "老板")))
    )


async def _stream_direct_management_report(
    session_id: str, user_message: str
) -> AsyncGenerator[str, None]:
    """绕过 LLM，直接生成停车经营管理层报表。"""
    step_group = f"direct-report-{uuid.uuid4().hex[:8]}"
    yield _sse(_build_step_event(
        phase="think",
        title="识别为单一停车报表请求",
        detail="当前问题已具备停车经营报表所需的核心信息，可以直接生成管理层报表。",
        step_group=step_group,
    ))
    tool_use_id = f"tool_direct_{uuid.uuid4().hex[:12]}"
    yield _sse(_build_step_event(
        phase="act",
        title="执行停车经营报表查询",
        detail="调用 query_data，对停车经营数据生成完整管理层报表。",
        status="started",
        tool_name="query_data",
        step_group=step_group,
    ))
    yield _sse({
        "type": "tool_use",
        "tool_name": "query_data",
        "tool_use_id": tool_use_id,
    })

    result = await asyncio.to_thread(
        _handle_query_data,
        {"source_name": "parking_ops", "question": user_message},
        session_id,
    )
    chart_svg = result.pop("_chart_svg", "")
    yield _sse(_build_step_event(
        phase="check",
        title="检查报表查询结果",
        detail=_summarize_tool_check("query_data", result),
        status="failed" if result.get("error") else "completed",
        tool_name="query_data",
        step_group=step_group,
    ))
    yield _sse(_build_step_event(
        phase="decide",
        title="决定下一步",
        detail=_summarize_tool_decision("query_data", result),
        status="failed" if result.get("error") else "completed",
        tool_name="query_data",
        step_group=step_group,
    ))
    yield _sse({
        "type": "tool_result",
        "tool_use_id": tool_use_id,
        "summary": result.get("summary", []),
        "row_count": result.get("row_count", 0),
        "needs_clarification": result.get("needs_clarification", False),
        "clarifying_question": result.get("clarifying_question"),
        "chart_svg": chart_svg,
        "sources": result.get("sources"),
        "report_id": result.get("report_id"),
        "report_url": result.get("report_url"),
        "error": result.get("error"),
    })

    if result.get("error"):
        yield _sse({"type": "error", "message": result["error"]})
        yield _sse({"type": "done"})
        return

    if result.get("needs_clarification") and result.get("clarifying_question"):
        yield _sse({"type": "text_delta", "content": result["clarifying_question"]})
        yield _sse({"type": "done"})
        return

    summary_lines = []
    for item in result.get("summary", []):
        if isinstance(item, list):
            summary_lines.extend(str(part) for part in item if part)
        elif isinstance(item, dict):
            narrative = item.get("narrative") or item.get("summary")
            if narrative:
                summary_lines.append(str(narrative))
        elif item:
            summary_lines.append(str(item))
    if result.get("report_url"):
        summary_lines.append("已生成独立管理层简报，可打开查看完整结构化报表。")

    final_text = "\n".join(summary_lines).strip() or "已生成管理层报表。"
    yield _sse({"type": "text_delta", "content": final_text})
    yield _sse({"type": "done"})


async def _stream_parking_skill_runtime(
    session_id: str,
    user_message: str,
) -> AsyncGenerator[str, None]:
    cfg = DATA_SOURCES["parking_ops"]
    task = detect_parking_task(
        question=user_message,
        schema_path=cfg["schema_path"],
        glossary_path=cfg["glossary_path"],
    )
    skill_name = select_skill_name(task)
    step_group = f"{skill_name}-{uuid.uuid4().hex[:8]}"
    yield _sse(_build_step_event(
        phase="think",
        title=f"识别并选择 {skill_name}",
        detail=build_runtime_think_detail(task, skill_name, user_message),
        step_group=step_group,
    ))

    if task.get("needs_clarification"):
        yield _sse(_build_step_event(
            phase="decide",
            title="决定下一步",
            detail="当前信息不足，停止继续执行并向用户发起澄清。",
            status="completed",
            tool_name=skill_name,
            step_group=step_group,
        ))
        yield _sse({"type": "text_delta", "content": task["clarifying_question"]})
        yield _sse({"type": "done"})
        return

    tool_use_id = f"tool_runtime_{uuid.uuid4().hex[:12]}"
    yield _sse(_build_step_event(
        phase="act",
        title=f"执行 {skill_name}",
        detail="调用停车经营 Skill Runtime，开始执行当前最小动作。",
        status="started",
        tool_name="query_data",
        step_group=step_group,
    ))
    yield _sse({
        "type": "tool_use",
        "tool_name": "query_data",
        "tool_use_id": tool_use_id,
    })

    output_dir = str(BASE_DIR / "run-output" / "parking_ops" / session_id)
    payload = await asyncio.to_thread(
        execute_skill,
        user_message,
        task,
        cfg["source_type"],
        cfg["source_path"],
        cfg["schema_path"],
        cfg["glossary_path"],
        output_dir,
    )
    chart_path = Path(output_dir) / "chart.svg"
    chart_svg = chart_path.read_text(encoding="utf-8") if chart_path.exists() else ""
    result = build_tool_result(payload, chart_svg)

    if "analysis" in payload and payload["analysis"].get("analysis_type") == "management_report":
        report_id = _persist_report_payload(
            payload=payload,
            question=user_message,
            session_id=session_id,
            chart_svg=chart_svg,
        )
        result["report_id"] = report_id
        result["report_url"] = f"/report/{report_id}"

    yield _sse(_build_step_event(
        phase="check",
        title=f"检查 {skill_name} 结果",
        detail=summarize_check(result),
        status="failed" if result.get("error") else "completed",
        tool_name="query_data",
        step_group=step_group,
    ))
    decision, decision_detail = decide_next_action(result)
    yield _sse(_build_step_event(
        phase="decide",
        title="决定下一步",
        detail=decision_detail,
        status="failed" if decision == "fail" else "completed",
        tool_name="query_data",
        step_group=step_group,
    ))

    final_chart_svg = result.pop("_chart_svg", "")
    yield _sse({
        "type": "tool_result",
        "tool_use_id": tool_use_id,
        "summary": result.get("summary", []),
        "row_count": result.get("row_count", 0),
        "needs_clarification": result.get("needs_clarification", False),
        "clarifying_question": result.get("clarifying_question"),
        "chart_svg": final_chart_svg,
        "sources": result.get("sources"),
        "report_id": result.get("report_id"),
        "report_url": result.get("report_url"),
        "default_time_note": result.get("default_time_note"),
        "error": result.get("error"),
        "error_hint": result.get("error_hint"),
    })

    if decision == "fail":
        yield _sse({"type": "error", "message": result.get("error", "Skill Runtime 执行失败")})
        yield _sse({"type": "done"})
        return

    final_text = build_final_text(result)
    if final_text:
        yield _sse({"type": "text_delta", "content": final_text})
    yield _sse({"type": "done"})


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


@app.get("/report/{report_id}")
async def serve_report_page(report_id: str) -> HTMLResponse:
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


@app.get("/api/report/{report_id}")
async def get_report(report_id: str) -> dict:
    return _load_report_payload(report_id)


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


def _build_step_event(
    phase: str,
    title: str,
    detail: str,
    status: str = "completed",
    tool_name: str | None = None,
    step_group: str | None = None,
) -> dict:
    return {
        "type": "step",
        "step_id": f"step_{uuid.uuid4().hex[:10]}",
        "step_group": step_group or "default",
        "phase": phase,
        "title": title,
        "detail": detail,
        "status": status,
        "tool_name": tool_name,
    }


def _tool_think_detail(tool_name: str, tool_input: dict) -> str:
    if tool_name == "query_data":
        return f"先确认数据源和目标问题，再执行查询。当前目标：{tool_input.get('question', '')}"
    if tool_name == "reflect":
        return "当前问题需要先拆成更小的分析步骤，避免直接执行出错。"
    if tool_name == "compare_periods":
        return "当前问题涉及两个时间窗口，先执行周期对比。"
    if tool_name == "save_insight":
        return "当前发现具备复用价值，先写入记忆。"
    if tool_name == "list_data_sources":
        return "先确认当前可用数据源，再决定后续查询路径。"
    return "先明确当前最小目标，再执行对应动作。"


def _summarize_tool_check(tool_name: str, result: dict) -> str:
    if result.get("error"):
        return result["error"]
    if result.get("needs_clarification") and result.get("clarifying_question"):
        return result["clarifying_question"]
    if tool_name == "query_data":
        if result.get("report_url"):
            row_count = int(result.get("row_count", 0) or 0)
            if row_count > 0:
                return f"查询已完成，生成了独立报表链接，并产出 {row_count} 条分析数据。"
            return "查询已完成，并生成了独立报表链接。"
        return f"查询已完成，得到 {result.get('row_count', 0)} 条结果。"
    if tool_name == "reflect":
        return "分析计划已生成。"
    if tool_name == "compare_periods":
        return f"周期对比已完成，得到 {result.get('row_count', 0)} 条结果。"
    if tool_name == "save_insight":
        return "关键发现已保存到记忆。"
    if tool_name == "list_data_sources":
        return "可用数据源列表已返回。"
    return "当前动作已执行完成。"


def _summarize_tool_decision(tool_name: str, result: dict) -> str:
    if result.get("error"):
        return "当前步骤失败，结束本轮分析并向用户展示错误。"
    if result.get("needs_clarification"):
        return "当前信息不足，停止继续分析并向用户发起澄清。"
    if tool_name == "reflect":
        return "当前计划已明确，下一步进入查询动作。"
    if tool_name == "query_data" and result.get("report_url"):
        return "当前结果已足够，结束工具调用并输出最终结论。"
    if tool_name == "query_data":
        row_count = int(result.get("row_count", 0) or 0)
        if row_count > 0:
            return "当前结果已返回，进入下一轮推理判断是否直接输出结论。"
        return "当前结果不足，进入下一轮推理判断是否需要继续查询或澄清。"
    if tool_name == "save_insight":
        return "记忆保存完成，返回结论生成阶段。"
    if tool_name == "compare_periods":
        return "当前对比结果已返回，进入下一轮推理生成结论。"
    return "当前步骤已完成，进入下一轮推理判断下一步。"


def _resolve_query_row_count(payload: dict) -> int:
    if "result" in payload:
        return int(payload["result"].get("row_count", 0) or 0)
    if "analysis" in payload:
        rows_sample = payload.get("rows_sample") or []
        if rows_sample:
            return len(rows_sample)
        analysis = payload.get("analysis") or {}
        focus_lots = analysis.get("focus_lots") or []
        if focus_lots:
            return len(focus_lots)
        executive_summary = payload.get("executive_summary") or []
        if executive_summary:
            return len(executive_summary)
    return 0


async def _chat_stream(session_id: str, user_message: str) -> AsyncGenerator[str, None]:
    """Tool Use 循环 + SSE 流式输出。"""
    # 取或创建 session
    if session_id not in sessions:
        sessions[session_id] = SessionData()
    session = sessions[session_id]
    session.last_active = time.time()

    # 追加用户消息
    session.messages.append({"role": "user", "content": user_message})

    if _is_direct_management_report_request(user_message):
        async for chunk in _stream_direct_management_report(session_id, user_message):
            yield chunk
        return

    if should_handle_with_runtime(
        question=user_message,
        schema_path=DATA_SOURCES["parking_ops"]["schema_path"],
        glossary_path=DATA_SOURCES["parking_ops"]["glossary_path"],
    ):
        async for chunk in _stream_parking_skill_runtime(session_id, user_message):
            yield chunk
        return

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
                system=SYSTEM_PROMPT + _load_memory_context(),
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
                if current_text.strip():
                    yield _sse(_build_step_event(
                        phase="decide",
                        title="完成最终决策",
                        detail="当前结果已收敛，停止继续推理并输出最终结论。",
                        status="completed",
                        tool_name=None,
                        step_group=f"final-answer-{uuid.uuid4().hex[:8]}",
                    ))
                    yield _sse({"type": "text_delta", "content": current_text})
                break  # Claude 已完成，退出循环

            tool_round += 1
            if tool_round > max_tool_rounds:
                yield _sse(_build_step_event(
                    phase="decide",
                    title="达到循环上限",
                    detail="已连续尝试多轮推理仍未收敛，停止继续分析并要求用户补充条件。",
                    status="failed",
                    tool_name=None,
                    step_group=f"loop-limit-{uuid.uuid4().hex[:8]}",
                ))
                yield _sse({"type": "text_delta", "content": "我已经连续尝试多轮分析，但当前仍无法唯一确认口径，请补充更明确的时间范围、对象或指标。"})
                yield _sse({"type": "error", "message": "工具调用次数超过上限"})
                break

            # ── 执行工具调用 ──────────────────────────────────────────────
            tool_results = []
            for tu in tool_uses:
                tool_name = tu["name"]
                tool_input = tu["input"]
                step_group = f"{tool_name}-{uuid.uuid4().hex[:8]}"
                yield _sse(_build_step_event(
                    phase="think",
                    title=f"准备执行 {tool_name}",
                    detail=_tool_think_detail(tool_name, tool_input),
                    tool_name=tool_name,
                    step_group=step_group,
                ))
                yield _sse(_build_step_event(
                    phase="act",
                    title=f"执行 {tool_name}",
                    detail=f"开始调用 {tool_name} 工具。",
                    status="started",
                    tool_name=tool_name,
                    step_group=step_group,
                ))

                # 在线程池中执行同步工具（run_query 是同步阻塞的）
                if tool_name == "list_data_sources":
                    result = _handle_list_data_sources(tool_input)
                    chart_svg = ""
                elif tool_name == "reflect":
                    result = _handle_reflect(tool_input)
                    chart_svg = ""
                    yield _sse({"type": "reflect", "plan": result.get("plan", "")})
                elif tool_name == "compare_periods":
                    result = await asyncio.to_thread(
                        _handle_compare_periods, tool_input, session_id
                    )
                    chart_svg = result.pop("_chart_svg", "")
                elif tool_name == "query_data":
                    result = await asyncio.to_thread(
                        _handle_query_data, tool_input, session_id
                    )
                    chart_svg = result.pop("_chart_svg", "")
                elif tool_name == "save_insight":
                    result = _handle_save_insight(tool_input)
                    chart_svg = ""
                else:
                    result = {"error": f"未知工具: {tool_name}"}
                    chart_svg = ""

                yield _sse(_build_step_event(
                    phase="check",
                    title=f"检查 {tool_name} 结果",
                    detail=_summarize_tool_check(tool_name, result),
                    status="failed" if result.get("error") else "completed",
                    tool_name=tool_name,
                    step_group=step_group,
                ))
                yield _sse(_build_step_event(
                    phase="decide",
                    title="决定下一步",
                    detail=_summarize_tool_decision(tool_name, result),
                    status="failed" if result.get("error") else "completed",
                    tool_name=tool_name,
                    step_group=step_group,
                ))

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
                    "report_id": result.get("report_id"),
                    "report_url": result.get("report_url"),
                    "default_time_note": result.get("default_time_note"),
                    "error": result.get("error"),
                    "error_hint": result.get("error_hint"),
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
