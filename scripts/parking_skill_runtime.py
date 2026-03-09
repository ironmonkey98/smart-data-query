from __future__ import annotations

import json
from pathlib import Path

from smart_query import run_query
from sql_generator import normalize_question


def detect_parking_task(question: str, schema_path: str, glossary_path: str) -> dict:
    schema_text = Path(schema_path).read_text(encoding="utf-8")
    glossary_text = Path(glossary_path).read_text(encoding="utf-8")
    return normalize_question(
        question=question,
        schema_text=schema_text,
        glossary_text=glossary_text,
    )


def should_handle_with_runtime(question: str, schema_path: str, glossary_path: str) -> bool:
    if "分析计划" in question or ("计划" in question and "查询" not in question and "报告" not in question):
        return False
    task = detect_parking_task(question, schema_path, glossary_path)
    return str(task.get("domain")) == "parking_ops" or str(task.get("intent", "")).startswith("parking_")


def select_skill_name(task: dict) -> str:
    intent = task.get("intent")
    query_profile = task.get("query_profile")
    if intent in {"parking_management_report", "parking_management_daily_report"}:
        return "parking_management_report_skill"
    if intent == "parking_period_assessment":
        return "parking_period_assessment_skill"
    if intent == "parking_anomaly_diagnosis":
        return "parking_anomaly_skill"
    if intent == "parking_flow_efficiency_analysis":
        return "parking_efficiency_skill"
    if intent == "parking_revenue_analysis":
        return "parking_revenue_skill"
    if query_profile and query_profile != "parking_daily_overview_join":
        return "parking_relational_query_skill"
    return "parking_query_skill"


def build_runtime_think_detail(task: dict, skill_name: str, question: str) -> str:
    query_profile = task.get("query_profile")
    if skill_name == "parking_management_report_skill":
        return f"先按停车经营报表 Skill 处理当前问题，并确定周期、报表类型和核心指标。目标问题：{question}"
    if skill_name == "parking_period_assessment_skill":
        return f"先按周期评估 Skill 判断当前周期相对上一周期是好转还是变坏，并归纳主要原因。目标问题：{question}"
    if skill_name == "parking_anomaly_skill":
        return f"先按异常诊断 Skill 识别高风险车场与异常信号。目标问题：{question}"
    if skill_name == "parking_relational_query_skill":
        return f"先按关系型联查 Skill 处理当前问题，并使用 {query_profile} 这类联查模板。目标问题：{question}"
    return f"先按停车经营 Skill 处理当前问题。目标问题：{question}"


def execute_skill(
    question: str,
    task: dict,
    source_type: str,
    source_path: str,
    schema_path: str,
    glossary_path: str,
    output_dir: str,
) -> dict:
    payload = run_query(
        question=question,
        source_type=source_type,
        source=source_path,
        schema=schema_path,
        glossary=glossary_path,
        output_dir=output_dir,
    )
    return payload


def resolve_row_count(payload: dict) -> int:
    if "result" in payload:
        return int(payload["result"].get("row_count", 0) or 0)
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


def build_tool_result(payload: dict, chart_svg: str) -> dict:
    result = {
        "needs_clarification": payload.get("needs_clarification", False),
        "clarifying_question": payload.get("clarifying_question"),
        "summary": [],
        "row_count": resolve_row_count(payload),
        "default_time_note": payload.get("default_time_note"),
    }
    if payload.get("needs_clarification"):
        return {**result, "_chart_svg": chart_svg}

    if "result" in payload:
        result["summary"] = payload.get("summary", [])
        result["rows_sample"] = payload["result"].get("rows", [])
    elif "analysis" in payload:
        result["summary"] = [
            payload.get("executive_summary", []),
            payload.get("narrative", ""),
        ]
        result["rows_sample"] = payload.get("rows_sample", [])
    return {**result, "_chart_svg": chart_svg}


def build_final_text(result: dict) -> str:
    if result.get("needs_clarification") and result.get("clarifying_question"):
        return result["clarifying_question"]

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
    return "\n".join(summary_lines).strip()


def summarize_check(result: dict) -> str:
    if result.get("error"):
        return result["error"]
    if result.get("needs_clarification") and result.get("clarifying_question"):
        return result["clarifying_question"]
    row_count = int(result.get("row_count", 0) or 0)
    if result.get("report_url"):
        return f"技能执行完成，生成了独立报表链接，并产出 {row_count} 条分析数据。"
    return f"技能执行完成，得到 {row_count} 条有效结果。"


def decide_next_action(result: dict) -> tuple[str, str]:
    if result.get("error"):
        return "fail", "当前步骤失败，停止继续执行并展示错误。"
    if result.get("needs_clarification"):
        return "clarify", "当前信息不足，停止继续执行并向用户发起澄清。"
    row_count = int(result.get("row_count", 0) or 0)
    if row_count > 0:
        return "finalize", "当前结果已足够，结束本轮 Skill 执行并输出最终结论。"
    return "clarify", "当前没有得到足够结果，停止继续执行并请用户补充更明确条件。"


def dump_payload(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False)
