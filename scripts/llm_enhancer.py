from __future__ import annotations

import json
import os
from urllib.error import URLError
from urllib.request import Request, urlopen


def enhance_analysis(
    task: dict,
    analysis: dict,
    enable_llm: bool = False,
    api_key: str | None = None,
    base_url: str | None = None,
    model: str | None = None,
) -> dict:
    narrative = _build_rule_narrative(task, analysis)
    suggestions = _build_follow_up_suggestions(task, analysis)

    if not enable_llm:
        return {
            "mode": "rule",
            "narrative": narrative,
            "follow_up_suggestions": suggestions,
        }

    resolved_api_key = api_key or os.getenv("OPENAI_API_KEY")
    resolved_base_url = (base_url or os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1").rstrip("/")
    resolved_model = model or os.getenv("OPENAI_MODEL") or "gpt-4o-mini"
    if not resolved_api_key:
        return {
            "mode": "rule",
            "narrative": narrative,
            "follow_up_suggestions": suggestions,
            "fallback_reason": "missing_api_key",
        }

    prompt = _build_analysis_prompt(task, analysis, narrative)
    try:
        llm_text = _call_openai_compatible(
            prompt=prompt,
            api_key=resolved_api_key,
            base_url=resolved_base_url,
            model=resolved_model,
        )
        return {
            "mode": "llm",
            "narrative": llm_text,
            "follow_up_suggestions": suggestions,
        }
    except URLError:
        return {
            "mode": "rule",
            "narrative": narrative,
            "follow_up_suggestions": suggestions,
            "fallback_reason": "network_error",
        }


def handle_follow_up(
    follow_up_question: str,
    session_payload: dict,
    enable_llm: bool = False,
    api_key: str | None = None,
    base_url: str | None = None,
    model: str | None = None,
) -> dict:
    task = session_payload.get("task", {})
    analysis = session_payload.get("analysis", {})
    base_narrative = session_payload.get("narrative", {}).get("narrative", "")
    answer = _build_follow_up_answer(follow_up_question, task, analysis, base_narrative)
    if not enable_llm:
        return {"mode": "rule", "answer": answer}

    resolved_api_key = api_key or os.getenv("OPENAI_API_KEY")
    resolved_base_url = (base_url or os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1").rstrip("/")
    resolved_model = model or os.getenv("OPENAI_MODEL") or "gpt-4o-mini"
    if not resolved_api_key:
        return {"mode": "rule", "answer": answer, "fallback_reason": "missing_api_key"}

    prompt = (
        "你是停车经营分析助理。请基于以下已有分析结果回答追问，不要虚构新数据。\n\n"
        f"已有任务：{json.dumps(task, ensure_ascii=False)}\n"
        f"已有分析：{json.dumps(analysis, ensure_ascii=False)}\n"
        f"已有摘要：{base_narrative}\n"
        f"追问：{follow_up_question}\n"
        "请输出简洁专业的中文回答。"
    )
    try:
        llm_text = _call_openai_compatible(
            prompt=prompt,
            api_key=resolved_api_key,
            base_url=resolved_base_url,
            model=resolved_model,
        )
        return {"mode": "llm", "answer": llm_text}
    except URLError:
        return {"mode": "rule", "answer": answer, "fallback_reason": "network_error"}


def plan_parking_question(
    question: str,
    schema_text: str,
    glossary_text: str,
    api_key: str | None = None,
    base_url: str | None = None,
    model: str | None = None,
) -> dict:
    resolved_api_key = api_key or os.getenv("OPENAI_API_KEY")
    if not resolved_api_key:
        raise ValueError("missing_api_key")

    resolved_base_url = (base_url or os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1").rstrip("/")
    resolved_model = model or os.getenv("OPENAI_MODEL") or "gpt-4o-mini"
    prompt = _build_parking_planner_prompt(question, schema_text, glossary_text)
    llm_text = _call_openai_compatible(
        prompt=prompt,
        api_key=resolved_api_key,
        base_url=resolved_base_url,
        model=resolved_model,
    )
    return _parse_planner_json(llm_text)


def _build_rule_narrative(task: dict, analysis: dict) -> str:
    analysis_type = analysis.get("analysis_type")
    if analysis_type == "revenue":
        lot = analysis.get("primary_lot", "目标车场")
        diagnosis = "、".join(item["factor"] for item in analysis.get("diagnosis", [])) or "收入结构变化"
        return f"{lot} 的收入下滑最明显，当前归因重点集中在 {diagnosis}。"
    if analysis_type == "anomaly":
        count = len(analysis.get("diagnosis", []))
        risk_level = analysis.get("risk_level", "unknown")
        return f"当前周期识别到 {count} 个异常车场，整体风险等级为 {risk_level}。"
    if analysis_type == "flow_efficiency":
        lot = analysis.get("primary_lot", "目标车场")
        diagnosis = "、".join(item["factor"] for item in analysis.get("diagnosis", [])) or "车流效率波动"
        return f"{lot} 的车流与利用率下滑最明显，主要信号包括 {diagnosis}。"
    if analysis_type == "management_report":
        overview = analysis.get("overview", {})
        if analysis.get("report_type") == "daily":
            return (
                f"截至 {overview.get('report_date', '今日')}，总收入 {overview.get('total_revenue', 0):.0f}，"
                f"总入场车次 {overview.get('total_entry_count', 0):.0f}，"
                f"平均利用率 {overview.get('avg_occupancy_rate', 0):.1%}。"
            )
        return (
            f"本期总收入 {overview.get('total_revenue', 0):.0f}，总入场车次 {overview.get('total_entry_count', 0):.0f}，"
            f"平均利用率 {overview.get('avg_occupancy_rate', 0):.1%}。"
        )
    return f"已完成 {task.get('intent', 'unknown')} 分析。"


def _build_follow_up_suggestions(task: dict, analysis: dict) -> list[str]:
    analysis_type = analysis.get("analysis_type")
    if analysis_type == "revenue":
        return ["为什么是这个车场？", "支付失败和异常开闸分别影响多大？", "下一步优先处理什么？"]
    if analysis_type == "management_report":
        if analysis.get("report_type") == "daily":
            return ["今天最需要关注哪个车场？", "今日最优先动作是什么？", "今日异常主要集中在哪些指标？"]
        return ["哪一个车场最需要管理层关注？", "本周最优先的动作是什么？", "高风险点集中在哪些指标？"]
    return ["能展开解释原因吗？", "建议动作的优先级是什么？"]


def _build_follow_up_answer(follow_up_question: str, task: dict, analysis: dict, base_narrative: str) -> str:
    question = follow_up_question.strip()
    primary_lot = analysis.get("primary_lot") or (
        analysis.get("focus_lots", [{}])[0].get("parking_lot") if analysis.get("focus_lots") else "目标车场"
    )
    if "为什么" in question:
        if analysis.get("diagnosis"):
            reasons = []
            for item in analysis["diagnosis"]:
                if "message" in item:
                    reasons.append(item["message"])
                elif "reasons" in item:
                    reasons.extend(item["reasons"])
            return f"{primary_lot} 被识别为重点对象，主要因为 " + "；".join(reasons[:3]) + "。"
        return f"{primary_lot} 被识别为重点对象，主要依据是当前分析结果中的异常波动。"
    if "动作" in question or "建议" in question:
        recommendations = analysis.get("recommendations") or analysis.get("priority_actions") or []
        if recommendations:
            return "优先建议是：" + "；".join(recommendations[:3])
    return base_narrative or f"基于当前分析，建议继续关注 {primary_lot} 的关键经营指标。"


def _build_analysis_prompt(task: dict, analysis: dict, rule_narrative: str) -> str:
    return (
        "你是停车经营分析助理。请基于给定的结构化分析结果，写一段适合业务管理者阅读的中文解释。"
        "要求：1）只基于输入数据，不要虚构；2）先结论后原因；3）最多 120 字。\n\n"
        f"任务：{json.dumps(task, ensure_ascii=False)}\n"
        f"分析：{json.dumps(analysis, ensure_ascii=False)}\n"
        f"规则摘要：{rule_narrative}"
    )


def _build_parking_planner_prompt(question: str, schema_text: str, glossary_text: str) -> str:
    schema_excerpt = "\n".join(line for line in schema_text.splitlines()[:12] if line.strip())
    glossary_excerpt = "\n".join(line for line in glossary_text.splitlines()[:20] if line.strip())
    return (
        "你是停车经营问数系统的语义规划器。你的任务是把用户问题拆解成稳定的结构化 JSON。"
        "不要写解释，不要输出 Markdown，只输出一个 JSON 对象。\n\n"
        "要求：\n"
        "1. domain 固定为 parking_ops。\n"
        "2. intent 只能是以下之一："
        "parking_management_daily_report, parking_management_report, parking_anomaly_diagnosis, "
        "parking_flow_efficiency_analysis, parking_revenue_analysis。\n"
        "3. time_range 必须包含 preset, start, end；如果缺时间范围可返回 preset=all 且 start/end=null。\n"
        "4. report_type 只能是 daily 或 weekly；非报表类可返回 null。\n"
        "5. focus_entities 只放车场名；focus_metrics 只放字段名："
        "total_revenue, entry_count, occupancy_rate, payment_failure_rate, abnormal_open_count, free_release_count。\n"
        "6. 如果问题缺关键口径，需要设置 needs_clarification=true，并给出 clarification_question。\n\n"
        "输出 JSON 模板：\n"
        "{"
        "\"domain\":\"parking_ops\","
        "\"intent\":\"parking_management_report\","
        "\"report_type\":\"weekly\","
        "\"time_range\":{\"preset\":\"last_7_days\",\"start\":\"2026-03-03\",\"end\":\"2026-03-09\"},"
        "\"focus_entities\":[],"
        "\"focus_metrics\":[\"total_revenue\"],"
        "\"needs_clarification\":false,"
        "\"clarification_question\":null"
        "}\n\n"
        f"Schema 摘要：\n{schema_excerpt}\n\n"
        f"术语摘要：\n{glossary_excerpt}\n\n"
        f"用户问题：{question}"
    )


def _parse_planner_json(llm_text: str) -> dict:
    cleaned = llm_text.strip()
    if cleaned.startswith("```"):
        parts = cleaned.split("```")
        cleaned = next((part for part in parts if "{" in part and "}" in part), cleaned)
        cleaned = cleaned.replace("json", "", 1).strip()
    return json.loads(cleaned)


def _call_openai_compatible(prompt: str, api_key: str, base_url: str, model: str) -> str:
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "你是专业的停车经营分析助理。"},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
    }
    request = Request(
        url=f"{base_url}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    with urlopen(request, timeout=30) as response:
        data = json.loads(response.read().decode("utf-8"))
    return data["choices"][0]["message"]["content"].strip()
