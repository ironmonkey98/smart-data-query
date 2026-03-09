from __future__ import annotations

import os
import re
from datetime import date, timedelta

from llm_enhancer import plan_parking_question


METRIC_ALIASES = {
    "成交额": ("paid_amount", "成交额"),
    "销售额": ("paid_amount", "成交额"),
    "收入": ("total_revenue", "总收入"),
    "总收入": ("total_revenue", "总收入"),
    "临停收入": ("temp_revenue", "临停收入"),
    "包月收入": ("monthly_revenue", "包月收入"),
}

PARKING_DOMAIN_KEYWORDS = (
    "车场", "停车", "场子", "停放", "出场", "入场", "岗亭", "开闸", "缴费",
)
MANAGEMENT_REPORT_KEYWORDS = ("周报", "日报", "管理层", "经营报告", "经营简报", "老板")
PARKING_OPERATION_KEYWORDS = ("经营", "情况", "表现", "异常", "有问题", "风险", "今日", "今天", "简报", "盘子")
PARKING_INTENTS = {
    "parking_management_daily_report",
    "parking_management_report",
    "parking_anomaly_diagnosis",
    "parking_flow_efficiency_analysis",
    "parking_revenue_analysis",
}
SEMANTIC_BUSINESS_GOALS = {
    "management_reporting",
    "risk_detection",
    "efficiency_diagnosis",
    "revenue_diagnosis",
}


def normalize_question(
    question: str,
    schema_text: str,
    glossary_text: str,
    planner=None,
    llm_base_url: str | None = None,
    llm_model: str | None = None,
) -> dict:
    if _is_parking_question(question):
        return _normalize_parking_question(
            question=question,
            schema_text=schema_text,
            glossary_text=glossary_text,
            planner=planner,
            llm_base_url=llm_base_url,
            llm_model=llm_model,
        )

    metric_field, metric_label = _detect_metric(question, glossary_text)
    regions = [region for region in ("华东", "华南", "华北", "华中", "华西") if region in question]
    intent = _detect_intent(question)
    chart_type = _detect_chart_type(question, intent)
    time_range = _detect_time_range(question)
    filters = []
    if regions:
        filters.append({"field": "region", "operator": "in", "values": regions})
    if "排除退款" in question or "去掉退款" in question:
        filters.append({"field": "order_status", "operator": "=", "value": "paid"})

    dimensions = ["region"] if regions else []
    return {
        "intent": intent,
        "metric": {
            "field": metric_field,
            "label": metric_label,
            "aggregation": "sum",
        },
        "dimensions": dimensions,
        "time_field": "order_date",
        "time_granularity": "day",
        "time_range": time_range,
        "filters": filters,
        "chart": {
            "type": chart_type,
            "x_field": "order_date",
            "y_field": metric_label,
            "series_field": "region" if regions else None,
        },
        "sort": [{"field": "order_date", "direction": "asc"}],
        "assumptions": _build_assumptions(question, metric_label, regions),
        "schema_hint": schema_text.strip().splitlines()[0] if schema_text.strip() else "",
    }


def _detect_metric(question: str, glossary_text: str) -> tuple[str, str]:
    for alias, mapping in METRIC_ALIASES.items():
        if alias in question:
            return mapping
    glossary_match = re.search(r"成交额\s*=\s*(\w+)", glossary_text)
    if glossary_match:
        return glossary_match.group(1), "成交额"
    return "paid_amount", "成交额"


def _detect_intent(question: str) -> str:
    if _is_daily_management_report_question(question):
        return "parking_management_daily_report"
    if "车场" in question and any(keyword in question for keyword in ("周报", "日报", "管理层", "经营报告", "老板", "简报")):
        return "parking_management_report"
    if "停车" in question and any(keyword in question for keyword in ("周报", "日报", "管理层", "经营报告", "老板", "简报")):
        return "parking_management_report"
    if "场子" in question and any(keyword in question for keyword in ("老板", "简报")):
        return "parking_management_report"
    if ("场子" in question or "停车" in question or "车场" in question) and ("有问题" in question or "异常" in question or "风险" in question):
        return "parking_anomaly_diagnosis"
    if "车场" in question and ("车流" in question or "利用率" in question):
        return "parking_flow_efficiency_analysis"
    if "车场" in question and ("异常" in question or "风险" in question or "支付失败" in question):
        return "parking_anomaly_diagnosis"
    if "车场" in question and ("收入" in question or "营收" in question):
        return "parking_revenue_analysis"
    if "趋势" in question or "最近" in question:
        return "trend_compare"
    if "对比" in question:
        return "compare"
    return "summary"


def _detect_chart_type(question: str, intent: str) -> str:
    return "line"


def _detect_time_range(question: str) -> dict:
    today = date.today()

    if "今天" in question or "今日" in question:
        return {
            "preset": "today",
            "start": today.isoformat(),
            "end": today.isoformat(),
        }

    if "最近3天" in question or "近3天" in question:
        return {
            "preset": "last_3_days",
            "start": (today - timedelta(days=2)).isoformat(),
            "end": today.isoformat(),
        }
    if "最近7天" in question or "近7天" in question:
        return {
            "preset": "last_7_days",
            "start": (today - timedelta(days=6)).isoformat(),
            "end": today.isoformat(),
        }
    if "最近14天" in question or "近14天" in question:
        return {
            "preset": "last_14_days",
            "start": (today - timedelta(days=13)).isoformat(),
            "end": today.isoformat(),
        }
    if "最近30天" in question or "近30天" in question:
        return {
            "preset": "last_30_days",
            "start": (today - timedelta(days=29)).isoformat(),
            "end": today.isoformat(),
        }

    if "本周" in question or "这周" in question:
        monday = today - timedelta(days=today.weekday())
        return {
            "preset": "this_week",
            "start": monday.isoformat(),
            "end": today.isoformat(),
        }
    if "上周" in question:
        last_monday = today - timedelta(days=today.weekday() + 7)
        last_sunday = last_monday + timedelta(days=6)
        return {
            "preset": "last_week",
            "start": last_monday.isoformat(),
            "end": last_sunday.isoformat(),
        }

    if "本月" in question or "这个月" in question:
        month_start = today.replace(day=1)
        return {
            "preset": "this_month",
            "start": month_start.isoformat(),
            "end": today.isoformat(),
        }
    if "上个月" in question or "上月" in question:
        first_of_this_month = today.replace(day=1)
        last_day_of_last_month = first_of_this_month - timedelta(days=1)
        first_day_of_last_month = last_day_of_last_month.replace(day=1)
        return {
            "preset": "last_month",
            "start": first_day_of_last_month.isoformat(),
            "end": last_day_of_last_month.isoformat(),
        }

    if "今年" in question:
        year_start = today.replace(month=1, day=1)
        return {
            "preset": "this_year",
            "start": year_start.isoformat(),
            "end": today.isoformat(),
        }

    return {"preset": "all", "start": None, "end": None}


def _build_assumptions(question: str, metric_label: str, regions: list[str]) -> list[str]:
    assumptions = [f"{metric_label}按 paid_amount 汇总"]
    if "排除退款" in question:
        assumptions.append("仅统计 order_status=paid 的记录")
    if regions:
        assumptions.append(f"区域范围限定为：{'、'.join(regions)}")
    return assumptions


def _normalize_parking_question(
    question: str,
    schema_text: str,
    glossary_text: str,
    planner=None,
    llm_base_url: str | None = None,
    llm_model: str | None = None,
) -> dict:
    semantic_plan = _plan_parking_question(
        question=question,
        schema_text=schema_text,
        glossary_text=glossary_text,
        planner=planner,
        llm_base_url=llm_base_url,
        llm_model=llm_model,
    )
    if semantic_plan is not None:
        task = _map_semantic_plan_to_task(
            semantic_plan=semantic_plan,
            question=question,
            schema_text=schema_text,
            glossary_text=glossary_text,
        )
        task["planner_mode"] = "llm"
        task["semantic_plan"] = semantic_plan
        return task

    task = _build_rule_parking_task(question, schema_text, glossary_text)
    task["planner_mode"] = "rule_fallback" if planner or os.getenv("OPENAI_API_KEY") else "rule"
    task["semantic_plan"] = _build_rule_semantic_plan(question, task)
    return task


def _plan_parking_question(
    question: str,
    schema_text: str,
    glossary_text: str,
    planner,
    llm_base_url: str | None,
    llm_model: str | None,
) -> dict | None:
    planner_func = planner
    if planner_func is None and os.getenv("OPENAI_API_KEY"):
        planner_func = lambda q, s, g: plan_parking_question(
            question=q,
            schema_text=s,
            glossary_text=g,
            base_url=llm_base_url,
            model=llm_model,
        )
    if planner_func is None:
        return None

    try:
        plan = planner_func(question, schema_text, glossary_text)
    except Exception:
        return None
    if not isinstance(plan, dict):
        return None
    return _normalize_semantic_plan(plan)


def _normalize_semantic_plan(plan: dict) -> dict | None:
    if plan.get("domain") not in {None, "", "parking_ops"}:
        return None
    if plan.get("business_goal") not in SEMANTIC_BUSINESS_GOALS:
        return None
    if not isinstance(plan.get("focus_metrics"), list):
        return None
    normalized = {
        "domain": "parking_ops",
        "business_goal": plan["business_goal"],
        "analysis_job": plan.get("analysis_job"),
        "decision_scope": plan.get("decision_scope"),
        "deliverable": plan.get("deliverable"),
        "time_scope": _normalize_semantic_time_scope(plan.get("time_scope")),
        "focus_entities": _coerce_focus_entities(plan.get("focus_entities")),
        "focus_dimensions": _normalize_focus_dimensions(plan.get("focus_dimensions")),
        "focus_metrics": [metric for metric in plan.get("focus_metrics", []) if isinstance(metric, str)],
        "implicit_requirements": _normalize_string_list(plan.get("implicit_requirements")),
        "missing_information": _normalize_string_list(plan.get("missing_information")),
    }
    return normalized


def _normalize_semantic_time_scope(time_scope) -> dict:
    if not isinstance(time_scope, dict):
        return {"preset": "all", "start": None, "end": None}
    preset = time_scope.get("preset") or "all"
    start = time_scope.get("start")
    end = time_scope.get("end")
    return {"preset": preset, "start": start, "end": end}


def _normalize_focus_dimensions(focus_dimensions) -> list[str]:
    if not isinstance(focus_dimensions, list):
        return ["parking_lot"]
    cleaned = [item for item in focus_dimensions if isinstance(item, str) and item.strip()]
    return cleaned or ["parking_lot"]


def _normalize_string_list(values) -> list[str]:
    if not isinstance(values, list):
        return []
    return [value for value in values if isinstance(value, str) and value.strip()]


def _map_semantic_plan_to_task(
    semantic_plan: dict,
    question: str,
    schema_text: str,
    glossary_text: str,
) -> dict:
    intent = _map_semantic_plan_to_intent(semantic_plan, question)
    time_range = _coerce_parking_time_range(semantic_plan.get("time_scope"), question, intent)
    focus_metrics = _coerce_focus_metrics(semantic_plan.get("focus_metrics"), intent, question)
    task = _build_parking_task(
        question=question,
        schema_text=schema_text,
        glossary_text=glossary_text,
        intent=intent,
        time_range=time_range,
        focus_metrics=focus_metrics,
        focus_entities=semantic_plan.get("focus_entities", []),
    )
    task["report_type"] = _resolve_report_type(intent, semantic_plan.get("deliverable"))
    if "time_scope" in semantic_plan.get("missing_information", []):
        task["needs_clarification"] = True
        task["clarifying_question"] = "请先明确时间范围，例如最近7天、最近30天或本月。"
    return task


def _map_semantic_plan_to_intent(semantic_plan: dict, question: str) -> str:
    business_goal = semantic_plan.get("business_goal")
    analysis_job = semantic_plan.get("analysis_job")
    decision_scope = semantic_plan.get("decision_scope")
    deliverable = semantic_plan.get("deliverable")

    if business_goal == "management_reporting" and analysis_job == "operational_overview" and decision_scope == "executive":
        if deliverable == "daily_brief":
            return "parking_management_daily_report"
        return "parking_management_report"
    if business_goal == "risk_detection" and analysis_job == "anomaly_focus":
        return "parking_anomaly_diagnosis"
    if business_goal == "efficiency_diagnosis" and analysis_job == "flow_or_occupancy":
        return "parking_flow_efficiency_analysis"
    if business_goal == "revenue_diagnosis" and analysis_job == "revenue_focus":
        return "parking_revenue_analysis"
    return _detect_intent(question)


def _build_rule_semantic_plan(question: str, task: dict) -> dict:
    intent = task["intent"]
    business_goal, analysis_job, deliverable = _intent_to_semantic_defaults(intent)
    missing_information = ["time_scope"] if task.get("needs_clarification") else []
    return {
        "domain": "parking_ops",
        "business_goal": business_goal,
        "analysis_job": analysis_job,
        "decision_scope": "executive" if intent in {"parking_management_report", "parking_management_daily_report"} else "operations",
        "deliverable": deliverable,
        "time_scope": task["time_range"],
        "focus_entities": task.get("focus_entities", []),
        "focus_dimensions": ["parking_lot"],
        "focus_metrics": task.get("focus_metrics", []),
        "implicit_requirements": ["summary_first"] if intent in {"parking_management_report", "parking_management_daily_report"} else [],
        "missing_information": missing_information,
    }


def _intent_to_semantic_defaults(intent: str) -> tuple[str, str, str | None]:
    if intent == "parking_management_daily_report":
        return "management_reporting", "operational_overview", "daily_brief"
    if intent == "parking_management_report":
        return "management_reporting", "operational_overview", "web_report"
    if intent == "parking_anomaly_diagnosis":
        return "risk_detection", "anomaly_focus", None
    if intent == "parking_flow_efficiency_analysis":
        return "efficiency_diagnosis", "flow_or_occupancy", None
    if intent == "parking_revenue_analysis":
        return "revenue_diagnosis", "revenue_focus", None
    return "management_reporting", "operational_overview", None


def _build_rule_parking_task(question: str, schema_text: str, glossary_text: str) -> dict:
    intent = _detect_intent(question)
    return _build_parking_task(
        question=question,
        schema_text=schema_text,
        glossary_text=glossary_text,
        intent=intent,
        time_range=_detect_time_range(question),
        focus_metrics=_detect_focus_metrics(question, intent),
        focus_entities=[],
    )


def _build_parking_task(
    question: str,
    schema_text: str,
    glossary_text: str,
    intent: str,
    time_range: dict,
    focus_metrics: list[str],
    focus_entities: list[str],
) -> dict:
    if intent == "parking_management_daily_report" and time_range["preset"] == "all":
        today = date.today().isoformat()
        time_range = {"preset": "today", "start": today, "end": today}

    metric_field, metric_label = _resolve_parking_metric(intent, glossary_text)
    task = {
        "intent": intent,
        "domain": "parking_ops",
        "metric": {
            "field": metric_field,
            "label": metric_label,
            "aggregation": "sum",
        },
        "entity_field": "parking_lot",
        "time_field": "stat_date",
        "time_granularity": "day",
        "time_range": time_range,
        "comparison_range": _build_comparison_range(time_range),
        "focus_metrics": focus_metrics,
        "focus_entities": focus_entities,
        "chart": {
            "type": "line",
            "x_field": "stat_date",
            "y_field": metric_label,
            "series_field": "parking_lot",
        },
        "assumptions": _build_parking_assumptions(question, metric_label, focus_metrics),
        "schema_hint": schema_text.strip().splitlines()[0] if schema_text.strip() else "",
    }
    if time_range["preset"] == "all":
        task["needs_clarification"] = True
        task["clarifying_question"] = "请先明确时间范围，例如最近7天、最近30天或本月。"
    else:
        task["needs_clarification"] = False
        task["clarifying_question"] = None
    task["report_type"] = _resolve_report_type(intent, None)
    return task


def _resolve_parking_metric(intent: str, glossary_text: str) -> tuple[str, str]:
    if intent == "parking_flow_efficiency_analysis":
        return "entry_count", "入场车次"
    if intent == "parking_anomaly_diagnosis":
        return "payment_failure_rate", "支付失败率"
    if intent in {"parking_management_report", "parking_management_daily_report", "parking_revenue_analysis"}:
        return "total_revenue", "总收入"
    return _detect_metric("", glossary_text)


def _detect_focus_metrics(question: str, intent: str) -> list[str]:
    focus_metrics = []
    if "支付失败" in question:
        focus_metrics.append("payment_failure_rate")
    if "异常开闸" in question:
        focus_metrics.append("abnormal_open_count")
    if "免费放行" in question:
        focus_metrics.append("free_release_count")
    if "车流" in question:
        focus_metrics.append("entry_count")
    if "利用率" in question:
        focus_metrics.append("occupancy_rate")
    if not focus_metrics and intent == "parking_anomaly_diagnosis":
        return ["payment_failure_rate", "abnormal_open_count", "free_release_count", "occupancy_rate"]
    if not focus_metrics and intent == "parking_flow_efficiency_analysis":
        return ["entry_count", "occupancy_rate"]
    if not focus_metrics and intent in {"parking_management_report", "parking_management_daily_report"}:
        return ["total_revenue", "entry_count", "occupancy_rate", "payment_failure_rate", "abnormal_open_count"]
    return focus_metrics


def _coerce_focus_metrics(focus_metrics, intent: str, question: str) -> list[str]:
    valid_metrics = {
        "total_revenue",
        "entry_count",
        "occupancy_rate",
        "payment_failure_rate",
        "abnormal_open_count",
        "free_release_count",
    }
    if isinstance(focus_metrics, list):
        cleaned = [item for item in focus_metrics if isinstance(item, str) and item in valid_metrics]
        if cleaned:
            return cleaned
    return _detect_focus_metrics(question, intent)


def _coerce_focus_entities(focus_entities) -> list[str]:
    if not isinstance(focus_entities, list):
        return []
    return [item.strip() for item in focus_entities if isinstance(item, str) and item.strip()]


def _coerce_parking_time_range(time_range, question: str, intent: str) -> dict:
    if isinstance(time_range, dict):
        preset = time_range.get("preset")
        start = time_range.get("start")
        end = time_range.get("end")
        if preset and (preset == "all" or (start and end)):
            return {"preset": preset, "start": start, "end": end}
    detected = _detect_time_range(question)
    if intent == "parking_management_daily_report" and detected["preset"] == "all":
        today = date.today().isoformat()
        return {"preset": "today", "start": today, "end": today}
    return detected


def _resolve_report_type(intent: str, report_type: str | None) -> str | None:
    if intent == "parking_management_daily_report":
        return "daily"
    if intent == "parking_management_report":
        if report_type == "daily_brief":
            return "daily"
        return "weekly" if report_type not in {"daily", "weekly"} else report_type
    return None


def _is_parking_question(question: str) -> bool:
    if any(keyword in question for keyword in PARKING_DOMAIN_KEYWORDS):
        return True
    return any(keyword in question for keyword in MANAGEMENT_REPORT_KEYWORDS) and any(
        keyword in question for keyword in PARKING_OPERATION_KEYWORDS
    )


def _is_daily_management_report_question(question: str) -> bool:
    daily_keywords = ("日报", "今天", "今日")
    report_tone_keywords = ("老板", "管理层", "经营", "情况", "简报", "报告")
    return any(keyword in question for keyword in daily_keywords) and any(
        keyword in question for keyword in report_tone_keywords
    )


def _build_comparison_range(time_range: dict) -> dict:
    if not time_range.get("start") or not time_range.get("end"):
        return {"start": None, "end": None}
    start_date = date.fromisoformat(time_range["start"])
    end_date = date.fromisoformat(time_range["end"])
    days = (end_date - start_date).days + 1
    previous_end = start_date - timedelta(days=1)
    previous_start = previous_end - timedelta(days=days - 1)
    return {
        "start": previous_start.isoformat(),
        "end": previous_end.isoformat(),
    }


def _build_parking_assumptions(question: str, metric_label: str, focus_metrics: list[str]) -> list[str]:
    assumptions = [f"{metric_label}按日汇总并按车场对比"]
    if "原因" in question:
        assumptions.append("归因优先从车流、支付失败、异常开闸、免费放行、利用率变化判断")
    if "车流" in question or "利用率" in question:
        assumptions.append("效率分析优先比较车流、利用率和车位周转的阶段性变化")
    if "周报" in question or "日报" in question or "管理层" in question or "老板" in question or "简报" in question:
        assumptions.append("综合报告需汇总收入、异常、车流效率并提炼管理层动作")
    if focus_metrics:
        assumptions.append(f"重点诊断指标：{', '.join(focus_metrics)}")
    return assumptions
