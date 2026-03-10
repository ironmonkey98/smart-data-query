from __future__ import annotations

import os
import re
import sqlite3
from datetime import date, timedelta
from pathlib import Path

from llm_enhancer import build_default_parking_planner


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
    "parking_period_assessment",
    "parking_anomaly_diagnosis",
    "parking_flow_efficiency_analysis",
    "parking_revenue_analysis",
    "parking_relational_query",
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
    parking_task = _normalize_parking_question(
        question=question,
        schema_text=schema_text,
        glossary_text=glossary_text,
        planner=planner,
        llm_base_url=llm_base_url,
        llm_model=llm_model,
        allow_rule_fallback=False,
    )
    if parking_task is not None:
        return parking_task

    if _is_parking_question(question):
        return _normalize_parking_question(
            question=question,
            schema_text=schema_text,
            glossary_text=glossary_text,
            planner=planner,
            llm_base_url=llm_base_url,
            llm_model=llm_model,
            allow_rule_fallback=True,
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
    if any(token in question for token in ("收费流水", "通行记录", "联起来看", "联查", "打通", "车牌", "单位车位", "车位收入", "支付方式", "基础数据")):
        return "parking_relational_query"
    if any(token in question for token in ("经营盘子", "盘子", "捋一版", "整体情况", "综合看")) and _is_parking_question(question):
        return "parking_management_report"
    if "车场" in question and any(keyword in question for keyword in ("周报", "日报", "管理层", "经营报告", "老板", "简报")):
        return "parking_management_report"
    if "停车" in question and any(keyword in question for keyword in ("周报", "日报", "管理层", "经营报告", "老板", "简报")):
        return "parking_management_report"
    if "场子" in question and any(keyword in question for keyword in ("老板", "简报")):
        return "parking_management_report"
    if ("场子" in question or "停车" in question or "车场" in question) and (
        "有问题" in question or "异常" in question or "风险" in question or "不正常" in question
    ):
        return "parking_anomaly_diagnosis"
    if ("场子" in question or "车场" in question) and any(keyword in question for keyword in ("免费放行", "支付失败", "利用率", "最不正常")):
        return "parking_anomaly_diagnosis"
    if "车场" in question and ("车流" in question or "利用率" in question):
        return "parking_flow_efficiency_analysis"
    if "车场" in question and ("异常" in question or "风险" in question or "支付失败" in question):
        return "parking_anomaly_diagnosis"
    if "车场" in question and ("收入" in question or "营收" in question):
        return "parking_revenue_analysis"
    if _is_parking_question(question) and any(keyword in question for keyword in ("好转", "变坏")):
        return "parking_period_assessment"
    if _is_parking_question(question) and any(keyword in question for keyword in ("情况", "怎么样")):
        return "parking_management_report"
    if "趋势" in question or "最近" in question:
        return "trend_compare"
    if "对比" in question:
        return "compare"
    return "summary"


def _detect_chart_type(question: str, intent: str) -> str:
    return "line"


def _detect_time_range(question: str) -> dict:
    today = date.today()
    relative_month_match = re.search(r"(今年|去年)\s*([1-9]|1[0-2])\s*月", question)
    if relative_month_match:
        target_year = today.year if relative_month_match.group(1) == "今年" else today.year - 1
        target_month = int(relative_month_match.group(2))
        month_start = date(target_year, target_month, 1)
        month_end = _month_end(month_start)
        preset = "this_year_month" if relative_month_match.group(1) == "今年" else "last_year_month"
        return {
            "preset": preset,
            "start": month_start.isoformat(),
            "end": month_end.isoformat(),
        }

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


def _month_end(month_start: date) -> date:
    if month_start.month == 12:
        return month_start.replace(day=31)
    return month_start.replace(month=month_start.month + 1, day=1) - timedelta(days=1)


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
    allow_rule_fallback: bool = True,
) -> dict | None:
    semantic_plan, planner_state = _plan_parking_question(
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

    if planner_state == "failed" and _should_clarify_after_planner_failure(question):
        return _build_planner_failure_task(question, schema_text, glossary_text)

    if not allow_rule_fallback:
        return None

    task = _build_rule_parking_task(question, schema_text, glossary_text)
    task["planner_mode"] = "planner_error" if planner_state == "failed" else ("rule_fallback" if planner_state == "available" else "rule")
    task["semantic_plan"] = _build_rule_semantic_plan(question, task)
    if planner_state == "failed" and _should_clarify_after_planner_failure(question, task):
        task["needs_clarification"] = True
        task["clarifying_question"] = _planner_failure_clarification_question(question)
        task["time_was_defaulted"] = False
        task["time_default_reason"] = None
        task["semantic_plan"]["missing_information"] = sorted(
            set(task["semantic_plan"].get("missing_information", []) + ["time_scope"])
        )
    return task


def _plan_parking_question(
    question: str,
    schema_text: str,
    glossary_text: str,
    planner,
    llm_base_url: str | None,
    llm_model: str | None,
) -> tuple[dict | None, str]:
    planner_func = planner
    if planner_func is None:
        planner_func = build_default_parking_planner(
            entity_context=_load_parking_entity_context(),
            openai_base_url=llm_base_url,
            openai_model=llm_model,
        )
    if planner_func is None:
        return None, "unavailable"

    try:
        plan = planner_func(question, schema_text, glossary_text)
    except Exception:
        return None, "failed"
    if not isinstance(plan, dict):
        return None, "failed"
    return _normalize_semantic_plan(plan), "available"


def _load_parking_entity_context() -> str:
    db_path = Path(__file__).resolve().parent.parent / "data" / "sample_parking_ops.db"
    if not db_path.exists():
        return ""
    try:
        connection = sqlite3.connect(str(db_path))
        try:
            rows = connection.execute(
                "SELECT parking_lot_name FROM parking_lots ORDER BY parking_lot_name"
            ).fetchall()
        finally:
            connection.close()
    except sqlite3.Error:
        return ""
    names = [str(row[0]).strip() for row in rows if row and str(row[0]).strip()]
    return "\n".join(names)


def _build_planner_failure_task(question: str, schema_text: str, glossary_text: str) -> dict:
    task = _build_rule_parking_task(question, schema_text, glossary_text)
    if not str(task.get("intent", "")).startswith("parking_"):
        task["intent"] = _infer_parking_intent_from_failed_planner(question)
        metric_field, metric_label = _resolve_parking_metric(task["intent"], glossary_text)
        task["metric"] = {
            "field": metric_field,
            "label": metric_label,
            "aggregation": "sum",
        }
        task["query_profile"] = _resolve_parking_query_profile(question, task["intent"], task.get("focus_metrics", []))
        task["relations"] = _build_parking_relations(task["query_profile"])
        task["entities"] = _build_parking_entities(task["query_profile"], task.get("focus_entities", []))
        task["chart"] = _resolve_parking_chart(task["query_profile"], metric_label)
        task["report_type"] = _resolve_report_type(task["intent"], None)
    task["domain"] = "parking_ops"
    task["planner_mode"] = "planner_error"
    task["needs_clarification"] = True
    task["clarifying_question"] = _planner_failure_clarification_question(question)
    task["time_was_defaulted"] = False
    task["time_default_reason"] = None
    task["semantic_plan"] = _build_rule_semantic_plan(question, task)
    task["semantic_plan"]["missing_information"] = sorted(
        set(task["semantic_plan"].get("missing_information", []) + ["time_scope"])
    )
    return task


def _infer_parking_intent_from_failed_planner(question: str) -> str:
    if any(token in question for token in ("有问题", "异常", "风险", "不正常")):
        return "parking_anomaly_diagnosis"
    if any(token in question for token in ("收入", "营收", "最差", "最好")):
        return "parking_period_assessment"
    if any(token in question for token in ("老板", "管理层", "日报", "周报", "报告", "简报", "情况", "经营")):
        return "parking_management_report"
    return "parking_query_skill"


def _planner_failure_clarification_question(question: str) -> str:
    if any(token in question for token in ("去年", "前年", "今年", "本年")):
        return "当前语义规划暂时不可用。请重试，或先明确时间口径，例如“去年全年”或“去年2月”。"
    return "当前语义规划暂时不可用。请重试，或补充更明确的时间范围、车场名称和判断口径。"


def _should_clarify_after_planner_failure(question: str, task: dict | None = None) -> bool:
    if any(token in question for token in ("去年", "前年", "今年", "上半年", "下半年")):
        return True
    if not _is_parking_question(question) and _question_mentions_parking_entity(question):
        return True
    if task and task.get("time_was_defaulted") and any(token in question for token in ("哪天", "为什么", "最差", "最好")):
        return True
    return False


def _question_mentions_parking_entity(question: str) -> bool:
    entity_context = _load_parking_entity_context()
    if not entity_context:
        return False
    compact_question = re.sub(r"\s+", "", question)
    tokens = {
        token
        for token in re.findall(r"[\u4e00-\u9fffA-Za-z0-9]{2,6}", compact_question)
        if token not in {"去年", "前年", "今年", "收入", "营收", "情况", "为什么", "哪天", "最差", "最好", "停车", "车场", "场子", "问题", "经营"}
    }
    if not tokens:
        return False
    for lot_name in entity_context.splitlines():
        normalized_name = re.sub(r"\s+", "", lot_name)
        if any(token in normalized_name for token in tokens):
            return True
    return False


def _normalize_semantic_plan(plan: dict) -> dict | None:
    domain = plan.get("domain")
    if domain in {"non_parking", "sales"}:
        return None
    if domain not in {None, "", "parking_ops"}:
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
        "entities": _normalize_string_list(plan.get("entities")),
        "relations": _normalize_string_list(plan.get("relations")),
        "focus_entities": _coerce_focus_entities(plan.get("focus_entities")),
        "focus_dimensions": _normalize_focus_dimensions(plan.get("focus_dimensions")),
        "focus_metrics": [metric for metric in plan.get("focus_metrics", []) if isinstance(metric, str)],
        "query_profile": _normalize_query_profile(plan.get("query_profile")),
        "sub_questions": _normalize_sub_questions(plan.get("sub_questions")),
        "constraints": _normalize_constraints(plan.get("constraints")),
        "implicit_requirements": _normalize_string_list(plan.get("implicit_requirements")),
        "missing_information": _normalize_string_list(plan.get("missing_information")),
    }
    return normalized


def _normalize_semantic_time_scope(time_scope) -> dict:
    if not isinstance(time_scope, dict):
        return {"kind": "preset", "preset": "all", "anchor": None, "start": None, "end": None}
    raw_preset = time_scope.get("preset")
    kind = time_scope.get("kind") or ("preset" if raw_preset else "custom")
    start = time_scope.get("start")
    end = time_scope.get("end")
    preset = raw_preset if raw_preset is not None else ("all" if not start and not end and kind == "preset" else None)
    anchor = time_scope.get("anchor") if isinstance(time_scope.get("anchor"), dict) else None
    return {"kind": kind, "preset": preset, "anchor": anchor, "start": start, "end": end}


def _normalize_focus_dimensions(focus_dimensions) -> list[str]:
    if not isinstance(focus_dimensions, list):
        return ["parking_lot"]
    cleaned = [item for item in focus_dimensions if isinstance(item, str) and item.strip()]
    return cleaned or ["parking_lot"]


def _normalize_string_list(values) -> list[str]:
    if not isinstance(values, list):
        return []
    return [value for value in values if isinstance(value, str) and value.strip()]


def _normalize_constraints(values) -> dict:
    if not isinstance(values, dict):
        return {}
    return {
        key: value
        for key, value in values.items()
        if isinstance(key, str) and key.strip()
    }


def _normalize_query_profile(value) -> str | None:
    valid_profiles = {
        "parking_daily_overview_join",
        "payment_passage_reconciliation_by_date",
        "payment_passage_reconciliation_by_plate",
        "lot_capacity_efficiency_ranking",
        "payment_method_risk_breakdown",
    }
    if isinstance(value, str) and value in valid_profiles:
        return value
    return None


def _normalize_sub_questions(values) -> list[dict]:
    if not isinstance(values, list):
        return []
    normalized = []
    for item in values:
        if not isinstance(item, dict):
            continue
        kind = item.get("kind")
        if not isinstance(kind, str) or not kind.strip():
            continue
        normalized.append(
            {
                "kind": kind.strip(),
                "query_profile": _normalize_query_profile(item.get("query_profile")),
                "question": item.get("question") if isinstance(item.get("question"), str) else None,
                "focus_metrics": _normalize_string_list(item.get("focus_metrics")),
                "constraints": _normalize_constraints(item.get("constraints")),
            }
        )
    return normalized


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
        query_profile_override=semantic_plan.get("query_profile"),
    )
    task["report_type"] = _resolve_report_type(intent, semantic_plan.get("deliverable"))
    if "time_scope" in semantic_plan.get("missing_information", []) and _requires_explicit_time_range(question):
        task["needs_clarification"] = True
        task["clarifying_question"] = "请先明确时间范围，例如最近7天、最近30天或本月。"
    task["entities"] = semantic_plan.get("entities", task.get("entities", []))
    task["relations"] = semantic_plan.get("relations", task.get("relations", []))
    task["constraints"] = {
        **task.get("constraints", {}),
        **semantic_plan.get("constraints", {}),
    }
    task["sub_questions"] = semantic_plan.get("sub_questions", [])
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
    if business_goal == "management_reporting" and analysis_job == "period_assessment":
        return "parking_period_assessment"
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
        "entities": task.get("entities", []),
        "relations": task.get("relations", []),
        "focus_entities": task.get("focus_entities", []),
        "focus_dimensions": ["parking_lot"],
        "focus_metrics": task.get("focus_metrics", []),
        "query_profile": task.get("query_profile"),
        "sub_questions": task.get("sub_questions", []),
        "constraints": task.get("constraints", {}),
        "implicit_requirements": ["summary_first"] if intent in {"parking_management_report", "parking_management_daily_report"} else [],
        "missing_information": missing_information,
    }


def _intent_to_semantic_defaults(intent: str) -> tuple[str, str, str | None]:
    if intent == "parking_management_daily_report":
        return "management_reporting", "operational_overview", "daily_brief"
    if intent == "parking_management_report":
        return "management_reporting", "operational_overview", "web_report"
    if intent == "parking_period_assessment":
        return "management_reporting", "period_assessment", "summary"
    if intent == "parking_anomaly_diagnosis":
        return "risk_detection", "anomaly_focus", None
    if intent == "parking_flow_efficiency_analysis":
        return "efficiency_diagnosis", "flow_or_occupancy", None
    if intent == "parking_revenue_analysis":
        return "revenue_diagnosis", "revenue_focus", None
    if intent == "parking_relational_query":
        return "risk_detection", "relational_join", None
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
    query_profile_override: str | None = None,
) -> dict:
    time_range, time_was_defaulted, time_default_reason = _finalize_parking_time_range(question, intent, time_range)
    if intent == "parking_management_daily_report" and time_range["preset"] == "all":
        today = date.today().isoformat()
        time_range = {"preset": "today", "start": today, "end": today}

    metric_field, metric_label = _resolve_parking_metric(intent, glossary_text)
    query_profile = query_profile_override or _resolve_parking_query_profile(question, intent, focus_metrics)
    relations = _build_parking_relations(query_profile)
    entities = _build_parking_entities(query_profile, focus_entities)
    constraints = _build_parking_constraints(question, query_profile)
    task = {
        "intent": intent,
        "domain": "parking_ops",
        "query_profile": query_profile,
        "entities": entities,
        "relations": relations,
        "constraints": constraints,
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
        "sub_questions": [],
        "chart": {
            "type": "line",
            "x_field": "stat_date",
            "y_field": metric_label,
            "series_field": "parking_lot",
        },
        "assumptions": _build_parking_assumptions(question, metric_label, focus_metrics),
        "schema_hint": schema_text.strip().splitlines()[0] if schema_text.strip() else "",
        "time_was_defaulted": time_was_defaulted,
        "time_default_reason": time_default_reason,
    }
    task["chart"] = _resolve_parking_chart(task["query_profile"], task["metric"]["label"])
    if time_range["preset"] == "all" and _requires_explicit_time_range(question):
        task["needs_clarification"] = True
        task["clarifying_question"] = "请先明确时间范围，例如最近7天、最近30天或本月。"
    else:
        task["needs_clarification"] = False
        task["clarifying_question"] = None
    _apply_parking_clarification_rules(question, task)
    task["report_type"] = _resolve_report_type(intent, None)
    return task


def _resolve_parking_metric(intent: str, glossary_text: str) -> tuple[str, str]:
    if intent == "parking_flow_efficiency_analysis":
        return "entry_count", "入场车次"
    if intent == "parking_anomaly_diagnosis":
        return "payment_failure_rate", "支付失败率"
    if intent == "parking_relational_query":
        return "total_revenue", "总收入"
    if intent in {"parking_management_report", "parking_management_daily_report", "parking_revenue_analysis", "parking_period_assessment"}:
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
    if not focus_metrics and intent in {"parking_management_report", "parking_management_daily_report", "parking_period_assessment"}:
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
        "revenue_per_space",
        "entry_per_space",
        "payment_count",
        "unmatched_payment_count",
        "unmatched_passage_count",
        "stay_minutes",
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
        kind = time_range.get("kind")
        preset = time_range.get("preset")
        anchor = time_range.get("anchor")
        start = time_range.get("start")
        end = time_range.get("end")
        if (preset or kind) and ((preset == "all") or (start and end)):
            return {
                "preset": preset or kind or "custom",
                "kind": kind or ("preset" if preset else "custom"),
                "anchor": anchor if isinstance(anchor, dict) else None,
                "start": start,
                "end": end,
            }
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
    if intent == "parking_period_assessment":
        return "summary"
    return None


def _finalize_parking_time_range(question: str, intent: str, time_range: dict) -> tuple[dict, bool, str | None]:
    if intent == "parking_management_daily_report" and time_range["preset"] == "all":
        today = date.today().isoformat()
        return {"preset": "today", "start": today, "end": today}, False, None
    if time_range["preset"] != "all":
        return time_range, False, None
    if _requires_explicit_time_range(question):
        return time_range, False, None
    defaulted = {
        "preset": "last_7_days",
        "start": (date.today() - timedelta(days=6)).isoformat(),
        "end": date.today().isoformat(),
    }
    return defaulted, True, "missing_time_scope"


def _requires_explicit_time_range(question: str) -> bool:
    explicit_period_tokens = ("本月", "上月", "上个月", "本周", "上周", "今天", "今日", "最近", "近", "今年")
    if any(token in question for token in explicit_period_tokens):
        return False
    compare_tokens = ("环比", "同比", "对比")
    return any(token in question for token in compare_tokens)


def _is_parking_question(question: str) -> bool:
    if any(keyword in question for keyword in PARKING_DOMAIN_KEYWORDS):
        return True
    if any(keyword in question for keyword in ("收费流水", "通行记录", "车牌", "车位", "支付方式", "基础数据")):
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


def _resolve_parking_query_profile(question: str, intent: str, focus_metrics: list[str]) -> str:
    if any(token in question for token in ("收费流水", "通行记录", "联起来看", "联查")) and any(
        token in question for token in ("收入但没通行", "通行但没收入", "无通行", "无收入")
    ):
        return "payment_passage_reconciliation_by_date"
    if "车牌" in question and any(token in question for token in ("打通", "联起来看", "联查", "收费", "通行")):
        return "payment_passage_reconciliation_by_plate"
    if any(token in question for token in ("单位车位", "车位收入", "基础数据", "总车位")):
        return "lot_capacity_efficiency_ranking"
    if "支付方式" in question or "缴费来源" in question:
        return "payment_method_risk_breakdown"
    if intent in {
        "parking_management_daily_report",
        "parking_management_report",
        "parking_period_assessment",
        "parking_revenue_analysis",
        "parking_anomaly_diagnosis",
        "parking_flow_efficiency_analysis",
    }:
        return "parking_daily_overview_join"
    if any(metric in {"revenue_per_space", "entry_per_space"} for metric in focus_metrics):
        return "lot_capacity_efficiency_ranking"
    return "parking_daily_overview_join"


def _build_parking_relations(query_profile: str) -> list[str]:
    relation_map = {
        "parking_daily_overview_join": [
            "parking_payment_records->parking_lots",
            "parking_passage_records->parking_lots",
        ],
        "payment_passage_reconciliation_by_date": [
            "parking_payment_records->parking_lots",
            "parking_passage_records->parking_lots",
            "payment_daily<->passage_daily",
        ],
        "payment_passage_reconciliation_by_plate": [
            "parking_payment_records->parking_passage_records by lot_id+license_plate+stat_date",
        ],
        "lot_capacity_efficiency_ranking": [
            "parking_payment_records->parking_lots",
            "parking_passage_records->parking_lots",
        ],
        "payment_method_risk_breakdown": [
            "parking_payment_records->parking_lots",
        ],
    }
    return relation_map.get(query_profile, [])


def _build_parking_entities(query_profile: str, focus_entities: list[str]) -> list[str]:
    base = list(focus_entities)
    default_entities = {
        "parking_daily_overview_join": ["parking_lot", "stat_date"],
        "payment_passage_reconciliation_by_date": ["parking_lot", "stat_date"],
        "payment_passage_reconciliation_by_plate": ["parking_lot", "license_plate", "stat_date"],
        "lot_capacity_efficiency_ranking": ["parking_lot"],
        "payment_method_risk_breakdown": ["parking_lot", "payment_method"],
    }
    for entity in default_entities.get(query_profile, []):
        if entity not in base:
            base.append(entity)
    return base


def _build_parking_constraints(question: str, query_profile: str) -> dict:
    constraints = {}
    if "有收入但没通行" in question or "无通行" in question:
        constraints["mismatch_type"] = "payment_without_passage"
    elif "有通行但没收入" in question or "无收入" in question:
        constraints["mismatch_type"] = "passage_without_payment"

    if "长时间停留" in question:
        constraints["min_stay_minutes"] = 180
    if "收费偏低" in question or "收费过低" in question:
        constraints["needs_fee_definition"] = True
    if "单位车位收入" in question or "车位收入" in question:
        constraints["ranking_metric"] = "revenue_per_space"
    elif "单位车位车流" in question:
        constraints["ranking_metric"] = "entry_per_space"
    if "支付方式" in question:
        constraints["breakdown_dimension"] = "payment_method"
    return constraints


def _resolve_parking_chart(query_profile: str, metric_label: str) -> dict:
    if query_profile == "lot_capacity_efficiency_ranking":
        return {
            "type": "bar",
            "x_field": "parking_lot",
            "y_field": "单位车位收入",
            "series_field": None,
        }
    if query_profile == "payment_method_risk_breakdown":
        return {
            "type": "bar",
            "x_field": "payment_method",
            "y_field": "支付失败率",
            "series_field": "parking_lot",
        }
    if query_profile == "payment_passage_reconciliation_by_date":
        return {
            "type": "bar",
            "x_field": "stat_date",
            "y_field": "总收入",
            "series_field": "parking_lot",
        }
    if query_profile == "payment_passage_reconciliation_by_plate":
        return {
            "type": "bar",
            "x_field": "license_plate",
            "y_field": "停留时长",
            "series_field": "parking_lot",
        }
    return {
        "type": "line",
        "x_field": "stat_date",
        "y_field": metric_label,
        "series_field": "parking_lot",
    }


def _apply_parking_clarification_rules(question: str, task: dict) -> None:
    query_profile = task.get("query_profile")
    constraints = task.get("constraints", {})
    if query_profile == "payment_passage_reconciliation_by_plate" and constraints.get("needs_fee_definition"):
        task["needs_clarification"] = True
        task["clarifying_question"] = (
            "“收费偏低”的口径不唯一。请明确是要看“有通行但未收费”、"
            "“实收低于应收”，还是“按停留时长折算后收费偏低”。"
        )
        return
    if query_profile == "payment_method_risk_breakdown" and "异常开闸" in question:
        task["needs_clarification"] = True
        task["clarifying_question"] = (
            "当前原始表里没有异常开闸明细来源。"
            "你可以改为查看“支付方式与支付失败率的关系”，或补充异常开闸明细表后再联查。"
        )
        return

    task["needs_clarification"] = bool(task.get("needs_clarification"))


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
