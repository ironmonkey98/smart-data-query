from __future__ import annotations

import re
from datetime import date, timedelta


METRIC_ALIASES = {
    "成交额": ("paid_amount", "成交额"),
    "销售额": ("paid_amount", "成交额"),
    "收入": ("total_revenue", "总收入"),
    "总收入": ("total_revenue", "总收入"),
    "临停收入": ("temp_revenue", "临停收入"),
    "包月收入": ("monthly_revenue", "包月收入"),
}


def normalize_question(question: str, schema_text: str, glossary_text: str) -> dict:
    if "车场" in question or "停车" in question:
        return _normalize_parking_question(question, schema_text, glossary_text)

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
    task = {
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
    return task


def _detect_metric(question: str, glossary_text: str) -> tuple[str, str]:
    for alias, mapping in METRIC_ALIASES.items():
        if alias in question:
            return mapping
    glossary_match = re.search(r"成交额\s*=\s*(\w+)", glossary_text)
    if glossary_match:
        return glossary_match.group(1), "成交额"
    return "paid_amount", "成交额"


def _detect_intent(question: str) -> str:
    if "车场" in question and ("周报" in question or "日报" in question or "管理层" in question or "经营报告" in question):
        return "parking_management_report"
    if "停车" in question and ("周报" in question or "日报" in question or "管理层" in question or "经营报告" in question):
        return "parking_management_report"
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
    # V1 只支持折线图，所有类型统一返回 line，避免下游 NotImplementedError
    return "line"


def _detect_time_range(question: str) -> dict:
    today = date.today()
    if "最近7天" in question or "近7天" in question:
        return {
            "preset": "last_7_days",
            "start": (today - timedelta(days=6)).isoformat(),
            "end": today.isoformat(),
        }
    if "最近30天" in question or "近30天" in question:
        return {
            "preset": "last_30_days",
            "start": (today - timedelta(days=29)).isoformat(),
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


def _normalize_parking_question(question: str, schema_text: str, glossary_text: str) -> dict:
    intent = _detect_intent(question)
    time_range = _detect_time_range(question)
    metric_field, metric_label = _detect_metric(question, glossary_text)
    if intent == "parking_flow_efficiency_analysis":
        metric_field, metric_label = "entry_count", "入场车次"
    elif intent == "parking_anomaly_diagnosis":
        metric_field, metric_label = "payment_failure_rate", "支付失败率"
    elif intent == "parking_management_report":
        metric_field, metric_label = "total_revenue", "总收入"
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
        focus_metrics = ["payment_failure_rate", "abnormal_open_count", "free_release_count", "occupancy_rate"]
    if not focus_metrics and intent == "parking_flow_efficiency_analysis":
        focus_metrics = ["entry_count", "occupancy_rate"]
    if not focus_metrics and intent == "parking_management_report":
        focus_metrics = ["total_revenue", "entry_count", "occupancy_rate", "payment_failure_rate", "abnormal_open_count"]

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
    return task


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
    if "周报" in question or "日报" in question or "管理层" in question:
        assumptions.append("综合报告需汇总收入、异常、车流效率并提炼管理层动作")
    if focus_metrics:
        assumptions.append(f"重点诊断指标：{', '.join(focus_metrics)}")
    return assumptions
