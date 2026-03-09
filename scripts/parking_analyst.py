from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime


def diagnose_parking_operation(rows: list[dict], task: dict) -> dict:
    if task["intent"] == "parking_revenue_analysis":
        return _analyze_revenue(rows, task)
    if task["intent"] == "parking_anomaly_diagnosis":
        return _analyze_anomaly(rows, task)
    if task["intent"] == "parking_flow_efficiency_analysis":
        return _analyze_flow_efficiency(rows, task)
    if task["intent"] == "parking_management_report":
        return _build_management_report(rows, task)
    raise ValueError(f"不支持的停车经营分析意图: {task['intent']}")


def _analyze_revenue(rows: list[dict], task: dict) -> dict:
    current_rows = _filter_range(rows, task["time_field"], task["time_range"])
    baseline_rows, recent_rows = _split_rows_for_revenue_baseline(current_rows, task["entity_field"])
    current_by_lot = _group_by_lot(recent_rows)
    previous_by_lot = _group_by_lot(baseline_rows)

    lot_changes = []
    for lot, current_items in current_by_lot.items():
        previous_items = previous_by_lot.get(lot, [])
        current_total = _avg(current_items, "total_revenue")
        previous_total = _avg(previous_items, "total_revenue")
        delta = round(current_total - previous_total, 2)
        lot_changes.append((lot, delta, current_items, previous_items))

    primary_lot, revenue_delta, primary_current, primary_previous = min(lot_changes, key=lambda item: item[1])
    current_stats = _summarize_lot(primary_current)
    previous_stats = _summarize_lot(primary_previous)
    diagnosis = []

    if current_stats["payment_failure_rate"] - previous_stats["payment_failure_rate"] >= 0.015:
        diagnosis.append({
            "factor": "payment_failure_rate",
            "message": f"支付失败率由 {previous_stats['payment_failure_rate']:.1%} 上升到 {current_stats['payment_failure_rate']:.1%}",
            "impact": "高",
        })
    if current_stats["abnormal_open_count"] - previous_stats["abnormal_open_count"] > 30:
        diagnosis.append({
            "factor": "abnormal_open_count",
            "message": f"异常开闸 7 天累计增加 {current_stats['abnormal_open_count'] - previous_stats['abnormal_open_count']:.0f} 次",
            "impact": "高",
        })
    if previous_stats["entry_count"] and (current_stats["entry_count"] / previous_stats["entry_count"]) < 0.92:
        diagnosis.append({
            "factor": "entry_count",
            "message": f"入场车次下降至前一周期的 {current_stats['entry_count'] / previous_stats['entry_count']:.1%}",
            "impact": "中",
        })
    if previous_stats["occupancy_rate"] and current_stats["occupancy_rate"] < previous_stats["occupancy_rate"] - 0.05:
        diagnosis.append({
            "factor": "occupancy_rate",
            "message": f"平均利用率由 {previous_stats['occupancy_rate']:.1%} 下滑到 {current_stats['occupancy_rate']:.1%}",
            "impact": "中",
        })

    recommendations = _build_revenue_recommendations(diagnosis)
    chart_rows = [{"stat_date": row["stat_date"], "parking_lot": row["parking_lot"], "总收入": row["total_revenue"]} for row in primary_current]
    return {
        "analysis_type": "revenue",
        "primary_lot": primary_lot,
        "revenue_delta": revenue_delta,
        "diagnosis": diagnosis,
        "recommendations": recommendations,
        "executive_summary": [
            f"{primary_lot} 是最近一周期收入下滑最明显的车场，较窗口前半段日均减少 {abs(revenue_delta):.0f}。",
            f"主要拖累因素集中在 { '、'.join(item['factor'] for item in diagnosis) if diagnosis else '收入结构变化'}。",
        ],
        "chart_rows": chart_rows,
        "chart_spec": {
            "type": "line",
            "x_field": "stat_date",
            "y_field": "总收入",
            "series_field": "parking_lot",
        },
    }


def _analyze_anomaly(rows: list[dict], task: dict) -> dict:
    current_rows = _filter_range(rows, task["time_field"], task["time_range"])
    current_by_lot = _group_by_lot(current_rows)
    diagnosis = []
    highest_risk = "low"

    for lot, items in current_by_lot.items():
        stats = _summarize_lot(items)
        reasons = []
        score = 0
        if stats["payment_failure_rate"] >= 0.035:
            reasons.append(f"支付失败率达到 {stats['payment_failure_rate']:.1%}")
            score += 2
        if stats["abnormal_open_count"] >= 50:
            reasons.append(f"异常开闸 7 天累计 {stats['abnormal_open_count']:.0f} 次")
            score += 3
        if stats["free_release_count"] >= 120:
            reasons.append(f"免费放行累计 {stats['free_release_count']:.0f} 次")
            score += 1
        if stats["occupancy_rate"] <= 0.70:
            reasons.append(f"利用率仅 {stats['occupancy_rate']:.1%}")
            score += 1
        if reasons:
            risk_level = _score_to_risk(score)
            highest_risk = _max_risk(highest_risk, risk_level)
            diagnosis.append({
                "parking_lot": lot,
                "risk_level": risk_level,
                "reasons": reasons,
            })

    recommendations = _build_anomaly_recommendations(diagnosis)
    chart_rows = [
        {"stat_date": row["stat_date"], "parking_lot": row["parking_lot"], "支付失败率": row["payment_failure_rate"]}
        for row in current_rows
        if any(item["parking_lot"] == row["parking_lot"] for item in diagnosis)
    ]
    return {
        "analysis_type": "anomaly",
        "risk_level": highest_risk,
        "diagnosis": diagnosis,
        "recommendations": recommendations,
        "executive_summary": [
            f"最近一周期识别到 {len(diagnosis)} 个存在经营风险的车场。",
            "风险主要集中在支付失败、异常开闸和免费放行异常。",
        ],
        "chart_rows": chart_rows,
        "chart_spec": {
            "type": "line",
            "x_field": "stat_date",
            "y_field": "支付失败率",
            "series_field": "parking_lot",
        },
    }


def _analyze_flow_efficiency(rows: list[dict], task: dict) -> dict:
    current_rows = _filter_range(rows, task["time_field"], task["time_range"])
    baseline_rows, recent_rows = _split_rows_for_revenue_baseline(current_rows, task["entity_field"])
    current_by_lot = _group_by_lot(recent_rows)
    previous_by_lot = _group_by_lot(baseline_rows)

    lot_scores = []
    for lot, current_items in current_by_lot.items():
        previous_items = previous_by_lot.get(lot, [])
        current_stats = _summarize_lot(current_items)
        previous_stats = _summarize_lot(previous_items)
        current_entry_avg = _daily_average(current_items, "entry_count")
        previous_entry_avg = _daily_average(previous_items, "entry_count")
        current_stats["entry_count_daily_avg"] = current_entry_avg
        previous_stats["entry_count_daily_avg"] = previous_entry_avg
        entry_drop = current_entry_avg - previous_entry_avg
        occupancy_drop = current_stats["occupancy_rate"] - previous_stats["occupancy_rate"]
        score = entry_drop + occupancy_drop * 1000
        lot_scores.append((lot, score, current_stats, previous_stats, current_items))

    primary_lot, _, current_stats, previous_stats, primary_rows = min(lot_scores, key=lambda item: item[1])
    diagnosis = []
    if current_stats["entry_count_daily_avg"] < previous_stats["entry_count_daily_avg"]:
        diagnosis.append({
            "factor": "entry_count",
            "message": f"日均入场车次从 {previous_stats['entry_count_daily_avg']:.0f} 下降到 {current_stats['entry_count_daily_avg']:.0f}",
            "impact": "高",
        })
    if current_stats["occupancy_rate"] < previous_stats["occupancy_rate"]:
        diagnosis.append({
            "factor": "occupancy_rate",
            "message": f"平均利用率从 {previous_stats['occupancy_rate']:.1%} 下降到 {current_stats['occupancy_rate']:.1%}",
            "impact": "高" if previous_stats["occupancy_rate"] - current_stats["occupancy_rate"] >= 0.05 else "中",
        })
    if current_stats["free_release_count"] > previous_stats["free_release_count"]:
        diagnosis.append({
            "factor": "free_release_count",
            "message": f"免费放行从 {previous_stats['free_release_count']:.0f} 增加到 {current_stats['free_release_count']:.0f}",
            "impact": "中",
        })

    recommendations = _build_flow_recommendations(diagnosis)
    chart_rows = [{"stat_date": row["stat_date"], "parking_lot": row["parking_lot"], "利用率": row["occupancy_rate"]} for row in primary_rows]
    return {
        "analysis_type": "flow_efficiency",
        "primary_lot": primary_lot,
        "diagnosis": diagnosis,
        "recommendations": recommendations,
        "executive_summary": [
            f"{primary_lot} 在最近窗口内车流和利用率下滑最明显。",
            f"主要信号为 { '、'.join(item['factor'] for item in diagnosis) if diagnosis else '车流效率走弱'}。",
        ],
        "chart_rows": chart_rows,
        "chart_spec": {
            "type": "line",
            "x_field": "stat_date",
            "y_field": "利用率",
            "series_field": "parking_lot",
        },
    }


def _build_management_report(rows: list[dict], task: dict) -> dict:
    revenue_report = _analyze_revenue(rows, {
        **task,
        "intent": "parking_revenue_analysis",
        "metric": {"field": "total_revenue", "label": "总收入", "aggregation": "sum"},
    })
    anomaly_report = _analyze_anomaly(rows, {
        **task,
        "intent": "parking_anomaly_diagnosis",
        "focus_metrics": ["payment_failure_rate", "abnormal_open_count", "free_release_count", "occupancy_rate"],
    })
    flow_report = _analyze_flow_efficiency(rows, {
        **task,
        "intent": "parking_flow_efficiency_analysis",
        "focus_metrics": ["entry_count", "occupancy_rate"],
    })

    current_rows = _filter_range(rows, task["time_field"], task["time_range"])
    total_revenue = _sum(current_rows, "total_revenue")
    total_entry = _sum(current_rows, "entry_count")
    avg_occupancy = _avg(current_rows, "occupancy_rate")

    focus_lots = [
        {
            "parking_lot": revenue_report["primary_lot"],
            "topic": "收入下滑",
            "summary": revenue_report["executive_summary"][0],
        },
        {
            "parking_lot": flow_report["primary_lot"],
            "topic": "车流效率下滑",
            "summary": flow_report["executive_summary"][0],
        },
    ]
    if anomaly_report["diagnosis"]:
        focus_lots.extend([
            {
                "parking_lot": item["parking_lot"],
                "topic": "经营异常",
                "summary": "；".join(item["reasons"]),
            }
            for item in anomaly_report["diagnosis"]
        ])

    priority_actions = _dedupe_actions(
        revenue_report["recommendations"] + anomaly_report["recommendations"] + flow_report["recommendations"]
    )

    chart_rows = [{"stat_date": row["stat_date"], "parking_lot": row["parking_lot"], "总收入": row["total_revenue"]} for row in current_rows]
    return {
        "analysis_type": "management_report",
        "overview": {
            "reporting_window": task["time_range"]["preset"],
            "total_revenue": total_revenue,
            "total_entry_count": total_entry,
            "avg_occupancy_rate": round(avg_occupancy, 4),
            "high_risk_lot_count": len(anomaly_report["diagnosis"]),
        },
        "focus_lots": focus_lots,
        "priority_actions": priority_actions,
        "modules": {
            "revenue": revenue_report["executive_summary"],
            "anomaly": anomaly_report["executive_summary"],
            "flow_efficiency": flow_report["executive_summary"],
        },
        "executive_summary": [
            f"最近周期总收入 {total_revenue:.0f}，总入场车次 {total_entry:.0f}，平均利用率 {avg_occupancy:.1%}。",
            f"需重点关注 {revenue_report['primary_lot']} 的收入下滑，以及 {anomaly_report['risk_level']} 风险异常。",
        ],
        "chart_rows": chart_rows,
        "chart_spec": {
            "type": "line",
            "x_field": "stat_date",
            "y_field": "总收入",
            "series_field": "parking_lot",
        },
    }


def _group_by_lot(rows: list[dict]) -> dict[str, list[dict]]:
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["parking_lot"]].append(row)
    return grouped


def _split_rows_for_revenue_baseline(rows: list[dict], entity_field: str) -> tuple[list[dict], list[dict]]:
    grouped = defaultdict(list)
    for row in rows:
        grouped[row[entity_field]].append(row)

    baseline_rows = []
    recent_rows = []
    for lot_rows in grouped.values():
        ordered = sorted(lot_rows, key=lambda item: item["stat_date"])
        midpoint = max(1, len(ordered) // 2)
        baseline_rows.extend(ordered[:midpoint])
        recent_rows.extend(ordered[midpoint:])
    return baseline_rows, recent_rows


def _filter_range(rows: list[dict], field: str, time_range: dict) -> list[dict]:
    start_date = date.fromisoformat(time_range["start"])
    end_date = date.fromisoformat(time_range["end"])
    return [
        row
        for row in rows
        if start_date <= _to_date(row[field]) <= end_date
    ]


def _to_date(value) -> date:
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def _sum(rows: list[dict], field: str) -> float:
    return round(sum(float(row[field]) for row in rows), 2)


def _avg(rows: list[dict], field: str) -> float:
    if not rows:
        return 0.0
    return round(sum(float(row[field]) for row in rows) / len(rows), 4)


def _daily_average(rows: list[dict], field: str) -> float:
    if not rows:
        return 0.0
    return round(sum(float(row[field]) for row in rows) / len(rows), 2)


def _summarize_lot(rows: list[dict]) -> dict:
    return {
        "total_revenue": _sum(rows, "total_revenue"),
        "entry_count": _sum(rows, "entry_count"),
        "payment_failure_rate": _avg(rows, "payment_failure_rate"),
        "abnormal_open_count": _sum(rows, "abnormal_open_count"),
        "free_release_count": _sum(rows, "free_release_count"),
        "occupancy_rate": _avg(rows, "occupancy_rate"),
    }


def _build_revenue_recommendations(diagnosis: list[dict]) -> list[str]:
    recommendations = []
    factors = {item["factor"] for item in diagnosis}
    if "payment_failure_rate" in factors:
        recommendations.append("优先排查支付通道、二维码链路和出场缴费终端，降低支付失败造成的收入流失。")
    if "abnormal_open_count" in factors:
        recommendations.append("复核异常开闸权限和岗亭放行流程，重点检查高峰时段人工干预记录。")
    if "entry_count" in factors or "occupancy_rate" in factors:
        recommendations.append("结合活动、价格和导流策略复盘车流下滑原因，评估是否需要调整时段优惠或流量投放。")
    if not recommendations:
        recommendations.append("建议进一步拆解收入结构与车流来源，确认是否存在价格或流量层面的结构性变化。")
    return recommendations


def _build_anomaly_recommendations(diagnosis: list[dict]) -> list[str]:
    if not diagnosis:
        return ["当前周期未识别到明显经营异常。"]
    actions = []
    if any("支付失败率" in " ".join(item["reasons"]) for item in diagnosis):
        actions.append("立即检查支付通道稳定性、出场设备网络和第三方支付回调日志。")
    if any("异常开闸" in " ".join(item["reasons"]) for item in diagnosis):
        actions.append("核查异常开闸授权、岗亭人工干预和设备误触发情况。")
    if any("免费放行" in " ".join(item["reasons"]) for item in diagnosis):
        actions.append("抽查免费放行记录，确认是否存在权限滥用或规则配置异常。")
    if any("利用率" in " ".join(item["reasons"]) for item in diagnosis):
        actions.append("复盘低利用率时段与周边竞品车场，评估调价和导流策略。")
    return actions


def _build_flow_recommendations(diagnosis: list[dict]) -> list[str]:
    actions = []
    factors = {item["factor"] for item in diagnosis}
    if "entry_count" in factors:
        actions.append("建议复盘近一周导流来源、活动投放和周边竞品变化，确认车流下滑原因。")
    if "occupancy_rate" in factors:
        actions.append("建议按时段检查车位利用率，必要时优化价格策略和包时产品，提高高峰外利用率。")
    if "free_release_count" in factors:
        actions.append("建议核查免费放行规则和现场人工操作，避免无效放行稀释车流效率。")
    if not actions:
        actions.append("建议继续观察车流和利用率变化，并补充周转率与停车时长分析。")
    return actions


def _dedupe_actions(actions: list[str]) -> list[str]:
    deduped = []
    for action in actions:
        if action not in deduped:
            deduped.append(action)
    return deduped[:5]


def _score_to_risk(score: int) -> str:
    if score >= 4:
        return "high"
    if score >= 2:
        return "medium"
    return "low"


def _max_risk(current: str, candidate: str) -> str:
    ranking = {"low": 1, "medium": 2, "high": 3}
    return candidate if ranking[candidate] > ranking[current] else current
