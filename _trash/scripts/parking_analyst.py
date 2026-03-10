from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta


def diagnose_parking_operation(rows: list[dict], task: dict) -> dict:
    profile = _resolve_analysis_profile(task)
    if profile == "revenue":
        return _analyze_revenue(rows, task)
    if profile == "period_assessment":
        return _build_period_assessment(rows, task)
    if profile == "anomaly":
        return _analyze_anomaly(rows, task)
    if profile == "flow_efficiency":
        return _analyze_flow_efficiency(rows, task)
    if profile == "management_daily":
        return _build_management_daily_report(rows, task)
    if profile == "management_weekly":
        return _build_management_report(rows, task)
    raise ValueError(f"不支持的停车经营分析意图: {task['intent']}")


def _resolve_analysis_profile(task: dict) -> str:
    semantic_plan = task.get("semantic_plan") or {}
    business_goal = semantic_plan.get("business_goal")
    analysis_job = semantic_plan.get("analysis_job")
    deliverable = semantic_plan.get("deliverable")

    if business_goal == "management_reporting" and analysis_job == "operational_overview":
        if deliverable == "daily_brief":
            return "management_daily"
        return "management_weekly"
    if business_goal == "management_reporting" and analysis_job == "period_assessment":
        return "period_assessment"
    if business_goal == "risk_detection" and analysis_job == "anomaly_focus":
        return "anomaly"
    if business_goal == "efficiency_diagnosis" and analysis_job == "flow_or_occupancy":
        return "flow_efficiency"
    if business_goal == "revenue_diagnosis" and analysis_job == "revenue_focus":
        return "revenue"

    intent = task["intent"]
    if intent == "parking_revenue_analysis":
        return "revenue"
    if intent == "parking_period_assessment":
        return "period_assessment"
    if intent == "parking_anomaly_diagnosis":
        return "anomaly"
    if intent == "parking_flow_efficiency_analysis":
        return "flow_efficiency"
    if intent == "parking_management_daily_report":
        return "management_daily"
    if intent == "parking_management_report":
        return "management_weekly"
    raise ValueError(f"不支持的停车经营分析意图: {intent}")


def _analyze_revenue(rows: list[dict], task: dict) -> dict:
    current_rows = _filter_focus_entities(_filter_range(rows, task["time_field"], task["time_range"]), task)
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
            "chart_family": "trend",
            "style_preset": "ops_dense",
            "tone": "operations",
            "x_field": "stat_date",
            "y_field": "总收入",
            "series_field": "parking_lot",
        },
    }


def _analyze_anomaly(rows: list[dict], task: dict) -> dict:
    current_rows = _filter_focus_entities(_filter_range(rows, task["time_field"], task["time_range"]), task)
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
        {
            "parking_lot": item["parking_lot"],
            "风险分": _risk_level_to_score(item["risk_level"]),
        }
        for item in diagnosis
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
            "type": "bar",
            "chart_family": "risk_overview",
            "style_preset": "executive_risk",
            "tone": "executive",
            "x_field": "parking_lot",
            "y_field": "风险分",
            "series_field": None,
            "emphasis": [item["parking_lot"] for item in diagnosis if item["risk_level"] == highest_risk],
        },
    }


def _analyze_flow_efficiency(rows: list[dict], task: dict) -> dict:
    current_rows = _filter_focus_entities(_filter_range(rows, task["time_field"], task["time_range"]), task)
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
            "chart_family": "trend",
            "style_preset": "ops_dense",
            "tone": "operations",
            "x_field": "stat_date",
            "y_field": "利用率",
            "series_field": "parking_lot",
        },
    }


def _build_management_report(rows: list[dict], task: dict) -> dict:
    revenue_report = _analyze_revenue(rows, _build_child_task(
        task,
        intent="parking_revenue_analysis",
        metric={"field": "total_revenue", "label": "总收入", "aggregation": "sum"},
        focus_metrics=["total_revenue"],
    ))
    anomaly_report = _analyze_anomaly(rows, _build_child_task(
        task,
        intent="parking_anomaly_diagnosis",
        focus_metrics=["payment_failure_rate", "abnormal_open_count", "free_release_count", "occupancy_rate"],
    ))
    flow_report = _analyze_flow_efficiency(rows, _build_child_task(
        task,
        intent="parking_flow_efficiency_analysis",
        focus_metrics=["entry_count", "occupancy_rate"],
    ))

    current_rows = _filter_focus_entities(_filter_range(rows, task["time_field"], task["time_range"]), task)
    previous_rows = _filter_focus_entities(_filter_comparison_range(rows, task["time_field"], task.get("comparison_range")), task)
    total_revenue = _sum(current_rows, "total_revenue")
    total_entry = _sum(current_rows, "entry_count")
    avg_occupancy = _avg(current_rows, "occupancy_rate")
    comparison_summary = _build_management_comparison_summary(
        current_rows=current_rows,
        previous_rows=previous_rows,
        current_total_revenue=total_revenue,
        current_total_entry=total_entry,
        current_avg_occupancy=avg_occupancy,
        current_risk_count=len(anomaly_report["diagnosis"]),
    )

    revenue_focus = {
        "parking_lot": revenue_report["primary_lot"],
        "topic": "收入下滑",
        "summary": revenue_report["executive_summary"][0],
    }
    flow_focus = {
        "parking_lot": flow_report["primary_lot"],
        "topic": "车流效率下滑",
        "summary": flow_report["executive_summary"][0],
    }
    anomaly_focus_lots = [
        {
            "parking_lot": item["parking_lot"],
            "topic": "经营异常",
            "summary": "；".join(item["reasons"]),
        }
        for item in anomaly_report["diagnosis"]
    ]

    focus_lots = [
        revenue_focus,
        flow_focus,
        *anomaly_focus_lots,
    ]
    focus_order = _resolve_management_focus_order(task)
    focus_lots = _sort_focus_lots(focus_lots, focus_order)

    priority_actions = _dedupe_actions(
        revenue_report["recommendations"] + anomaly_report["recommendations"] + flow_report["recommendations"]
    )

    summary_templates = {
        "revenue": revenue_report["executive_summary"][0],
        "anomaly": f"需优先处理 {anomaly_report['risk_level']} 风险异常，重点关注 {len(anomaly_report['diagnosis'])} 个异常车场。",
        "flow_efficiency": flow_report["executive_summary"][0],
    }
    first_summary_key = focus_order[0]
    second_summary_key = "anomaly" if first_summary_key != "anomaly" else "revenue"

    chart_rows = [{"stat_date": row["stat_date"], "parking_lot": row["parking_lot"], "总收入": row["total_revenue"]} for row in current_rows]
    return {
        "analysis_type": "management_report",
        "report_type": "weekly",
        "overview": {
            "reporting_window": task["time_range"]["preset"],
            "total_revenue": total_revenue,
            "total_entry_count": total_entry,
            "avg_occupancy_rate": round(avg_occupancy, 4),
            "high_risk_lot_count": len(anomaly_report["diagnosis"]),
        },
        "focus_lots": focus_lots,
        "priority_actions": priority_actions,
        "modules": _build_management_modules(focus_order, revenue_report, anomaly_report, flow_report),
        "executive_summary": [
            comparison_summary or f"最近周期总收入 {total_revenue:.0f}，总入场车次 {total_entry:.0f}，平均利用率 {avg_occupancy:.1%}。",
            summary_templates[second_summary_key] if first_summary_key == "revenue" else summary_templates[first_summary_key],
        ],
        "chart_rows": chart_rows,
        "chart_spec": {
            "type": "line",
            "chart_family": "trend",
            "style_preset": _resolve_management_chart_style(task),
            "tone": "executive",
            "x_field": "stat_date",
            "y_field": "总收入",
            "series_field": "parking_lot",
        },
    }


def _build_period_assessment(rows: list[dict], task: dict) -> dict:
    current_rows = _filter_focus_entities(_filter_range(rows, task["time_field"], task["time_range"]), task)
    previous_rows = _filter_focus_entities(_filter_comparison_range(rows, task["time_field"], task.get("comparison_range")), task)
    current_total_revenue = _sum(current_rows, "total_revenue")
    current_total_entry = _sum(current_rows, "entry_count")
    current_avg_occupancy = _avg(current_rows, "occupancy_rate")
    current_risk_count = _count_high_risk_lots(current_rows)

    previous_total_revenue = _sum(previous_rows, "total_revenue")
    previous_total_entry = _sum(previous_rows, "entry_count")
    previous_avg_occupancy = _avg(previous_rows, "occupancy_rate")
    previous_risk_count = _count_high_risk_lots(previous_rows)

    trend, reason_factors = _assess_period_trend(
        current_total_revenue=current_total_revenue,
        previous_total_revenue=previous_total_revenue,
        current_total_entry=current_total_entry,
        previous_total_entry=previous_total_entry,
        current_avg_occupancy=current_avg_occupancy,
        previous_avg_occupancy=previous_avg_occupancy,
        current_risk_count=current_risk_count,
        previous_risk_count=previous_risk_count,
    )
    comparison_summary = _build_period_assessment_summary(trend, reason_factors)
    supporting_metrics = {
        "current_total_revenue": current_total_revenue,
        "previous_total_revenue": previous_total_revenue,
        "current_total_entry_count": current_total_entry,
        "previous_total_entry_count": previous_total_entry,
        "current_avg_occupancy_rate": current_avg_occupancy,
        "previous_avg_occupancy_rate": previous_avg_occupancy,
        "current_risk_lot_count": current_risk_count,
        "previous_risk_lot_count": previous_risk_count,
    }

    return {
        "analysis_type": "period_assessment",
        "trend": trend,
        "comparison_summary": comparison_summary,
        "reason_factors": reason_factors,
        "supporting_metrics": supporting_metrics,
        "executive_summary": [
            comparison_summary,
            "主要原因：" + "；".join(reason_factors[:2]) if reason_factors else "主要原因：当前暂无明显拖累因素。",
        ],
        "chart_rows": [
            {"stat_date": row["stat_date"], "parking_lot": row["parking_lot"], "总收入": row["total_revenue"]}
            for row in current_rows
        ],
        "chart_spec": {
            "type": "line",
            "chart_family": "trend",
            "style_preset": "executive_clean",
            "tone": "operations",
            "x_field": "stat_date",
            "y_field": "总收入",
            "series_field": "parking_lot",
        },
    }


def _build_management_comparison_summary(
    current_rows: list[dict],
    previous_rows: list[dict],
    current_total_revenue: float,
    current_total_entry: float,
    current_avg_occupancy: float,
    current_risk_count: int,
) -> str | None:
    if not previous_rows:
        return None

    previous_total_revenue = _sum(previous_rows, "total_revenue")
    previous_total_entry = _sum(previous_rows, "entry_count")
    previous_avg_occupancy = _avg(previous_rows, "occupancy_rate")
    previous_risk_count = _count_high_risk_lots(previous_rows)

    improvements = 0
    deteriorations = 0
    reasons = []

    if current_total_revenue >= previous_total_revenue:
        improvements += 1
    else:
        deteriorations += 1
        reasons.append(f"总收入较上一周期减少 {abs(current_total_revenue - previous_total_revenue):.0f}")

    if current_total_entry >= previous_total_entry:
        improvements += 1
    else:
        deteriorations += 1
        reasons.append(f"入场车次减少 {abs(current_total_entry - previous_total_entry):.0f}")

    if current_avg_occupancy >= previous_avg_occupancy:
        improvements += 1
    else:
        deteriorations += 1
        reasons.append(
            f"平均利用率由 {previous_avg_occupancy:.1%} 下降到 {current_avg_occupancy:.1%}"
        )

    if current_risk_count > previous_risk_count:
        deteriorations += 1
        reasons.append(f"异常车场数由 {previous_risk_count} 个增加到 {current_risk_count} 个")
    elif current_risk_count < previous_risk_count:
        improvements += 1

    trend = "好转" if improvements >= deteriorations else "变坏"
    if reasons:
        reason_text = "主要受 " + "、".join(reasons[:2]) + " 影响。"
    else:
        reason_text = "核心指标整体改善。"
    return f"与上一周期相比，停车经营整体{trend}。{reason_text}"


def _count_high_risk_lots(rows: list[dict]) -> int:
    current_by_lot = _group_by_lot(rows)
    count = 0
    for items in current_by_lot.values():
        stats = _summarize_lot(items)
        score = 0
        if stats["payment_failure_rate"] >= 0.035:
            score += 2
        if stats["abnormal_open_count"] >= 50:
            score += 3
        if stats["free_release_count"] >= 120:
            score += 1
        if stats["occupancy_rate"] <= 0.70:
            score += 1
        if score > 0:
            count += 1
    return count


def _assess_period_trend(
    current_total_revenue: float,
    previous_total_revenue: float,
    current_total_entry: float,
    previous_total_entry: float,
    current_avg_occupancy: float,
    previous_avg_occupancy: float,
    current_risk_count: int,
    previous_risk_count: int,
) -> tuple[str, list[str]]:
    improvements = 0
    deteriorations = 0
    reasons = []

    if current_total_revenue >= previous_total_revenue:
        improvements += 1
        if current_total_revenue > previous_total_revenue:
            reasons.append(f"总收入较上一周期增加 {abs(current_total_revenue - previous_total_revenue):.0f}")
    else:
        deteriorations += 1
        reasons.append(f"总收入较上一周期减少 {abs(current_total_revenue - previous_total_revenue):.0f}")

    if current_total_entry >= previous_total_entry:
        improvements += 1
        if current_total_entry > previous_total_entry:
            reasons.append(f"入场车次增加 {abs(current_total_entry - previous_total_entry):.0f}")
    else:
        deteriorations += 1
        reasons.append(f"入场车次减少 {abs(current_total_entry - previous_total_entry):.0f}")

    if current_avg_occupancy >= previous_avg_occupancy:
        improvements += 1
        if current_avg_occupancy > previous_avg_occupancy:
            reasons.append(f"平均利用率由 {previous_avg_occupancy:.1%} 提升到 {current_avg_occupancy:.1%}")
    else:
        deteriorations += 1
        reasons.append(f"平均利用率由 {previous_avg_occupancy:.1%} 下降到 {current_avg_occupancy:.1%}")

    if current_risk_count > previous_risk_count:
        deteriorations += 1
        reasons.append(f"异常车场数由 {previous_risk_count} 个增加到 {current_risk_count} 个")
    elif current_risk_count < previous_risk_count:
        improvements += 1
        reasons.append(f"异常车场数由 {previous_risk_count} 个减少到 {current_risk_count} 个")

    if improvements and deteriorations:
        trend = "mixed"
    elif improvements >= deteriorations:
        trend = "improved"
    else:
        trend = "worsened"
    return trend, reasons


def _build_period_assessment_summary(trend: str, reason_factors: list[str]) -> str:
    trend_label = {
        "improved": "好转",
        "worsened": "变坏",
        "mixed": "出现分化",
    }.get(trend, "出现波动")
    if reason_factors:
        return f"与上一周期相比，停车经营整体{trend_label}。主要受 {'、'.join(reason_factors[:2])} 影响。"
    return f"与上一周期相比，停车经营整体{trend_label}。核心指标变化相对平稳。"


def _build_management_daily_report(rows: list[dict], task: dict) -> dict:
    current_rows = _filter_focus_entities(_filter_range(rows, task["time_field"], task["time_range"]), task)
    if not current_rows:
        current_rows = _filter_focus_entities(_latest_day_rows(rows, task["time_field"]), task)
    current_date = max(_to_date(row[task["time_field"]]) for row in current_rows)
    previous_date = current_date - timedelta(days=1)
    previous_rows = _filter_focus_entities([
        row
        for row in rows
        if _to_date(row[task["time_field"]]) == previous_date
    ], task)

    current_by_lot = _group_by_lot(current_rows)
    previous_by_lot = _group_by_lot(previous_rows)

    focus_lots = []
    anomaly_diagnosis = []
    for lot, items in current_by_lot.items():
        current_stats = _summarize_lot(items)
        previous_stats = _summarize_lot(previous_by_lot.get(lot, []))
        revenue_delta = round(current_stats["total_revenue"] - previous_stats["total_revenue"], 2)
        if revenue_delta < 0:
            focus_lots.append({
                "parking_lot": lot,
                "topic": "今日收入承压",
                "summary": f"较前一日下降 {abs(revenue_delta):.0f}，今日收入 {current_stats['total_revenue']:.0f}。",
            })
        if current_stats["payment_failure_rate"] >= 0.05 or current_stats["abnormal_open_count"] >= 20:
            anomaly_diagnosis.append({
                "parking_lot": lot,
                "risk_level": "high" if current_stats["payment_failure_rate"] >= 0.08 else "medium",
                "reasons": _build_daily_anomaly_reasons(current_stats),
            })

    if anomaly_diagnosis:
        focus_lots.extend(
            {
                "parking_lot": item["parking_lot"],
                "topic": "今日异常",
                "summary": "；".join(item["reasons"]),
            }
            for item in anomaly_diagnosis
        )

    if not focus_lots:
        best_lot, best_stats = max(
            ((lot, _summarize_lot(items)) for lot, items in current_by_lot.items()),
            key=lambda item: item[1]["total_revenue"],
        )
        focus_lots.append({
            "parking_lot": best_lot,
            "topic": "今日表现最佳",
            "summary": f"今日收入 {best_stats['total_revenue']:.0f}，利用率 {best_stats['occupancy_rate']:.1%}。",
        })

    overview = {
        "reporting_window": "today",
        "report_date": current_date.isoformat(),
        "total_revenue": _sum(current_rows, "total_revenue"),
        "total_entry_count": _sum(current_rows, "entry_count"),
        "avg_occupancy_rate": round(_avg(current_rows, "occupancy_rate"), 4),
        "high_risk_lot_count": len(anomaly_diagnosis),
    }
    priority_actions = _dedupe_actions(
        _build_anomaly_recommendations(anomaly_diagnosis)
        + _build_daily_actions(current_by_lot, previous_by_lot)
    )
    chart_rows = _latest_n_day_rows(rows, task["time_field"], 7)
    primary_focus = focus_lots[0]

    return {
        "analysis_type": "management_report",
        "report_type": "daily",
        "overview": overview,
        "focus_lots": focus_lots[:4],
        "priority_actions": priority_actions,
        "modules": {
            "daily_overview": [
                f"今日总收入 {overview['total_revenue']:.0f}，总入场车次 {overview['total_entry_count']:.0f}。"
            ],
            "daily_risks": [
                f"今日识别到 {len(anomaly_diagnosis)} 个高关注车场。"
                if anomaly_diagnosis else "今日未识别到高风险车场。"
            ],
            "daily_focus": [primary_focus["summary"]],
        },
        "executive_summary": [
            f"截至 {current_date.isoformat()}，今日总收入 {overview['total_revenue']:.0f}，平均利用率 {overview['avg_occupancy_rate']:.1%}。",
            f"当前最需关注 {primary_focus['parking_lot']}，主题为“{primary_focus['topic']}”。",
        ],
        "chart_rows": [
            {"stat_date": row["stat_date"], "parking_lot": row["parking_lot"], "总收入": row["total_revenue"]}
            for row in chart_rows
        ],
        "chart_spec": {
            "type": "line",
            "chart_family": "trend",
            "style_preset": _resolve_management_chart_style(task),
            "tone": "executive",
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
    filtered = [
        row
        for row in rows
        if start_date <= _to_date(row[field]) <= end_date
    ]
    if filtered:
        return filtered
    if time_range.get("preset") == "today":
        return _latest_day_rows(rows, field)
    latest_date = max(_to_date(row[field]) for row in rows)
    window_days = max((end_date - start_date).days, 0)
    fallback_start = latest_date - timedelta(days=window_days)
    return [
        row
        for row in rows
        if fallback_start <= _to_date(row[field]) <= latest_date
    ]


def _filter_comparison_range(rows: list[dict], field: str, time_range: dict | None) -> list[dict]:
    if not time_range or not time_range.get("start") or not time_range.get("end"):
        return []
    return _filter_range(rows, field, time_range)


def _filter_focus_entities(rows: list[dict], task: dict) -> list[dict]:
    focus_entities = task.get("focus_entities") or []
    if not focus_entities:
        semantic_plan = task.get("semantic_plan") or {}
        focus_entities = semantic_plan.get("focus_entities") or []
    if not focus_entities:
        return rows
    allowed = {str(item).strip() for item in focus_entities if str(item).strip()}
    if not allowed:
        return rows
    return [row for row in rows if str(row.get("parking_lot", "")).strip() in allowed]


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


def _latest_day_rows(rows: list[dict], field: str) -> list[dict]:
    if not rows:
        return []
    latest_date = max(_to_date(row[field]) for row in rows)
    return [row for row in rows if _to_date(row[field]) == latest_date]


def _latest_n_day_rows(rows: list[dict], field: str, days: int) -> list[dict]:
    if not rows:
        return []
    ordered_dates = sorted({_to_date(row[field]) for row in rows})
    selected_dates = set(ordered_dates[-days:])
    return [row for row in rows if _to_date(row[field]) in selected_dates]


def _build_daily_anomaly_reasons(stats: dict) -> list[str]:
    reasons = []
    if stats["payment_failure_rate"] >= 0.05:
        reasons.append(f"支付失败率达到 {stats['payment_failure_rate']:.1%}")
    if stats["abnormal_open_count"] >= 20:
        reasons.append(f"异常开闸今日累计 {stats['abnormal_open_count']:.0f} 次")
    if stats["occupancy_rate"] <= 0.65:
        reasons.append(f"利用率仅 {stats['occupancy_rate']:.1%}")
    return reasons or ["今日经营指标波动较大"]


def _build_daily_actions(current_by_lot: dict[str, list[dict]], previous_by_lot: dict[str, list[dict]]) -> list[str]:
    actions = []
    for lot, items in current_by_lot.items():
        current_stats = _summarize_lot(items)
        previous_stats = _summarize_lot(previous_by_lot.get(lot, []))
        if previous_stats["total_revenue"] and current_stats["total_revenue"] < previous_stats["total_revenue"] * 0.9:
            actions.append(f"复盘 {lot} 今日收入下滑原因，优先检查支付、导流和现场放行。")
        if current_stats["payment_failure_rate"] >= 0.05:
            actions.append(f"排查 {lot} 今日支付链路与出场设备状态。")
    if not actions:
        actions.append("继续监控今日高峰期经营指标，关注支付失败和利用率波动。")
    return actions


def _build_child_task(task: dict, intent: str, focus_metrics: list[str], metric: dict | None = None) -> dict:
    semantic_plan = dict(task.get("semantic_plan") or {})
    if intent == "parking_revenue_analysis":
        semantic_plan.update({"business_goal": "revenue_diagnosis", "analysis_job": "revenue_focus", "deliverable": None})
    elif intent == "parking_anomaly_diagnosis":
        semantic_plan.update({"business_goal": "risk_detection", "analysis_job": "anomaly_focus", "deliverable": None})
    elif intent == "parking_flow_efficiency_analysis":
        semantic_plan.update({"business_goal": "efficiency_diagnosis", "analysis_job": "flow_or_occupancy", "deliverable": None})
    semantic_plan["focus_metrics"] = focus_metrics

    child_task = {
        **task,
        "intent": intent,
        "focus_metrics": focus_metrics,
        "semantic_plan": semantic_plan,
    }
    if metric is not None:
        child_task["metric"] = metric
    return child_task


def _resolve_management_focus_order(task: dict) -> list[str]:
    focus_metrics = list(task.get("focus_metrics", []))
    semantic_plan = task.get("semantic_plan") or {}
    focus_metrics.extend(metric for metric in semantic_plan.get("focus_metrics", []) if metric not in focus_metrics)

    anomaly_metrics = {"payment_failure_rate", "abnormal_open_count", "free_release_count"}
    flow_metrics = {"entry_count", "occupancy_rate"}
    revenue_metrics = {"total_revenue"}

    if any(metric in anomaly_metrics for metric in focus_metrics):
        return ["anomaly", "revenue", "flow_efficiency"]
    if any(metric in flow_metrics for metric in focus_metrics):
        return ["flow_efficiency", "revenue", "anomaly"]
    if any(metric in revenue_metrics for metric in focus_metrics):
        return ["revenue", "anomaly", "flow_efficiency"]
    return ["revenue", "anomaly", "flow_efficiency"]


def _sort_focus_lots(focus_lots: list[dict], focus_order: list[str]) -> list[dict]:
    topic_to_category = {
        "经营异常": "anomaly",
        "收入下滑": "revenue",
        "车流效率下滑": "flow_efficiency",
    }
    ranking = {category: index for index, category in enumerate(focus_order)}
    return sorted(focus_lots, key=lambda item: ranking.get(topic_to_category.get(item["topic"], "revenue"), 99))


def _build_management_modules(
    focus_order: list[str],
    revenue_report: dict,
    anomaly_report: dict,
    flow_report: dict,
) -> dict:
    module_map = {
        "revenue": revenue_report["executive_summary"],
        "anomaly": anomaly_report["executive_summary"],
        "flow_efficiency": flow_report["executive_summary"],
    }
    return {key: module_map[key] for key in focus_order}


def _resolve_management_chart_style(task: dict) -> str:
    semantic_plan = task.get("semantic_plan") or {}
    focus_metrics = semantic_plan.get("focus_metrics", []) or task.get("focus_metrics", [])
    anomaly_metrics = {"payment_failure_rate", "abnormal_open_count", "free_release_count"}
    if any(metric in anomaly_metrics for metric in focus_metrics):
        return "executive_risk"
    return "executive_clean"


def _risk_level_to_score(risk_level: str) -> int:
    mapping = {"high": 92, "medium": 68, "low": 38}
    return mapping.get(risk_level, 20)
