from __future__ import annotations

import argparse
import json
import sqlite3
from contextlib import closing
from datetime import date as PathDate
from datetime import timedelta as TimeDelta
from pathlib import Path

from chart_render import render_svg_chart
from connect_db import execute_structured_task, load_dataset
from llm_enhancer import enhance_analysis, handle_follow_up
from parking_analyst import diagnose_parking_operation
from sql_generator import normalize_question


def run_query(
    question: str,
    source_type: str,
    source: str,
    schema: str,
    glossary: str,
    output_dir: str,
    enable_llm: bool = False,
    session_file: str | None = None,
    llm_base_url: str | None = None,
    llm_model: str | None = None,
    task: dict | None = None,
) -> dict:
    schema_text = Path(schema).read_text(encoding="utf-8")
    glossary_text = Path(glossary).read_text(encoding="utf-8")
    resolved_task = task or normalize_question(
        question=question,
        schema_text=schema_text,
        glossary_text=glossary_text,
        llm_base_url=llm_base_url,
        llm_model=llm_model,
    )

    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    chart_path = target_dir / "chart.svg"
    summary_path = target_dir / "summary.json"
    if resolved_task.get("needs_clarification"):
        payload = {
            "task": resolved_task,
            "needs_clarification": True,
            "clarifying_question": resolved_task["clarifying_question"],
            "default_time_note": _build_default_time_note(resolved_task),
        }
        summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        if session_file:
            _write_session(session_file, payload)
        return payload

    if resolved_task["intent"].startswith("parking_") and source_type.lower() == "sqlite":
        resolved_task = _align_parking_sqlite_time_range(resolved_task, source)

    rows = load_dataset(source=source, source_type=source_type, task=resolved_task)
    if resolved_task["intent"].startswith("parking_") and resolved_task.get("query_profile") == "parking_daily_overview_join":
        resolved_task = _align_parking_time_range(resolved_task, rows)

    if resolved_task["intent"].startswith("parking_") and resolved_task.get("query_profile") == "parking_daily_overview_join":
        analysis = diagnose_parking_operation(rows, resolved_task)
        narrative = enhance_analysis(
            task=resolved_task,
            analysis=analysis,
            enable_llm=enable_llm,
            base_url=llm_base_url,
            model=llm_model,
        )
        try:
            render_svg_chart(
                rows=analysis["chart_rows"],
                chart_spec=analysis["chart_spec"],
                output_path=str(chart_path),
                title="停车经营分析结果",
            )
        except NotImplementedError:
            pass  # 图表类型暂不支持，跳过渲染，数据结果照常返回
        payload = {
            "task": resolved_task,
            "analysis": {
                key: value
                for key, value in analysis.items()
                if key not in {"chart_rows", "chart_spec"}
            },
            "narrative": narrative,
            "executive_summary": analysis["executive_summary"],
            "artifacts": {
                "summary": str(summary_path),
                "chart": str(chart_path),
            },
            # 供 Claude 做追问分析用的原始行样本（最多60行）
            "rows_sample": analysis.get("chart_rows", [])[:60],
            "default_time_note": _build_default_time_note(resolved_task),
        }
    elif resolved_task["intent"].startswith("parking_"):
        result = _build_parking_relational_result(resolved_task, rows)
        try:
            render_svg_chart(
                rows=result["chart_rows"],
                chart_spec=result["chart_spec"],
                output_path=str(chart_path),
                title="停车多表联查结果",
            )
        except NotImplementedError:
            pass
        payload = {
            "task": resolved_task,
            "result": {
                "row_count": result["row_count"],
                "rows": result["rows"],
                "metric": resolved_task.get("metric"),
                "dimensions": resolved_task.get("entities", []),
            },
            "artifacts": {
                "summary": str(summary_path),
                "chart": str(chart_path),
            },
            "summary": result["summary"],
            "rows_sample": result["rows"][:60],
            "default_time_note": _build_default_time_note(resolved_task),
        }
    else:
        result = execute_structured_task(rows=rows, task=resolved_task)
        try:
            render_svg_chart(
                rows=result["rows"],
                chart_spec=resolved_task["chart"],
                output_path=str(chart_path),
                title="智能问数结果",
            )
        except NotImplementedError:
            pass  # 图表类型暂不支持，跳过渲染，数据结果照常返回
        payload = {
            "task": resolved_task,
            "result": result,
            "artifacts": {
                "summary": str(summary_path),
                "chart": str(chart_path),
            },
            "summary": build_summary(result),
            # 供 Claude 做追问分析用的原始行样本（最多60行）
            "rows_sample": result.get("rows", [])[:60],
            "default_time_note": _build_default_time_note(resolved_task),
        }
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if session_file:
        _write_session(session_file, payload)
    return payload


def run_follow_up(
    follow_up_question: str,
    session_file: str,
    output_dir: str,
    enable_llm: bool = False,
    llm_base_url: str | None = None,
    llm_model: str | None = None,
) -> dict:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    summary_path = target_dir / "summary.json"
    session_payload = _read_session(session_file)
    answer = handle_follow_up(
        follow_up_question=follow_up_question,
        session_payload=session_payload,
        enable_llm=enable_llm,
        base_url=llm_base_url,
        model=llm_model,
    )
    payload = {
        "follow_up_question": follow_up_question,
        "follow_up_answer": answer,
    }
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_session(session_file, {**session_payload, "last_follow_up": payload})
    return payload


def build_summary(result: dict) -> list[str]:
    if not result["rows"]:
        return ["未查到符合条件的数据。"]
    totals = {}
    for row in result["rows"]:
        region = row.get("region", "全部")
        totals.setdefault(region, 0.0)
        totals[region] += float(row["成交额"])
    sorted_regions = sorted(totals.items(), key=lambda item: item[1], reverse=True)
    top_region, top_value = sorted_regions[0]
    return [
        f"共返回 {result['row_count']} 行聚合结果。",
        f"{top_region}成交额最高，累计为 {top_value:.0f}。",
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="smart-data-query demo cli")
    parser.add_argument("--source-type", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--glossary", required=True)
    parser.add_argument("--question")
    parser.add_argument("--follow-up-question")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--session-file")
    parser.add_argument("--enable-llm", action="store_true")
    parser.add_argument("--llm-base-url")
    parser.add_argument("--llm-model")
    args = parser.parse_args()

    if not args.question and not args.follow_up_question:
        parser.error("必须提供 --question 或 --follow-up-question。")

    if args.follow_up_question:
        if not args.session_file:
            parser.error("使用 --follow-up-question 时必须提供 --session-file。")
        payload = run_follow_up(
            follow_up_question=args.follow_up_question,
            session_file=args.session_file,
            output_dir=args.output_dir,
            enable_llm=args.enable_llm,
            llm_base_url=args.llm_base_url,
            llm_model=args.llm_model,
        )
    else:
        payload = run_query(
            question=args.question,
            source_type=args.source_type,
            source=args.source,
            schema=args.schema,
            glossary=args.glossary,
            output_dir=args.output_dir,
            enable_llm=args.enable_llm,
            session_file=args.session_file,
            llm_base_url=args.llm_base_url,
            llm_model=args.llm_model,
        )
    print(json.dumps(payload, ensure_ascii=False))


def _write_session(session_file: str, payload: dict) -> None:
    Path(session_file).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_session(session_file: str) -> dict:
    return json.loads(Path(session_file).read_text(encoding="utf-8"))


def _build_default_time_note(task: dict) -> str | None:
    if not task.get("time_was_defaulted"):
        return None
    if task.get("time_range", {}).get("preset") == "last_7_days":
        return "未指定时间范围，已按最近7天分析。"
    return None


def _build_parking_relational_result(task: dict, rows: list[dict]) -> dict:
    query_profile = task.get("query_profile")
    if query_profile == "payment_passage_reconciliation_by_date":
        filtered = [row for row in rows if row.get("mismatch_type") != "matched"]
        summary = _summarize_reconciliation_by_date(filtered, task)
        return _build_relational_result_payload(
            rows=filtered,
            summary=summary,
            chart_rows=[
                {"stat_date": row["stat_date"], "parking_lot": row["parking_lot"], "总收入": row["total_revenue"]}
                for row in filtered[:20]
            ],
            chart_spec={
                "type": "bar",
                "x_field": "stat_date",
                "y_field": "总收入",
                "series_field": "parking_lot",
            },
        )

    if query_profile == "payment_passage_reconciliation_by_plate":
        return _build_relational_result_payload(
            rows=rows,
            summary=_summarize_reconciliation_by_plate(rows),
            chart_rows=[
                {"license_plate": row["license_plate"], "parking_lot": row["parking_lot"], "停留时长": row["stay_minutes"]}
                for row in rows[:20]
            ],
            chart_spec={
                "type": "bar",
                "x_field": "license_plate",
                "y_field": "停留时长",
                "series_field": "parking_lot",
            },
        )

    if query_profile == "lot_capacity_efficiency_ranking":
        return _build_relational_result_payload(
            rows=rows,
            summary=_summarize_capacity_ranking(rows, task),
            chart_rows=[
                {"parking_lot": row["parking_lot"], "单位车位收入": row["revenue_per_space"]}
                for row in rows[:20]
            ],
            chart_spec={
                "type": "bar",
                "x_field": "parking_lot",
                "y_field": "单位车位收入",
                "series_field": None,
            },
        )

    if query_profile == "payment_method_risk_breakdown":
        return _build_relational_result_payload(
            rows=rows,
            summary=_summarize_payment_method_breakdown(rows),
            chart_rows=[
                {"payment_method": row["payment_method"], "parking_lot": row["parking_lot"], "支付失败率": row["payment_failure_rate"]}
                for row in rows[:20]
            ],
            chart_spec={
                "type": "bar",
                "x_field": "payment_method",
                "y_field": "支付失败率",
                "series_field": "parking_lot",
            },
        )

    return _build_relational_result_payload(
        rows=rows,
        summary=["当前联查结果已返回原始行，请进一步限定问题口径。"],
        chart_rows=[],
        chart_spec=task.get("chart", {}),
    )


def _build_relational_result_payload(rows: list[dict], summary: list[str], chart_rows: list[dict], chart_spec: dict) -> dict:
    return {
        "row_count": len(rows),
        "rows": rows,
        "summary": summary,
        "chart_rows": chart_rows,
        "chart_spec": chart_spec,
    }


def _summarize_reconciliation_by_date(rows: list[dict], task: dict) -> list[str]:
    if not rows:
        return ["当前时间窗口内未发现收费与通行不一致的日期。"]
    mismatch_type = task.get("constraints", {}).get("mismatch_type")
    top_row = max(rows, key=lambda row: float(row.get("total_revenue", 0) or 0))
    if mismatch_type == "payment_without_passage":
        return [
            f"共识别到 {len(rows)} 条“有收入但无通行”日期记录。",
            f"{top_row['parking_lot']} 在 {top_row['stat_date']} 的异常金额最高，收入 {top_row['total_revenue']:.0f}。",
        ]
    if mismatch_type == "passage_without_payment":
        return [
            f"共识别到 {len(rows)} 条“有通行但无收入”日期记录。",
            f"{top_row['parking_lot']} 在 {top_row['stat_date']} 的对账缺口最值得优先排查。",
        ]
    return [
        f"共识别到 {len(rows)} 条收费与通行不一致的日期记录。",
        f"{top_row['parking_lot']} 在 {top_row['stat_date']} 的异常金额最高，收入 {top_row['total_revenue']:.0f}。",
    ]


def _summarize_reconciliation_by_plate(rows: list[dict]) -> list[str]:
    if not rows:
        return ["当前时间窗口内未发现满足条件的车牌联查记录。"]
    top_row = max(rows, key=lambda row: float(row.get("stay_minutes", 0) or 0))
    return [
        f"共识别到 {len(rows)} 条车牌级联查记录。",
        f"{top_row['parking_lot']} 的 {top_row['license_plate']} 停留 {top_row['stay_minutes']:.0f} 分钟，最值得优先复核收费口径。",
    ]


def _summarize_capacity_ranking(rows: list[dict], task: dict) -> list[str]:
    if not rows:
        return ["当前时间窗口内未生成单位车位效率结果。"]
    metric = task.get("constraints", {}).get("ranking_metric", "revenue_per_space")
    top_row = max(rows, key=lambda row: float(row.get(metric, 0) or 0))
    if metric == "entry_per_space":
        return [
            f"{top_row['parking_lot']} 的单位车位车流最高，为 {top_row['entry_per_space']:.2f}。",
            f"该车场总车位 {top_row['total_spaces']:.0f}，总入场车次 {top_row['entry_count']:.0f}。",
        ]
    return [
        f"{top_row['parking_lot']} 的单位车位收入最高，为 {top_row['revenue_per_space']:.2f}。",
        f"该车场总车位 {top_row['total_spaces']:.0f}，总收入 {top_row['total_revenue']:.0f}。",
    ]


def _summarize_payment_method_breakdown(rows: list[dict]) -> list[str]:
    if not rows:
        return ["当前时间窗口内未发现支付方式相关异常。"]
    top_row = max(rows, key=lambda row: float(row.get("payment_failure_rate", 0) or 0))
    return [
        f"{top_row['parking_lot']} 的 {top_row['payment_method']} 支付失败率最高，为 {top_row['payment_failure_rate']:.1%}。",
        f"该方式共发生 {top_row['payment_count']:.0f} 笔支付，其中失败 {top_row['failure_count']:.0f} 笔。",
    ]


def _align_parking_time_range(task: dict, rows: list[dict]) -> dict:
    if not rows:
        return task
    time_range = dict(task.get("time_range") or {})
    start_value = time_range.get("start")
    end_value = time_range.get("end")
    if not start_value or not end_value:
        return task

    latest_date = max(PathDate.fromisoformat(str(row["stat_date"])) for row in rows)
    requested_end = PathDate.fromisoformat(str(end_value))
    if requested_end <= latest_date:
        return task

    if time_range.get("preset") == "today":
        shifted_range = {
            **time_range,
            "start": latest_date.isoformat(),
            "end": latest_date.isoformat(),
        }
    else:
        requested_start = PathDate.fromisoformat(str(start_value))
        window_days = max((requested_end - requested_start).days, 0)
        shifted_range = {
            **time_range,
            "start": (latest_date - TimeDelta(days=window_days)).isoformat(),
            "end": latest_date.isoformat(),
        }

    assumptions = list(task.get("assumptions", []))
    assumptions.append(f"数据最新日期为 {latest_date.isoformat()}，已将相对时间窗口对齐到最新可用数据。")
    return {
        **task,
        "time_range": shifted_range,
        "assumptions": assumptions,
    }


def _align_parking_sqlite_time_range(task: dict, source: str) -> dict:
    time_range = dict(task.get("time_range") or {})
    start_value = time_range.get("start")
    end_value = time_range.get("end")
    if not start_value or not end_value:
        return task

    latest_date = _get_latest_parking_sqlite_date(source)
    requested_end = PathDate.fromisoformat(str(end_value))
    if requested_end <= latest_date:
        return task

    if time_range.get("preset") == "today":
        shifted_range = {
            **time_range,
            "start": latest_date.isoformat(),
            "end": latest_date.isoformat(),
        }
    else:
        requested_start = PathDate.fromisoformat(str(start_value))
        window_days = max((requested_end - requested_start).days, 0)
        shifted_range = {
            **time_range,
            "start": (latest_date - TimeDelta(days=window_days)).isoformat(),
            "end": latest_date.isoformat(),
        }

    assumptions = list(task.get("assumptions", []))
    assumptions.append(f"数据最新日期为 {latest_date.isoformat()}，已将相对时间窗口对齐到最新可用数据。")
    return {
        **task,
        "time_range": shifted_range,
        "assumptions": assumptions,
    }


def _get_latest_parking_sqlite_date(source: str) -> PathDate:
    query = """
    SELECT MAX(stat_date) FROM (
        SELECT date(paid_at) AS stat_date FROM parking_payment_records WHERE paid_at IS NOT NULL
        UNION ALL
        SELECT date(entry_at) AS stat_date FROM parking_passage_records WHERE entry_at IS NOT NULL
    )
    """
    with closing(sqlite3.connect(source)) as connection:
        latest_value = connection.execute(query).fetchone()[0]
    if not latest_value:
        raise ValueError("停车经营 SQLite 数据源中没有可用日期。")
    return PathDate.fromisoformat(str(latest_value))


if __name__ == "__main__":
    main()
