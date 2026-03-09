from __future__ import annotations

import argparse
import json
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
) -> dict:
    schema_text = Path(schema).read_text(encoding="utf-8")
    glossary_text = Path(glossary).read_text(encoding="utf-8")
    task = normalize_question(
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
    if task.get("needs_clarification"):
        payload = {
            "task": task,
            "needs_clarification": True,
            "clarifying_question": task["clarifying_question"],
        }
        summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        if session_file:
            _write_session(session_file, payload)
        return payload

    rows = load_dataset(source=source, source_type=source_type)

    if task["intent"].startswith("parking_"):
        analysis = diagnose_parking_operation(rows, task)
        narrative = enhance_analysis(
            task=task,
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
            "task": task,
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
        }
    else:
        result = execute_structured_task(rows=rows, task=task)
        try:
            render_svg_chart(
                rows=result["rows"],
                chart_spec=task["chart"],
                output_path=str(chart_path),
                title="智能问数结果",
            )
        except NotImplementedError:
            pass  # 图表类型暂不支持，跳过渲染，数据结果照常返回
        payload = {
            "task": task,
            "result": result,
            "artifacts": {
                "summary": str(summary_path),
                "chart": str(chart_path),
            },
            "summary": build_summary(result),
            # 供 Claude 做追问分析用的原始行样本（最多60行）
            "rows_sample": result.get("rows", [])[:60],
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


if __name__ == "__main__":
    main()
