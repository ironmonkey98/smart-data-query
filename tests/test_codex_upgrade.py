from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import server
from scripts import sql_generator
from scripts.connect_db import load_dataset
from scripts.chart_render import render_svg_chart
from scripts.llm_enhancer import enhance_analysis, handle_follow_up
from scripts.parking_analyst import diagnose_parking_operation


class DetectTimeRangeTests(unittest.TestCase):
    def test_detect_time_range_supports_last_year_specific_month(self) -> None:
        result = sql_generator._detect_time_range("去年 2 月，停车情况是好转还是变坏，为什么")

        self.assertEqual(result["preset"], "last_year_month")
        self.assertEqual(result["start"], "2025-02-01")
        self.assertEqual(result["end"], "2025-02-28")

    def test_detect_time_range_supports_last_month(self) -> None:
        result = sql_generator._detect_time_range("上个月各区域销售排名")

        self.assertEqual(result["preset"], "last_month")
        self.assertIsNotNone(result["start"])
        self.assertIsNotNone(result["end"])

    def test_detect_time_range_supports_this_week(self) -> None:
        result = sql_generator._detect_time_range("本周华东成交额对比")

        self.assertEqual(result["preset"], "this_week")
        self.assertIsNotNone(result["start"])
        self.assertIsNotNone(result["end"])

    def test_management_report_without_time_range_defaults_to_last_7_days(self) -> None:
        result = sql_generator.normalize_question(
            "生成停车经营报告，给管理层看",
            schema_text="schema",
            glossary_text="glossary",
        )

        self.assertEqual(result["intent"], "parking_management_report")
        self.assertFalse(result["needs_clarification"])
        self.assertEqual(result["time_range"]["preset"], "last_7_days")
        self.assertTrue(result["time_was_defaulted"])

    def test_daily_management_report_intent_for_today_phrase(self) -> None:
        result = sql_generator.normalize_question(
            "给老板看下今天经营情况",
            schema_text="schema",
            glossary_text="glossary",
        )

        self.assertEqual(result["intent"], "parking_management_daily_report")
        self.assertEqual(result["time_range"]["preset"], "today")
        self.assertFalse(result["needs_clarification"])

    def test_daily_management_report_intent_for_daily_report_phrase(self) -> None:
        result = sql_generator.normalize_question(
            "做个停车经营日报给管理层",
            schema_text="schema",
            glossary_text="glossary",
        )

        self.assertEqual(result["intent"], "parking_management_daily_report")
        self.assertFalse(result["needs_clarification"])

    def test_parking_anomaly_colloquial_phrase_maps_to_parking_domain(self) -> None:
        result = sql_generator.normalize_question(
            "哪个场子今天有问题",
            schema_text="schema",
            glossary_text="glossary",
        )

        self.assertIn(result["intent"], {"parking_anomaly_diagnosis", "parking_management_daily_report"})
        self.assertEqual(result.get("domain"), "parking_ops")

    def test_normalize_question_prefers_llm_planner_when_result_is_valid(self) -> None:
        result = sql_generator.normalize_question(
            "把最近7天停车盘子给老板捋一版",
            schema_text="schema",
            glossary_text="glossary",
            planner=lambda *_args, **_kwargs: {
                "domain": "parking_ops",
                "business_goal": "management_reporting",
                "analysis_job": "operational_overview",
                "decision_scope": "executive",
                "deliverable": "web_report",
                "time_scope": {
                    "preset": "last_7_days",
                    "start": "2026-03-03",
                    "end": "2026-03-09",
                },
                "focus_entities": ["A停车场"],
                "focus_dimensions": ["parking_lot"],
                "focus_metrics": ["total_revenue", "occupancy_rate"],
                "implicit_requirements": ["summary_first", "actionable_recommendations"],
                "missing_information": [],
            },
        )

        self.assertEqual(result["intent"], "parking_management_report")
        self.assertEqual(result["time_range"]["preset"], "last_7_days")
        self.assertEqual(result["focus_entities"], ["A停车场"])
        self.assertEqual(result["planner_mode"], "llm")
        self.assertEqual(result["semantic_plan"]["business_goal"], "management_reporting")
        self.assertEqual(result["semantic_plan"]["deliverable"], "web_report")

    def test_normalize_question_falls_back_to_rule_when_planner_result_is_invalid(self) -> None:
        result = sql_generator.normalize_question(
            "给老板看下今天经营情况",
            schema_text="schema",
            glossary_text="glossary",
            planner=lambda *_args, **_kwargs: {
                "domain": "parking_ops",
                "business_goal": "unknown_goal",
            },
        )

        self.assertEqual(result["intent"], "parking_management_daily_report")
        self.assertEqual(result["time_range"]["preset"], "today")
        self.assertEqual(result["planner_mode"], "rule_fallback")

    def test_normalize_question_maps_semantic_daily_brief_to_daily_report(self) -> None:
        result = sql_generator.normalize_question(
            "给老板看下今天经营情况",
            schema_text="schema",
            glossary_text="glossary",
            planner=lambda *_args, **_kwargs: {
                "domain": "parking_ops",
                "business_goal": "management_reporting",
                "analysis_job": "operational_overview",
                "decision_scope": "executive",
                "deliverable": "daily_brief",
                "time_scope": {
                    "preset": "today",
                    "start": "2026-03-09",
                    "end": "2026-03-09",
                },
                "focus_entities": [],
                "focus_dimensions": ["parking_lot"],
                "focus_metrics": ["total_revenue", "payment_failure_rate"],
                "implicit_requirements": ["summary_first"],
                "missing_information": [],
            },
        )

        self.assertEqual(result["intent"], "parking_management_daily_report")
        self.assertEqual(result["report_type"], "daily")
        self.assertEqual(result["semantic_plan"]["deliverable"], "daily_brief")

    def test_normalize_question_maps_period_assessment_semantic_plan_to_new_task(self) -> None:
        result = sql_generator.normalize_question(
            "去年 2 月，停车情况是好转还是变坏，为什么",
            schema_text="schema",
            glossary_text="glossary",
            planner=lambda *_args, **_kwargs: {
                "domain": "parking_ops",
                "business_goal": "management_reporting",
                "analysis_job": "period_assessment",
                "decision_scope": "operations",
                "deliverable": "summary",
                "time_scope": {
                    "kind": "relative_year_month",
                    "preset": None,
                    "anchor": {"relative_year": -1, "month": 2},
                    "start": "2025-02-01",
                    "end": "2025-02-28",
                },
                "focus_entities": [],
                "focus_dimensions": ["parking_lot"],
                "focus_metrics": ["total_revenue", "entry_count", "occupancy_rate"],
                "implicit_requirements": ["comparison_required", "reason_explanation"],
                "missing_information": [],
            },
        )

        self.assertEqual(result["intent"], "parking_period_assessment")
        self.assertEqual(result["time_range"]["start"], "2025-02-01")
        self.assertEqual(result["time_range"]["end"], "2025-02-28")
        self.assertEqual(result["semantic_plan"]["analysis_job"], "period_assessment")
        self.assertEqual(result["semantic_plan"]["time_scope"]["kind"], "relative_year_month")
        self.assertIn("comparison_required", result["semantic_plan"]["implicit_requirements"])
        self.assertIn("reason_explanation", result["semantic_plan"]["implicit_requirements"])

    def test_normalize_question_defaults_missing_time_information_for_non_compare_question(self) -> None:
        result = sql_generator.normalize_question(
            "生成停车经营报告，给管理层看",
            schema_text="schema",
            glossary_text="glossary",
            planner=lambda *_args, **_kwargs: {
                "domain": "parking_ops",
                "business_goal": "management_reporting",
                "analysis_job": "operational_overview",
                "decision_scope": "executive",
                "deliverable": "web_report",
                "time_scope": {
                    "preset": "all",
                    "start": None,
                    "end": None,
                },
                "focus_entities": [],
                "focus_dimensions": ["parking_lot"],
                "focus_metrics": ["total_revenue"],
                "implicit_requirements": ["summary_first"],
                "missing_information": ["time_scope"],
            },
        )

        self.assertFalse(result["needs_clarification"])
        self.assertEqual(result["time_range"]["preset"], "last_7_days")
        self.assertTrue(result["time_was_defaulted"])
        self.assertEqual(result["semantic_plan"]["missing_information"], ["time_scope"])

    def test_parking_revenue_question_without_time_defaults_to_last_7_days(self) -> None:
        result = sql_generator.normalize_question(
            "哪个车场收入下滑最明显，原因是什么",
            schema_text="schema",
            glossary_text="glossary",
        )

        self.assertEqual(result["intent"], "parking_revenue_analysis")
        self.assertFalse(result["needs_clarification"])
        self.assertEqual(result["time_range"]["preset"], "last_7_days")
        self.assertTrue(result["time_was_defaulted"])

    def test_parking_compare_question_without_time_still_requires_clarification(self) -> None:
        result = sql_generator.normalize_question(
            "停车经营环比怎么样",
            schema_text="schema",
            glossary_text="glossary",
        )

        self.assertTrue(result["needs_clarification"])
        self.assertIn("时间范围", result["clarifying_question"])

    def test_colloquial_management_question_maps_to_management_report(self) -> None:
        result = sql_generator.normalize_question(
            "把最近7天五个场子的经营盘子捋一版",
            schema_text="schema",
            glossary_text="glossary",
        )

        self.assertEqual(result["intent"], "parking_management_report")
        self.assertEqual(result["query_profile"], "parking_daily_overview_join")

    def test_parking_condition_question_maps_to_period_assessment_with_specific_month(self) -> None:
        result = sql_generator.normalize_question(
            "去年 2 月，停车情况是好转还是变坏，为什么",
            schema_text="schema",
            glossary_text="glossary",
        )

        self.assertEqual(result["intent"], "parking_period_assessment")
        self.assertEqual(result["time_range"]["start"], "2025-02-01")
        self.assertEqual(result["time_range"]["end"], "2025-02-28")
        self.assertFalse(result["time_was_defaulted"])

    def test_relational_reconciliation_question_maps_to_date_join_profile(self) -> None:
        result = sql_generator.normalize_question(
            "把收费流水和通行记录联起来看，找出有收入但没通行的日期",
            schema_text="schema",
            glossary_text="glossary",
        )

        self.assertEqual(result["intent"], "parking_relational_query")
        self.assertEqual(result["query_profile"], "payment_passage_reconciliation_by_date")
        self.assertEqual(result["constraints"]["mismatch_type"], "payment_without_passage")
        self.assertFalse(result["needs_clarification"])

    def test_plate_join_question_requires_fee_definition_clarification(self) -> None:
        result = sql_generator.normalize_question(
            "按车牌把支付和通行打通，看哪些车长时间停留但收费偏低",
            schema_text="schema",
            glossary_text="glossary",
        )

        self.assertEqual(result["query_profile"], "payment_passage_reconciliation_by_plate")
        self.assertTrue(result["needs_clarification"])
        self.assertIn("收费偏低", result["clarifying_question"])

    def test_payment_method_question_with_unsupported_abnormal_open_requests_clarification(self) -> None:
        result = sql_generator.normalize_question(
            "看下各车场支付方式和异常开闸的关系",
            schema_text="schema",
            glossary_text="glossary",
        )

        self.assertEqual(result["query_profile"], "payment_method_risk_breakdown")
        self.assertTrue(result["needs_clarification"])
        self.assertIn("异常开闸", result["clarifying_question"])

    def test_free_release_and_low_occupancy_question_maps_to_anomaly_analysis(self) -> None:
        result = sql_generator.normalize_question(
            "免费放行多且利用率低的是哪些场子",
            schema_text="schema",
            glossary_text="glossary",
        )

        self.assertEqual(result["intent"], "parking_anomaly_diagnosis")
        self.assertEqual(result["query_profile"], "parking_daily_overview_join")
        self.assertIn("free_release_count", result["focus_metrics"])
        self.assertIn("occupancy_rate", result["focus_metrics"])


class ComparePeriodsTests(unittest.TestCase):
    def test_compare_periods_rejects_parking_data_source(self) -> None:
        result = server._handle_compare_periods(
            {
                "source_name": "parking_ops",
                "question": "最近7天停车经营周报",
                "period_a": "本周",
                "period_b": "上周",
            },
            "compare-periods-test",
        )

        self.assertEqual(
            result["error"],
            "compare_periods 当前仅支持 sales 数据源，请直接使用 query_data 处理停车经营报表。",
        )


class ReflectTests(unittest.TestCase):
    def test_handle_reflect_returns_plan(self) -> None:
        result = server._handle_reflect({"plan": "1. 查看停车收入\n2. 对比异常指标"})

        self.assertEqual(result["status"], "ok")
        self.assertIn("查看停车收入", result["plan"])


class SemanticExecutionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rows = load_dataset(
            source="/Users/yehong/smart-data-query 3/data/sample_parking_ops.csv",
            source_type="csv",
        )

    def test_semantic_plan_can_override_intent_to_daily_report(self) -> None:
        task = {
            "intent": "parking_management_report",
            "domain": "parking_ops",
            "entity_field": "parking_lot",
            "time_field": "stat_date",
            "time_range": {"preset": "today", "start": "2026-03-09", "end": "2026-03-09"},
            "focus_metrics": ["total_revenue", "payment_failure_rate"],
            "focus_entities": [],
            "semantic_plan": {
                "business_goal": "management_reporting",
                "analysis_job": "operational_overview",
                "decision_scope": "executive",
                "deliverable": "daily_brief",
                "focus_metrics": ["total_revenue", "payment_failure_rate"],
                "focus_entities": [],
            },
        }

        analysis = diagnose_parking_operation(self.rows, task)

        self.assertEqual(analysis["analysis_type"], "management_report")
        self.assertEqual(analysis["report_type"], "daily")

    def test_semantic_plan_can_override_intent_to_anomaly_analysis(self) -> None:
        task = {
            "intent": "parking_management_report",
            "domain": "parking_ops",
            "entity_field": "parking_lot",
            "time_field": "stat_date",
            "time_range": {"preset": "last_7_days", "start": "2026-03-03", "end": "2026-03-09"},
            "focus_metrics": ["payment_failure_rate"],
            "focus_entities": [],
            "semantic_plan": {
                "business_goal": "risk_detection",
                "analysis_job": "anomaly_focus",
                "decision_scope": "operations",
                "deliverable": None,
                "focus_metrics": ["payment_failure_rate"],
                "focus_entities": [],
            },
        }

        analysis = diagnose_parking_operation(self.rows, task)

        self.assertEqual(analysis["analysis_type"], "anomaly")
        self.assertIn("风险", analysis["executive_summary"][1])

    def test_management_report_prioritizes_anomaly_focus_from_semantic_plan(self) -> None:
        task = {
            "intent": "parking_management_report",
            "domain": "parking_ops",
            "entity_field": "parking_lot",
            "time_field": "stat_date",
            "time_range": {"preset": "last_7_days", "start": "2026-03-03", "end": "2026-03-09"},
            "focus_metrics": ["payment_failure_rate", "abnormal_open_count"],
            "focus_entities": [],
            "semantic_plan": {
                "business_goal": "management_reporting",
                "analysis_job": "operational_overview",
                "decision_scope": "executive",
                "deliverable": "web_report",
                "focus_metrics": ["payment_failure_rate", "abnormal_open_count"],
                "focus_entities": [],
            },
        }

        analysis = diagnose_parking_operation(self.rows, task)

        self.assertEqual(analysis["analysis_type"], "management_report")
        self.assertEqual(analysis["focus_lots"][0]["topic"], "经营异常")
        self.assertIn("风险异常", analysis["executive_summary"][1])

    def test_management_report_uses_executive_clean_chart_style(self) -> None:
        task = {
            "intent": "parking_management_report",
            "domain": "parking_ops",
            "entity_field": "parking_lot",
            "time_field": "stat_date",
            "time_range": {"preset": "last_7_days", "start": "2026-03-03", "end": "2026-03-09"},
            "focus_metrics": ["total_revenue"],
            "focus_entities": [],
            "semantic_plan": {
                "business_goal": "management_reporting",
                "analysis_job": "operational_overview",
                "decision_scope": "executive",
                "deliverable": "web_report",
                "focus_metrics": ["total_revenue"],
                "focus_entities": [],
            },
        }

        analysis = diagnose_parking_operation(self.rows, task)

        self.assertEqual(analysis["chart_spec"]["type"], "line")
        self.assertEqual(analysis["chart_spec"]["style_preset"], "executive_clean")
        self.assertEqual(analysis["chart_spec"]["chart_family"], "trend")

    def test_management_report_uses_comparison_summary_when_previous_period_exists(self) -> None:
        task = {
            "intent": "parking_management_report",
            "domain": "parking_ops",
            "entity_field": "parking_lot",
            "time_field": "stat_date",
            "time_range": {"preset": "last_year_month", "start": "2025-02-01", "end": "2025-02-28"},
            "comparison_range": {"start": "2025-01-04", "end": "2025-01-31"},
            "focus_metrics": ["total_revenue"],
            "focus_entities": [],
            "semantic_plan": {
                "business_goal": "management_reporting",
                "analysis_job": "operational_overview",
                "decision_scope": "operations",
                "deliverable": None,
                "focus_metrics": ["total_revenue"],
                "focus_entities": [],
            },
        }

        analysis = diagnose_parking_operation(self.rows, task)

        self.assertIn("与上一周期相比", analysis["executive_summary"][0])
        self.assertTrue(any(token in analysis["executive_summary"][0] for token in ("好转", "变坏")))

    def test_period_assessment_returns_trend_and_reason_factors(self) -> None:
        task = {
            "intent": "parking_period_assessment",
            "domain": "parking_ops",
            "entity_field": "parking_lot",
            "time_field": "stat_date",
            "time_range": {"preset": "last_year_month", "start": "2025-02-01", "end": "2025-02-28"},
            "comparison_range": {"start": "2025-01-04", "end": "2025-01-31"},
            "focus_metrics": ["total_revenue", "entry_count", "occupancy_rate"],
            "focus_entities": [],
            "semantic_plan": {
                "business_goal": "management_reporting",
                "analysis_job": "period_assessment",
                "decision_scope": "operations",
                "deliverable": "summary",
                "focus_metrics": ["total_revenue", "entry_count", "occupancy_rate"],
                "focus_entities": [],
                "implicit_requirements": ["comparison_required", "reason_explanation"],
            },
        }

        analysis = diagnose_parking_operation(self.rows, task)

        self.assertEqual(analysis["analysis_type"], "period_assessment")
        self.assertIn(analysis["trend"], {"improved", "worsened", "mixed"})
        self.assertTrue(analysis["reason_factors"])
        self.assertIn("与上一周期相比", analysis["comparison_summary"])

    def test_anomaly_analysis_uses_bar_chart_for_risk_overview(self) -> None:
        task = {
            "intent": "parking_anomaly_diagnosis",
            "domain": "parking_ops",
            "entity_field": "parking_lot",
            "time_field": "stat_date",
            "time_range": {"preset": "last_7_days", "start": "2026-03-03", "end": "2026-03-09"},
            "focus_metrics": ["payment_failure_rate"],
            "focus_entities": [],
            "semantic_plan": {
                "business_goal": "risk_detection",
                "analysis_job": "anomaly_focus",
                "decision_scope": "operations",
                "deliverable": None,
                "focus_metrics": ["payment_failure_rate"],
                "focus_entities": [],
            },
        }

        analysis = diagnose_parking_operation(self.rows, task)

        self.assertEqual(analysis["chart_spec"]["type"], "bar")
        self.assertEqual(analysis["chart_spec"]["style_preset"], "executive_risk")
        self.assertEqual(analysis["chart_spec"]["chart_family"], "risk_overview")


class ChartRenderTests(unittest.TestCase):
    def test_render_svg_chart_supports_bar_chart_with_risk_style(self) -> None:
        rows = [
            {"parking_lot": "B停车场", "风险分": 92},
            {"parking_lot": "E停车场", "风险分": 61},
        ]
        chart_spec = {
            "type": "bar",
            "x_field": "parking_lot",
            "y_field": "风险分",
            "series_field": None,
            "style_preset": "executive_risk",
            "chart_family": "risk_overview",
            "tone": "executive",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            output = render_svg_chart(rows, chart_spec, str(Path(tmpdir) / "chart.svg"), "风险概览")
            svg = Path(output).read_text(encoding="utf-8")

        self.assertIn("risk-overview", svg)
        self.assertIn("fill=\"#fff4f1\"", svg)
        self.assertIn("<rect", svg)


class SemanticNarrativeTests(unittest.TestCase):
    def test_enhance_analysis_uses_semantic_plan_for_management_narrative(self) -> None:
        task = {
            "intent": "parking_management_report",
            "semantic_plan": {
                "business_goal": "management_reporting",
                "analysis_job": "operational_overview",
                "decision_scope": "executive",
                "deliverable": "web_report",
                "focus_metrics": ["payment_failure_rate", "abnormal_open_count"],
            },
        }
        analysis = {
            "analysis_type": "management_report",
            "report_type": "weekly",
            "overview": {
                "total_revenue": 120000,
                "total_entry_count": 6000,
                "avg_occupancy_rate": 0.81,
                "high_risk_lot_count": 2,
            },
        }

        result = enhance_analysis(task=task, analysis=analysis, enable_llm=False)

        self.assertEqual(result["mode"], "rule")
        self.assertIn("高风险车场", result["narrative"])
        self.assertIn("高风险点集中在哪些指标", result["follow_up_suggestions"])

    def test_handle_follow_up_uses_semantic_plan_to_answer_risk_question(self) -> None:
        session_payload = {
            "task": {
                "intent": "parking_management_report",
                "semantic_plan": {
                    "business_goal": "management_reporting",
                    "analysis_job": "operational_overview",
                    "decision_scope": "executive",
                    "deliverable": "web_report",
                    "focus_metrics": ["payment_failure_rate"],
                },
            },
            "analysis": {
                "analysis_type": "management_report",
                "report_type": "weekly",
                "focus_lots": [
                    {"parking_lot": "B停车场", "topic": "经营异常", "summary": "支付失败率高，异常开闸增加。"},
                ],
                "priority_actions": ["立即检查支付通道稳定性。"],
            },
            "narrative": {"narrative": "需优先关注异常车场。"},
        }

        result = handle_follow_up(
            follow_up_question="高风险点主要在哪些指标？",
            session_payload=session_payload,
            enable_llm=False,
        )

        self.assertEqual(result["mode"], "rule")
        self.assertIn("支付失败率", result["answer"])


class MemoryInsightTests(unittest.TestCase):
    def test_save_insight_and_load_memory_context(self) -> None:
        original_base_dir = server.BASE_DIR
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                server.BASE_DIR = Path(tmpdir)

                saved = server._handle_save_insight(
                    {"topic": "B停车场故障", "insight": "支付失败率超过5%"}
                )

                self.assertEqual(saved["status"], "saved")
                memory_file = Path(tmpdir) / "memory" / "insights.jsonl"
                self.assertTrue(memory_file.exists())

                context = server._load_memory_context()

                self.assertIn("历史记忆", context)
                self.assertIn("B停车场故障", context)
                self.assertIn("支付失败率超过5%", context)
        finally:
            server.BASE_DIR = original_base_dir


if __name__ == "__main__":
    unittest.main()
