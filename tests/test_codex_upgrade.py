from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import server
from scripts import sql_generator


class DetectTimeRangeTests(unittest.TestCase):
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

    def test_management_report_without_time_range_requires_clarification(self) -> None:
        result = sql_generator.normalize_question(
            "生成停车经营报告，给管理层看",
            schema_text="schema",
            glossary_text="glossary",
        )

        self.assertEqual(result["intent"], "parking_management_report")
        self.assertTrue(result["needs_clarification"])
        self.assertIn("时间范围", result["clarifying_question"])

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
                "intent": "parking_management_report",
                "report_type": "weekly",
                "time_range": {
                    "preset": "last_7_days",
                    "start": "2026-03-03",
                    "end": "2026-03-09",
                },
                "focus_entities": ["A停车场"],
                "focus_metrics": ["total_revenue", "occupancy_rate"],
                "needs_clarification": False,
                "clarification_question": None,
            },
        )

        self.assertEqual(result["intent"], "parking_management_report")
        self.assertEqual(result["time_range"]["preset"], "last_7_days")
        self.assertEqual(result["focus_entities"], ["A停车场"])
        self.assertEqual(result["planner_mode"], "llm")

    def test_normalize_question_falls_back_to_rule_when_planner_result_is_invalid(self) -> None:
        result = sql_generator.normalize_question(
            "给老板看下今天经营情况",
            schema_text="schema",
            glossary_text="glossary",
            planner=lambda *_args, **_kwargs: {
                "domain": "parking_ops",
                "intent": "not_supported",
            },
        )

        self.assertEqual(result["intent"], "parking_management_daily_report")
        self.assertEqual(result["time_range"]["preset"], "today")
        self.assertEqual(result["planner_mode"], "rule_fallback")


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
