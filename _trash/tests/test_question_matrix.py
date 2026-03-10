from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from scripts import sql_generator
from scripts.parking_skill_runtime import detect_parking_task, select_skill_name, should_handle_with_runtime


SCHEMA_TEXT = Path("references/db-schema.md").read_text(encoding="utf-8")
GLOSSARY_TEXT = Path("references/term-glossary.md").read_text(encoding="utf-8")
SCHEMA_PATH = "references/db-schema.md"
GLOSSARY_PATH = "references/term-glossary.md"


def _planner(plan: dict):
    return lambda *_args, **_kwargs: plan


def _build_plan(
    *,
    business_goal: str,
    analysis_job: str,
    decision_scope: str,
    deliverable: str | None,
    time_scope: dict,
    focus_entities: list[str] | None = None,
    focus_metrics: list[str] | None = None,
    query_profile: str | None = None,
    sub_questions: list[dict] | None = None,
    missing_information: list[str] | None = None,
) -> dict:
    return {
        "domain": "parking_ops",
        "business_goal": business_goal,
        "analysis_job": analysis_job,
        "decision_scope": decision_scope,
        "deliverable": deliverable,
        "time_scope": time_scope,
        "focus_entities": focus_entities or [],
        "focus_dimensions": ["parking_lot"],
        "focus_metrics": focus_metrics or ["total_revenue"],
        "query_profile": query_profile,
        "sub_questions": sub_questions or [],
        "implicit_requirements": [],
        "missing_information": missing_information or [],
    }


def _get_value(payload: dict, path: str):
    current = payload
    for part in path.split("."):
        current = current[part]
    return current


RULE_NORMALIZE_CASES = [
    {
        "name": "parking_weekly_report_recent7",
        "question": "生成最近7天停车经营周报，给管理层看",
        "expected": {
            "intent": "parking_management_report",
            "domain": "parking_ops",
            "query_profile": "parking_daily_overview_join",
            "report_type": "weekly",
            "time_range.preset": "last_7_days",
        },
    },
    {
        "name": "parking_daily_boss_today",
        "question": "给老板看下今天经营情况",
        "expected": {
            "intent": "parking_management_daily_report",
            "domain": "parking_ops",
            "report_type": "daily",
            "time_range.preset": "today",
        },
    },
    {
        "name": "parking_daily_report_without_time",
        "question": "做个停车经营日报给管理层",
        "expected": {
            "intent": "parking_management_daily_report",
            "domain": "parking_ops",
            "report_type": "daily",
            "time_range.preset": "today",
        },
    },
    {
        "name": "parking_weekly_colloquial",
        "question": "把最近7天五个场子的经营盘子捋一版",
        "expected": {
            "intent": "parking_management_report",
            "query_profile": "parking_daily_overview_join",
            "time_range.preset": "last_7_days",
        },
    },
    {
        "name": "parking_anomaly_this_week",
        "question": "哪个场子这周最不正常",
        "expected": {
            "intent": "parking_anomaly_diagnosis",
            "time_range.preset": "this_week",
        },
    },
    {
        "name": "parking_anomaly_today",
        "question": "哪个场子今天有问题",
        "expected": {
            "intent": "parking_anomaly_diagnosis",
            "time_range.preset": "today",
        },
    },
    {
        "name": "parking_anomaly_free_release_low_occupancy",
        "question": "免费放行多且利用率低的是哪些场子",
        "expected": {
            "intent": "parking_anomaly_diagnosis",
            "query_profile": "parking_daily_overview_join",
        },
        "contains": {
            "focus_metrics": ["free_release_count", "occupancy_rate"],
        },
    },
    {
        "name": "parking_revenue_drop",
        "question": "哪个车场收入下滑最明显，原因是什么",
        "expected": {
            "intent": "parking_revenue_analysis",
            "time_range.preset": "last_7_days",
        },
    },
    {
        "name": "parking_payment_failure_highest",
        "question": "哪个车场支付失败率最高？最近有没有异常？",
        "expected": {
            "intent": "parking_anomaly_diagnosis",
            "query_profile": "parking_daily_overview_join",
        },
        "contains": {"focus_metrics": ["payment_failure_rate"]},
    },
    {
        "name": "parking_flow_phrase_current_behavior",
        "question": "哪个车场车流和利用率下滑最明显",
        "expected": {
            "intent": "parking_anomaly_diagnosis",
            "time_range.preset": "last_7_days",
        },
        "contains": {"focus_metrics": ["entry_count", "occupancy_rate"]},
    },
    {
        "name": "parking_period_assessment_last_year_month",
        "question": "去年 2 月，停车情况是好转还是变坏，为什么",
        "expected": {
            "intent": "parking_period_assessment",
            "report_type": "summary",
            "time_range.start": "2025-02-01",
            "time_range.end": "2025-02-28",
        },
    },
    {
        "name": "parking_management_report_defaults_last7",
        "question": "生成停车经营报告，给管理层看",
        "expected": {
            "intent": "parking_management_report",
            "time_range.preset": "last_7_days",
            "time_was_defaulted": True,
        },
    },
    {
        "name": "parking_compare_requires_clarification",
        "question": "停车经营环比怎么样",
        "expected": {
            "intent": "parking_management_report",
            "needs_clarification": True,
            "time_range.preset": "all",
        },
        "contains": {"clarifying_question": ["时间范围"]},
    },
    {
        "name": "parking_relational_date_join",
        "question": "把收费流水和通行记录联起来看，找出有收入但没通行的日期",
        "expected": {
            "intent": "parking_relational_query",
            "query_profile": "payment_passage_reconciliation_by_date",
            "constraints.mismatch_type": "payment_without_passage",
        },
    },
    {
        "name": "parking_relational_plate_join_clarify",
        "question": "按车牌把支付和通行打通，看哪些车长时间停留但收费偏低",
        "expected": {
            "intent": "parking_relational_query",
            "query_profile": "payment_passage_reconciliation_by_plate",
            "needs_clarification": True,
        },
        "contains": {"clarifying_question": ["收费偏低"]},
    },
    {
        "name": "parking_payment_method_breakdown_clarify",
        "question": "看下各车场支付方式和异常开闸的关系",
        "expected": {
            "intent": "parking_relational_query",
            "query_profile": "payment_method_risk_breakdown",
            "needs_clarification": True,
        },
        "contains": {"clarifying_question": ["异常开闸"]},
    },
    {
        "name": "parking_capacity_ranking",
        "question": "把停车场基础数据也带上，算单位车位收入排名",
        "expected": {
            "intent": "parking_relational_query",
            "query_profile": "lot_capacity_efficiency_ranking",
        },
    },
    {
        "name": "parking_payment_method_distribution",
        "question": "看下各车场支付方式分布",
        "expected": {
            "intent": "parking_relational_query",
            "query_profile": "payment_method_risk_breakdown",
            "needs_clarification": False,
        },
    },
    {
        "name": "parking_last_week_abnormal_open",
        "question": "上周各车场异常开闸情况",
        "expected": {
            "intent": "parking_anomaly_diagnosis",
            "time_range.preset": "last_week",
        },
        "contains": {"focus_metrics": ["abnormal_open_count"]},
    },
    {
        "name": "parking_today_management_short",
        "question": "今天经营情况，给老板看",
        "expected": {
            "intent": "parking_management_daily_report",
            "time_range.preset": "today",
        },
    },
    {
        "name": "sales_region_trend",
        "question": "最近30天华东和华南的成交额趋势",
        "expected": {
            "intent": "trend_compare",
            "time_range.preset": "last_30_days",
        },
        "contains": {"filters": ["华东", "华南"]},
    },
    {
        "name": "sales_last_month_compare",
        "question": "上个月各区域销售情况对比",
        "expected": {
            "intent": "compare",
            "time_range.preset": "last_month",
        },
    },
    {
        "name": "sales_last_month_summary",
        "question": "上个月各区域销售汇总",
        "expected": {
            "intent": "summary",
            "time_range.preset": "last_month",
        },
    },
    {
        "name": "sales_this_week_compare",
        "question": "本周华东成交额对比",
        "expected": {
            "intent": "compare",
            "time_range.preset": "this_week",
        },
        "contains": {"filters": ["华东"]},
    },
    {
        "name": "sales_exclude_refunds",
        "question": "最近30天华南成交额，排除退款",
        "expected": {
            "intent": "trend_compare",
            "time_range.preset": "last_30_days",
        },
        "contains": {"filters": ["华南", "paid"]},
    },
]


PLANNER_CASES = [
    {
        "name": "llm_management_weekly_report",
        "question": "把最近7天停车盘子给老板捋一版",
        "plan": _build_plan(
            business_goal="management_reporting",
            analysis_job="operational_overview",
            decision_scope="executive",
            deliverable="web_report",
            time_scope={"preset": "last_7_days", "start": "2026-03-03", "end": "2026-03-09"},
        ),
        "expected": {"intent": "parking_management_report", "planner_mode": "llm", "report_type": "weekly"},
    },
    {
        "name": "llm_management_daily_report",
        "question": "给老板看下今天经营情况",
        "plan": _build_plan(
            business_goal="management_reporting",
            analysis_job="operational_overview",
            decision_scope="executive",
            deliverable="daily_brief",
            time_scope={"preset": "today", "start": "2026-03-09", "end": "2026-03-09"},
        ),
        "expected": {"intent": "parking_management_daily_report", "report_type": "daily", "planner_mode": "llm"},
    },
    {
        "name": "llm_period_assessment_relative_year_month",
        "question": "去年 2 月，停车情况是好转还是变坏，为什么",
        "plan": _build_plan(
            business_goal="management_reporting",
            analysis_job="period_assessment",
            decision_scope="operations",
            deliverable="summary",
            time_scope={
                "kind": "relative_year_month",
                "preset": None,
                "anchor": {"relative_year": -1, "month": 2},
                "start": "2025-02-01",
                "end": "2025-02-28",
            },
            focus_metrics=["total_revenue", "entry_count", "occupancy_rate"],
        ),
        "expected": {"intent": "parking_period_assessment", "time_range.start": "2025-02-01"},
    },
    {
        "name": "llm_anomaly_focus",
        "question": "最近7天哪个车场异常最多",
        "plan": _build_plan(
            business_goal="risk_detection",
            analysis_job="anomaly_focus",
            decision_scope="operations",
            deliverable=None,
            time_scope={"preset": "last_7_days", "start": "2026-03-03", "end": "2026-03-09"},
            focus_metrics=["payment_failure_rate"],
        ),
        "expected": {"intent": "parking_anomaly_diagnosis", "planner_mode": "llm"},
    },
    {
        "name": "llm_efficiency_focus",
        "question": "最近7天哪个车场效率最低",
        "plan": _build_plan(
            business_goal="efficiency_diagnosis",
            analysis_job="flow_or_occupancy",
            decision_scope="operations",
            deliverable=None,
            time_scope={"preset": "last_7_days", "start": "2026-03-03", "end": "2026-03-09"},
            focus_metrics=["entry_count", "occupancy_rate"],
        ),
        "expected": {"intent": "parking_flow_efficiency_analysis", "planner_mode": "llm"},
    },
    {
        "name": "llm_revenue_focus",
        "question": "最近7天哪个车场营收最差",
        "plan": _build_plan(
            business_goal="revenue_diagnosis",
            analysis_job="revenue_focus",
            decision_scope="operations",
            deliverable=None,
            time_scope={"preset": "last_7_days", "start": "2026-03-03", "end": "2026-03-09"},
            focus_metrics=["total_revenue"],
        ),
        "expected": {"intent": "parking_revenue_analysis", "planner_mode": "llm"},
    },
    {
        "name": "llm_missing_time_defaults",
        "question": "生成停车经营报告，给管理层看",
        "plan": _build_plan(
            business_goal="management_reporting",
            analysis_job="operational_overview",
            decision_scope="executive",
            deliverable="web_report",
            time_scope={"preset": "all", "start": None, "end": None},
            missing_information=["time_scope"],
        ),
        "expected": {"intent": "parking_management_report", "time_range.preset": "last_7_days", "time_was_defaulted": True},
    },
    {
        "name": "llm_missing_time_compare_clarifies",
        "question": "停车经营环比怎么样",
        "plan": _build_plan(
            business_goal="management_reporting",
            analysis_job="operational_overview",
            decision_scope="executive",
            deliverable="summary",
            time_scope={"preset": "all", "start": None, "end": None},
            missing_information=["time_scope"],
        ),
        "expected": {"needs_clarification": True, "time_range.preset": "all"},
    },
    {
        "name": "llm_focus_entities_preserved",
        "question": "高林去年 2 月经营情况怎么样",
        "plan": _build_plan(
            business_goal="management_reporting",
            analysis_job="period_assessment",
            decision_scope="operations",
            deliverable="summary",
            time_scope={
                "kind": "relative_year_month",
                "preset": None,
                "anchor": {"relative_year": -1, "month": 2},
                "start": "2025-02-01",
                "end": "2025-02-28",
            },
            focus_entities=["厦门高林居住区高林一里 A1-1地块商业中心"],
            focus_metrics=["total_revenue"],
        ),
        "expected": {"focus_entities": ["厦门高林居住区高林一里 A1-1地块商业中心"]},
    },
    {
        "name": "llm_query_profile_preserved",
        "question": "把收费流水和通行记录联起来看",
        "plan": _build_plan(
            business_goal="risk_detection",
            analysis_job="anomaly_focus",
            decision_scope="operations",
            deliverable=None,
            time_scope={"preset": "last_7_days", "start": "2026-03-03", "end": "2026-03-09"},
            query_profile="payment_passage_reconciliation_by_date",
        ),
        "expected": {"query_profile": "payment_passage_reconciliation_by_date"},
    },
    {
        "name": "llm_sub_questions_preserved",
        "question": "去年 2 月高林营收最差是哪天，为什么",
        "plan": _build_plan(
            business_goal="management_reporting",
            analysis_job="period_assessment",
            decision_scope="operations",
            deliverable="summary",
            time_scope={
                "kind": "relative_year_month",
                "preset": None,
                "anchor": {"relative_year": -1, "month": 2},
                "start": "2025-02-01",
                "end": "2025-02-28",
            },
            focus_entities=["厦门高林居住区高林一里 A1-1地块商业中心"],
            sub_questions=[
                {"kind": "worst_day", "query_profile": "parking_daily_overview_join"},
                {"kind": "reason_explanation", "query_profile": "parking_daily_overview_join"},
            ],
        ),
        "expected": {"semantic_plan.sub_questions": 2},
    },
    {
        "name": "llm_specific_month_management_report",
        "question": "2025 年 2 月停车经营情况",
        "plan": _build_plan(
            business_goal="management_reporting",
            analysis_job="operational_overview",
            decision_scope="executive",
            deliverable="summary",
            time_scope={
                "kind": "specific_month",
                "preset": None,
                "anchor": {"year": 2025, "month": 2},
                "start": "2025-02-01",
                "end": "2025-02-28",
            },
        ),
        "expected": {"intent": "parking_management_report", "time_range.start": "2025-02-01", "time_range.end": "2025-02-28"},
    },
    {
        "name": "llm_relative_year_month_period_assessment",
        "question": "去年 3 月停车经营判断",
        "plan": _build_plan(
            business_goal="management_reporting",
            analysis_job="period_assessment",
            decision_scope="operations",
            deliverable="summary",
            time_scope={
                "kind": "relative_year_month",
                "preset": None,
                "anchor": {"relative_year": -1, "month": 3},
                "start": "2025-03-01",
                "end": "2025-03-31",
            },
        ),
        "expected": {"intent": "parking_period_assessment", "time_range.start": "2025-03-01"},
    },
    {
        "name": "llm_invalid_plan_falls_back",
        "question": "给老板看下今天经营情况",
        "plan": {"domain": "parking_ops", "business_goal": "unknown_goal"},
        "expected": {"intent": "parking_management_daily_report", "planner_mode": "rule_fallback"},
    },
    {
        "name": "llm_complex_highlin_alias",
        "question": "去年 2 月高林的营收情况，哪天最差，为什么，哪天是有人没交钱跑了吗",
        "plan": _build_plan(
            business_goal="management_reporting",
            analysis_job="period_assessment",
            decision_scope="operations",
            deliverable="summary",
            time_scope={
                "kind": "relative_year_month",
                "preset": None,
                "anchor": {"relative_year": -1, "month": 2},
                "start": "2025-02-01",
                "end": "2025-02-28",
            },
            focus_entities=["厦门高林居住区高林一里 A1-1地块商业中心"],
            query_profile="parking_daily_overview_join",
            sub_questions=[
                {"kind": "worst_day", "query_profile": "parking_daily_overview_join"},
                {"kind": "suspected_fare_evasion", "query_profile": "payment_passage_reconciliation_by_date"},
            ],
        ),
        "expected": {"intent": "parking_period_assessment", "planner_mode": "llm"},
        "contains": {"focus_entities": ["厦门高林居住区高林一里 A1-1地块商业中心"]},
    },
]


RUNTIME_CASES = [
    {
        "name": "runtime_anomaly_rule",
        "question": "哪个场子这周最不正常",
        "expected_handle": True,
        "expected_skill": "parking_anomaly_skill",
    },
    {
        "name": "runtime_daily_rule",
        "question": "给老板看下今天经营情况",
        "expected_handle": True,
        "expected_skill": "parking_management_report_skill",
    },
    {
        "name": "runtime_weekly_rule",
        "question": "生成最近7天停车经营周报，给管理层看",
        "expected_handle": True,
        "expected_skill": "parking_management_report_skill",
    },
    {
        "name": "runtime_period_rule",
        "question": "去年 2 月，停车情况是好转还是变坏，为什么",
        "expected_handle": True,
        "expected_skill": "parking_period_assessment_skill",
    },
    {
        "name": "runtime_relational_date_rule",
        "question": "把收费流水和通行记录联起来看，找出有收入但没通行的日期",
        "expected_handle": True,
        "expected_skill": "parking_relational_query_skill",
    },
    {
        "name": "runtime_relational_plate_rule",
        "question": "按车牌把支付和通行打通，看哪些车长时间停留但收费偏低",
        "expected_handle": True,
        "expected_skill": "parking_relational_query_skill",
    },
    {
        "name": "runtime_payment_method_rule",
        "question": "看下各车场支付方式分布",
        "expected_handle": True,
        "expected_skill": "parking_relational_query_skill",
    },
    {
        "name": "runtime_sales_trend_false",
        "question": "最近30天华东和华南的成交额趋势",
        "expected_handle": False,
    },
    {
        "name": "runtime_sales_compare_false",
        "question": "上个月各区域销售情况对比",
        "expected_handle": False,
    },
    {
        "name": "runtime_complex_highlin_with_planner",
        "question": "高林去年 2 月哪天收入最差，为什么",
        "expected_handle": True,
        "expected_skill": "parking_period_assessment_skill",
        "planner": _planner(
            _build_plan(
                business_goal="management_reporting",
                analysis_job="period_assessment",
                decision_scope="operations",
                deliverable="summary",
                time_scope={
                    "kind": "relative_year_month",
                    "preset": None,
                    "anchor": {"relative_year": -1, "month": 2},
                    "start": "2025-02-01",
                    "end": "2025-02-28",
                },
                focus_entities=["厦门高林居住区高林一里 A1-1地块商业中心"],
                sub_questions=[{"kind": "worst_day", "query_profile": "parking_daily_overview_join"}],
            )
        ),
    },
]


class RuleQuestionMatrixTests(unittest.TestCase):
    def _assert_normalize_case(self, case: dict) -> None:
        result = sql_generator.normalize_question(
            case["question"],
            schema_text=SCHEMA_TEXT,
            glossary_text=GLOSSARY_TEXT,
        )
        for path, expected in case.get("expected", {}).items():
            actual = _get_value(result, path) if "." in path else result.get(path)
            self.assertEqual(actual, expected, f"{case['name']} failed at {path}")
        for path, expected_values in case.get("contains", {}).items():
            actual = _get_value(result, path) if "." in path else result.get(path)
            if isinstance(actual, list):
                actual_text = " ".join(str(item) for item in actual)
                for expected in expected_values:
                    self.assertIn(expected, actual_text, f"{case['name']} missing {expected} in {path}")
            else:
                actual_text = str(actual)
                for expected in expected_values:
                    self.assertIn(expected, actual_text, f"{case['name']} missing {expected} in {path}")


class PlannerQuestionMatrixTests(unittest.TestCase):
    def _assert_planner_case(self, case: dict) -> None:
        result = sql_generator.normalize_question(
            case["question"],
            schema_text=SCHEMA_TEXT,
            glossary_text=GLOSSARY_TEXT,
            planner=_planner(case["plan"]),
        )
        for path, expected in case.get("expected", {}).items():
            if path == "semantic_plan.sub_questions":
                self.assertEqual(len(result["semantic_plan"]["sub_questions"]), expected, case["name"])
                continue
            actual = _get_value(result, path) if "." in path else result.get(path)
            self.assertEqual(actual, expected, f"{case['name']} failed at {path}")
        for path, expected_values in case.get("contains", {}).items():
            actual = _get_value(result, path) if "." in path else result.get(path)
            actual_text = " ".join(str(item) for item in actual) if isinstance(actual, list) else str(actual)
            for expected in expected_values:
                self.assertIn(expected, actual_text, f"{case['name']} missing {expected} in {path}")


class RuntimeQuestionMatrixTests(unittest.TestCase):
    def _assert_runtime_case(self, case: dict) -> None:
        planner = case.get("planner")
        handle = should_handle_with_runtime(
            case["question"],
            SCHEMA_PATH,
            GLOSSARY_PATH,
            planner=planner,
        )
        self.assertEqual(handle, case["expected_handle"], case["name"])
        if not handle:
            return
        task = detect_parking_task(
            case["question"],
            SCHEMA_PATH,
            GLOSSARY_PATH,
            planner=planner,
        )
        self.assertEqual(select_skill_name(task), case["expected_skill"], case["name"])


def _install_rule_cases() -> None:
    for index, case in enumerate(RULE_NORMALIZE_CASES, start=1):
        def _test(self, case=case):
            self._assert_normalize_case(case)

        setattr(
            RuleQuestionMatrixTests,
            f"test_rule_case_{index:02d}_{case['name']}",
            _test,
        )


def _install_planner_cases() -> None:
    offset = len(RULE_NORMALIZE_CASES)
    for local_index, case in enumerate(PLANNER_CASES, start=1):
        def _test(self, case=case):
            self._assert_planner_case(case)

        setattr(
            PlannerQuestionMatrixTests,
            f"test_planner_case_{offset + local_index:02d}_{case['name']}",
            _test,
        )


def _install_runtime_cases() -> None:
    offset = len(RULE_NORMALIZE_CASES) + len(PLANNER_CASES)
    for local_index, case in enumerate(RUNTIME_CASES, start=1):
        def _test(self, case=case):
            self._assert_runtime_case(case)

        setattr(
            RuntimeQuestionMatrixTests,
            f"test_runtime_case_{offset + local_index:02d}_{case['name']}",
            _test,
        )


_install_rule_cases()
_install_planner_cases()
_install_runtime_cases()


if __name__ == "__main__":
    unittest.main()
