from __future__ import annotations

import csv
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from scripts.parking_analyst import diagnose_parking_operation
from smart_query import run_query


class RealParkingIngestionTests(unittest.TestCase):
    def test_normalize_parking_lot_name_maps_known_alias(self) -> None:
        from scripts.build_parking_ops_from_excels import normalize_parking_lot_name

        self.assertEqual(
            normalize_parking_lot_name("高林社区商业中心"),
            "厦门高林居住区高林一里 A1-1地块商业中心",
        )

    def test_build_daily_ops_rows_aggregates_payment_and_passage_metrics(self) -> None:
        from scripts.build_parking_ops_from_excels import build_daily_ops_rows

        payment_rows = [
            {
                "支付时间": "2025-07-01 08:30:00",
                "停车场名称": "测试车场",
                "实收(元)": 10,
                "收费结果": "缴费成功",
            },
            {
                "支付时间": "2025-07-01 09:30:00",
                "停车场名称": "测试车场",
                "实收(元)": 5,
                "收费结果": "支付成功,通知车场失败",
            },
            {
                "支付时间": "2025-07-01 10:30:00",
                "停车场名称": "测试车场",
                "实收(元)": 0,
                "收费结果": "退款",
            },
        ]
        passage_rows = [
            {
                "入场时间": "2025-07-01 08:00:00",
                "停车场名称": "测试车场",
                "车辆类型": "临时车",
                "停留时间（分）": 120,
                "应收（元）": 10,
                "实收（元）": 10,
            },
            {
                "入场时间": "2025-07-01 12:00:00",
                "停车场名称": "测试车场",
                "车辆类型": "临时车",
                "停留时间（分）": 60,
                "应收（元）": 0,
                "实收（元）": 0,
            },
        ]

        rows = build_daily_ops_rows(
            capacities={"测试车场": 10},
            payment_rows=payment_rows,
            passage_rows=passage_rows,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["stat_date"], "2025-07-01")
        self.assertEqual(rows[0]["parking_lot"], "测试车场")
        self.assertEqual(rows[0]["total_revenue"], 15.0)
        self.assertEqual(rows[0]["temp_revenue"], 15.0)
        self.assertEqual(rows[0]["monthly_revenue"], 0.0)
        self.assertEqual(rows[0]["entry_count"], 2)
        self.assertAlmostEqual(rows[0]["payment_failure_rate"], 0.3333, places=4)
        self.assertEqual(rows[0]["abnormal_open_count"], 0)
        self.assertEqual(rows[0]["free_release_count"], 1)
        self.assertAlmostEqual(rows[0]["occupancy_rate"], 0.0125, places=4)

    def test_build_sqlite_database_creates_three_tables_and_joinable_rows(self) -> None:
        from scripts.build_parking_ops_from_excels import build_sqlite_database
        from scripts.connect_db import load_dataset

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "parking_ops.db"
            build_sqlite_database(
                capacities={"测试车场": 10},
                payment_rows=[
                    {
                        "支付时间": "2025-07-01 08:30:00",
                        "发起支付时间": "2025-07-01 08:28:00",
                        "停车场名称": "测试车场",
                        "车牌号": "闽D12345",
                        "入场时间": "2025-07-01 08:00:00",
                        "应收(元)": 10,
                        "实收(元)": 10,
                        "收费结果": "缴费成功",
                        "支付方式": "微信支付",
                        "退款金额(元)": 0,
                        "缴费来源": "线上",
                        "是否开票": "否",
                    },
                    {
                        "支付时间": "2025-07-02 09:00:00",
                        "发起支付时间": "2025-07-02 08:58:00",
                        "停车场名称": "测试车场",
                        "车牌号": "闽D54321",
                        "入场时间": "2025-07-02 08:00:00",
                        "应收(元)": 20,
                        "实收(元)": 20,
                        "收费结果": "缴费成功",
                        "支付方式": "支付宝支付",
                        "退款金额(元)": 0,
                        "缴费来源": "线上",
                        "是否开票": "否",
                    }
                ],
                passage_rows=[
                    {
                        "停车场名称": "测试车场",
                        "车牌号": "闽D12345",
                        "车辆类型": "临时车",
                        "入场时间": "2025-07-01 08:00:00",
                        "入场门道": "东门",
                        "出场时间": "2025-07-01 10:00:00",
                        "出场门道": "西门",
                        "停留时间（分）": 120,
                        "应收（元）": 10,
                        "实收（元）": 10,
                        "备注": "",
                    },
                    {
                        "停车场名称": "测试车场",
                        "车牌号": "闽D54321",
                        "车辆类型": "临时车",
                        "入场时间": "2025-07-02 08:00:00",
                        "入场门道": "东门",
                        "出场时间": "2025-07-02 11:00:00",
                        "出场门道": "西门",
                        "停留时间（分）": 180,
                        "应收（元）": 20,
                        "实收（元）": 20,
                        "备注": "",
                    }
                ],
                output_path=db_path,
            )

            with closing(sqlite3.connect(db_path)) as conn:
                tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertTrue({"parking_lots", "parking_payment_records", "parking_passage_records"}.issubset(tables))

            rows = load_dataset(
                source=str(db_path),
                source_type="sqlite",
                task={
                    "intent": "parking_management_report",
                    "domain": "parking_ops",
                    "time_field": "stat_date",
                    "time_range": {"preset": "all", "start": None, "end": None},
                },
            )
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["parking_lot"], "测试车场")
            self.assertEqual(rows[0]["total_revenue"], 10.0)
            self.assertEqual(rows[0]["entry_count"], 1)

    def test_run_query_supports_sqlite_join_for_parking_reports(self) -> None:
        from scripts.build_parking_ops_from_excels import build_sqlite_database

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            db_path = base_path / "parking_ops.db"
            out_dir = base_path / "out"
            build_sqlite_database(
                capacities={"测试车场": 10},
                payment_rows=[
                    {
                        "支付时间": "2025-07-01 08:30:00",
                        "发起支付时间": "2025-07-01 08:28:00",
                        "停车场名称": "测试车场",
                        "车牌号": "闽D12345",
                        "入场时间": "2025-07-01 08:00:00",
                        "应收(元)": 10,
                        "实收(元)": 10,
                        "收费结果": "缴费成功",
                        "支付方式": "微信支付",
                        "退款金额(元)": 0,
                        "缴费来源": "线上",
                        "是否开票": "否",
                    },
                    {
                        "支付时间": "2025-07-02 09:00:00",
                        "发起支付时间": "2025-07-02 08:58:00",
                        "停车场名称": "测试车场",
                        "车牌号": "闽D54321",
                        "入场时间": "2025-07-02 08:00:00",
                        "应收(元)": 20,
                        "实收(元)": 20,
                        "收费结果": "缴费成功",
                        "支付方式": "支付宝支付",
                        "退款金额(元)": 0,
                        "缴费来源": "线上",
                        "是否开票": "否",
                    }
                ],
                passage_rows=[
                    {
                        "停车场名称": "测试车场",
                        "车牌号": "闽D12345",
                        "车辆类型": "临时车",
                        "入场时间": "2025-07-01 08:00:00",
                        "入场门道": "东门",
                        "出场时间": "2025-07-01 10:00:00",
                        "出场门道": "西门",
                        "停留时间（分）": 120,
                        "应收（元）": 10,
                        "实收（元）": 10,
                        "备注": "",
                    },
                    {
                        "停车场名称": "测试车场",
                        "车牌号": "闽D54321",
                        "车辆类型": "临时车",
                        "入场时间": "2025-07-02 08:00:00",
                        "入场门道": "东门",
                        "出场时间": "2025-07-02 11:00:00",
                        "出场门道": "西门",
                        "停留时间（分）": 180,
                        "应收（元）": 20,
                        "实收（元）": 20,
                        "备注": "",
                    }
                ],
                output_path=db_path,
            )
            payload = run_query(
                question="生成最近7天停车经营周报，给管理层看",
                source_type="sqlite",
                source=str(db_path),
                schema="references/db-schema.md",
                glossary="references/term-glossary.md",
                output_dir=str(out_dir),
            )

            self.assertIn("analysis", payload)
            self.assertEqual(payload["analysis"]["analysis_type"], "management_report")
            self.assertEqual(payload["analysis"]["report_type"], "weekly")

    def test_generated_csv_can_drive_management_report(self) -> None:
        rows = []
        with Path("data/sample_parking_ops.csv").open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                rows.append(
                    {
                        "stat_date": row["stat_date"],
                        "parking_lot": row["parking_lot"],
                        "total_revenue": float(row["total_revenue"]),
                        "temp_revenue": float(row["temp_revenue"]),
                        "monthly_revenue": float(row["monthly_revenue"]),
                        "entry_count": float(row["entry_count"]),
                        "payment_failure_rate": float(row["payment_failure_rate"]),
                        "abnormal_open_count": float(row["abnormal_open_count"]),
                        "free_release_count": float(row["free_release_count"]),
                        "occupancy_rate": float(row["occupancy_rate"]),
                    }
                )

        task = {
            "intent": "parking_management_report",
            "domain": "parking_ops",
            "entity_field": "parking_lot",
            "time_field": "stat_date",
            "time_range": {"preset": "real_data_slice", "start": "2025-07-01", "end": "2025-07-07"},
            "focus_metrics": ["total_revenue", "occupancy_rate"],
            "focus_entities": [],
            "semantic_plan": {
                "business_goal": "management_reporting",
                "analysis_job": "operational_overview",
                "decision_scope": "executive",
                "deliverable": "web_report",
                "focus_metrics": ["total_revenue", "occupancy_rate"],
                "focus_entities": [],
            },
        }

        analysis = diagnose_parking_operation(rows, task)

        self.assertEqual(analysis["analysis_type"], "management_report")
        self.assertEqual(analysis["report_type"], "weekly")
        self.assertGreater(len(analysis["focus_lots"]), 0)
        self.assertIn("overview", analysis)

    def test_run_query_supports_relational_capacity_ranking(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            payload = run_query(
                question="把停车场基础数据也带上，算单位车位收入排名",
                source_type="sqlite",
                source="data/sample_parking_ops.db",
                schema="references/db-schema.md",
                glossary="references/term-glossary.md",
                output_dir=tmpdir,
            )

        self.assertEqual(payload["task"]["query_profile"], "lot_capacity_efficiency_ranking")
        self.assertIn("单位车位收入最高", payload["summary"][0])
        self.assertGreater(payload["result"]["row_count"], 0)
        self.assertIn("revenue_per_space", payload["result"]["rows"][0])

    def test_run_query_relational_plate_join_can_request_clarification(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            payload = run_query(
                question="按车牌把支付和通行打通，看哪些车长时间停留但收费偏低",
                source_type="sqlite",
                source="data/sample_parking_ops.db",
                schema="references/db-schema.md",
                glossary="references/term-glossary.md",
                output_dir=tmpdir,
            )

        self.assertTrue(payload["needs_clarification"])
        self.assertIn("收费偏低", payload["clarifying_question"])

    def test_run_query_supports_reconciliation_by_date_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            payload = run_query(
                question="把收费流水和通行记录联起来看，找出有收入但没通行的日期",
                source_type="sqlite",
                source="data/sample_parking_ops.db",
                schema="references/db-schema.md",
                glossary="references/term-glossary.md",
                output_dir=tmpdir,
            )

        self.assertEqual(payload["task"]["query_profile"], "payment_passage_reconciliation_by_date")
        self.assertIn("summary", payload)
        self.assertIn("result", payload)


if __name__ == "__main__":
    unittest.main()
