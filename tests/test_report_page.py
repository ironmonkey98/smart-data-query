from __future__ import annotations

import asyncio
import json
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

import server


class ManagementReportTests(unittest.TestCase):
    def test_query_data_returns_report_link_for_management_report(self) -> None:
        result = server._handle_query_data(
            {
                "source_name": "parking_ops",
                "question": "生成最近7天停车经营周报，给管理层看",
            },
            "report-page-test",
        )

        self.assertIn("report_id", result)
        self.assertIn("report_url", result)
        self.assertTrue(result["report_url"].startswith("/report/"))

    def test_report_api_returns_saved_management_report(self) -> None:
        result = server._handle_query_data(
            {
                "source_name": "parking_ops",
                "question": "生成最近7天停车经营周报，给管理层看",
            },
            "report-api-test",
        )

        client = TestClient(server.app)
        response = client.get(f"/api/report/{result['report_id']}")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["analysis_type"], "management_report")
        self.assertIn("overview", payload)
        self.assertIn("focus_lots", payload)
        self.assertIn("priority_actions", payload)

    def test_chat_stream_bypasses_llm_for_management_report_request(self) -> None:
        async def collect_events() -> list[dict]:
            events = []
            async for chunk in server._chat_stream(
                "report-direct-chat-test",
                "生成最近7天停车经营周报，给管理层看",
            ):
                for line in chunk.splitlines():
                    if line.startswith("data: "):
                        events.append(json.loads(line[6:]))
            return events

        events = asyncio.run(collect_events())

        self.assertEqual(events[0]["type"], "tool_use")
        self.assertEqual(events[0]["tool_name"], "query_data")
        tool_result = next(event for event in events if event["type"] == "tool_result")
        self.assertIn("report_url", tool_result)
        self.assertTrue(tool_result["report_url"].startswith("/report/"))
        self.assertFalse(any(event.get("tool_name") == "compare_periods" for event in events))

    def test_query_data_returns_daily_report_link_for_management_daily_report(self) -> None:
        result = server._handle_query_data(
            {
                "source_name": "parking_ops",
                "question": "给老板看下今天经营情况",
            },
            "daily-report-page-test",
        )

        self.assertIn("report_id", result)
        self.assertIn("report_url", result)
        self.assertTrue(result["report_url"].startswith("/report/"))

    def test_report_api_returns_saved_management_daily_report(self) -> None:
        result = server._handle_query_data(
            {
                "source_name": "parking_ops",
                "question": "做个停车经营日报给管理层",
            },
            "daily-report-api-test",
        )

        client = TestClient(server.app)
        response = client.get(f"/api/report/{result['report_id']}")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["analysis_type"], "management_report")
        self.assertEqual(payload["report_type"], "daily")
        self.assertIn("overview", payload)
        self.assertIn("focus_lots", payload)
        self.assertIn("priority_actions", payload)


class ReportPageMarkupTests(unittest.TestCase):
    def test_report_page_markup_exists_in_static_shell(self) -> None:
        html = Path("static/index.html").read_text(encoding="utf-8")

        self.assertIn("report-shell", html)
        self.assertIn("renderReportPage", html)
        self.assertIn("report-kpis", html)


if __name__ == "__main__":
    unittest.main()
