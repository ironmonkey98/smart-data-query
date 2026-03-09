from __future__ import annotations

import asyncio
import json
import unittest
from pathlib import Path
from unittest.mock import patch

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


class ReflectStreamTests(unittest.TestCase):
    def test_chat_stream_emits_reflect_event(self) -> None:
        class FakeEvent:
            def __init__(self, event_type: str, **kwargs):
                self.type = event_type
                for key, value in kwargs.items():
                    setattr(self, key, value)

        class FakeContentBlock:
            def __init__(self, block_type: str, **kwargs):
                self.type = block_type
                for key, value in kwargs.items():
                    setattr(self, key, value)

        class FakeDelta:
            def __init__(self, delta_type: str, **kwargs):
                self.type = delta_type
                for key, value in kwargs.items():
                    setattr(self, key, value)

        class FakeStream:
            def __init__(self, events, final_message):
                self._events = events
                self._final_message = final_message

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def __iter__(self):
                return iter(self._events)

            def get_final_message(self):
                return self._final_message

        class FakeTextBlock:
            type = "text"
            text = "已完成分析。"

        class FakeToolUseBlock:
            type = "tool_use"

            def __init__(self):
                self.id = "tool_reflect_1"
                self.name = "reflect"
                self.input = {"plan": "1. 查看停车收入\n2. 排查异常车场"}

        class FakeFinalMessage:
            def __init__(self, stop_reason, content):
                self.stop_reason = stop_reason
                self.content = content

        class FakeMessages:
            def __init__(self):
                self.calls = 0

            def stream(self, **_kwargs):
                self.calls += 1
                if self.calls == 1:
                    return FakeStream(
                        [
                            FakeEvent(
                                "content_block_start",
                                content_block=FakeContentBlock("tool_use", id="tool_reflect_1", name="reflect"),
                            ),
                            FakeEvent(
                                "content_block_delta",
                                delta=FakeDelta(
                                    "input_json_delta",
                                    partial_json="{\"plan\": \"1. 查看停车收入\\n2. 排查异常车场\"}",
                                ),
                            ),
                            FakeEvent("content_block_stop"),
                            FakeEvent("message_stop"),
                        ],
                        FakeFinalMessage("tool_use", [FakeToolUseBlock()]),
                    )
                return FakeStream(
                    [
                        FakeEvent(
                            "content_block_start",
                            content_block=FakeContentBlock("text"),
                        ),
                        FakeEvent(
                            "content_block_delta",
                            delta=FakeDelta("text_delta", text="已完成分析。"),
                        ),
                        FakeEvent("message_stop"),
                    ],
                    FakeFinalMessage("end_turn", [FakeTextBlock()]),
                )

        class FakeAnthropicClient:
            def __init__(self, **_kwargs):
                self.messages = FakeMessages()

        async def collect_events() -> list[dict]:
            events = []
            async for chunk in server._chat_stream("reflect-chat-test", "帮我做个最近7天停车经营综合分析计划"):
                for line in chunk.splitlines():
                    if line.startswith("data: "):
                        events.append(json.loads(line[6:]))
            return events

        with patch.dict(server.os.environ, {"ANTHROPIC_API_KEY": "test-key"}, clear=False):
            with patch.object(server.anthropic, "Anthropic", FakeAnthropicClient):
                events = asyncio.run(collect_events())

        reflect_event = next(event for event in events if event["type"] == "reflect")
        self.assertIn("查看停车收入", reflect_event["plan"])
        tool_result = next(event for event in events if event["type"] == "tool_result")
        self.assertEqual(tool_result["summary"], [])


if __name__ == "__main__":
    unittest.main()
