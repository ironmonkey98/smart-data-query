#!/usr/bin/env python3
"""
10 轮接口 + 数据健康测试
覆盖：服务可用性、数据源接口、chat SSE 流、SQL 直连数据
"""
import json
import sqlite3
import time
import traceback
import uuid
from pathlib import Path

import httpx

BASE_URL = "http://localhost:8000"
DB_PATH  = Path(__file__).parent.parent / "data" / "sample_parking_ops.db"

# ─── 测试用例定义 ──────────────────────────────────────────────────────────────
CASES = [
    # (编号, 描述, 函数名)
    (1,  "服务健康检查 GET /",               "test_health"),
    (2,  "数据源列表 GET /api/data-sources",  "test_data_sources"),
    (3,  "数据库直连 — 停车场数量",            "test_db_parking_lots"),
    (4,  "数据库直连 — 支付记录总数",          "test_db_payment_count"),
    (5,  "数据库直连 — 通道记录总数",          "test_db_passage_count"),
    (6,  "数据库直连 — 日期范围校验",          "test_db_date_range"),
    (7,  "数据库直连 — 支付成功率",            "test_db_payment_success_rate"),
    (8,  "Chat SSE — 简单问候（不触发 SQL）",  "test_chat_greeting"),
    (9,  "Chat SSE — 查询停车场列表",          "test_chat_list_lots"),
    (10, "Chat SSE — 查询最近收入汇总",        "test_chat_revenue"),
]

results: list[dict] = []
client = httpx.Client(timeout=60)


# ─── 工具函数 ─────────────────────────────────────────────────────────────────

def _db():
    return sqlite3.connect(str(DB_PATH))


def _chat_sse(message: str) -> list[dict]:
    """发送 chat 请求，收集所有 SSE 事件，返回事件列表。"""
    session_id = str(uuid.uuid4())
    events = []
    with client.stream(
        "POST",
        f"{BASE_URL}/api/chat",
        json={"session_id": session_id, "message": message},
        headers={"Accept": "text/event-stream"},
    ) as resp:
        resp.raise_for_status()
        for line in resp.iter_lines():
            if line.startswith("data: "):
                try:
                    ev = json.loads(line[6:])
                    events.append(ev)
                    if ev.get("type") == "done":
                        break
                except json.JSONDecodeError:
                    pass
    return events


def _record(no: int, desc: str, passed: bool, detail: str = ""):
    mark = "✅" if passed else "❌"
    print(f"  {mark} [{no:02d}] {desc}")
    if detail:
        # 每行缩进
        for ln in detail.splitlines():
            print(f"       {ln}")
    results.append({"no": no, "desc": desc, "passed": passed, "detail": detail})


# ─── 各测试用例 ───────────────────────────────────────────────────────────────

def test_health(no, desc):
    resp = client.get(f"{BASE_URL}/")
    assert resp.status_code == 200, f"HTTP {resp.status_code}"
    _record(no, desc, True, f"HTTP 200, content-type: {resp.headers.get('content-type','')}")


def test_data_sources(no, desc):
    resp = client.get(f"{BASE_URL}/api/data-sources")
    assert resp.status_code == 200
    data = resp.json()
    assert "parking_ops" in data, f"缺少 parking_ops，返回: {list(data.keys())}"
    _record(no, desc, True, f"数据源: {list(data.keys())}")


def test_db_parking_lots(no, desc):
    with _db() as conn:
        rows = conn.execute("SELECT lot_id, parking_lot_name, total_spaces FROM parking_lots").fetchall()
    assert len(rows) > 0, "停车场表为空"
    detail = "\n".join(f"  lot_id={r[0]}, name={r[1]}, spaces={r[2]}" for r in rows)
    _record(no, desc, True, f"共 {len(rows)} 个停车场:\n{detail}")


def test_db_payment_count(no, desc):
    with _db() as conn:
        count = conn.execute("SELECT COUNT(*) FROM parking_payment_records").fetchone()[0]
    assert count > 0, "支付记录表为空"
    _record(no, desc, True, f"支付记录总数: {count:,}")


def test_db_passage_count(no, desc):
    with _db() as conn:
        count = conn.execute("SELECT COUNT(*) FROM parking_passage_records").fetchone()[0]
    assert count > 0, "通道记录表为空"
    _record(no, desc, True, f"通道记录总数: {count:,}")


def test_db_date_range(no, desc):
    with _db() as conn:
        row = conn.execute(
            "SELECT date(MIN(paid_at)), date(MAX(paid_at)) FROM parking_payment_records"
        ).fetchone()
    min_d, max_d = row
    assert min_d and max_d, "无法获取日期范围"
    _record(no, desc, True, f"支付数据日期范围: {min_d} ~ {max_d}")


def test_db_payment_success_rate(no, desc):
    with _db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM parking_payment_records").fetchone()[0]
        success = conn.execute(
            "SELECT COUNT(*) FROM parking_payment_records WHERE payment_result = '支付成功'"
        ).fetchone()[0]
    rate = success / total * 100 if total else 0
    _record(no, desc, True, f"支付成功: {success:,} / {total:,} = {rate:.1f}%")


def test_chat_greeting(no, desc):
    events = _chat_sse("你好，你是谁？")
    types = [e.get("type") for e in events]
    has_text = "text_delta" in types
    has_done = "done" in types
    assert has_done, f"未收到 done 事件，收到: {types}"
    text_parts = [e.get("text", "") for e in events if e.get("type") == "text_delta"]
    reply = "".join(text_parts)[:100]
    _record(no, desc, True, f"SSE 事件类型: {set(types)}\n回复摘要: {reply}…")


def test_chat_list_lots(no, desc):
    events = _chat_sse("列出所有停车场的名称和车位数")
    types  = [e.get("type") for e in events]
    assert "done" in types, f"未收到 done 事件，收到: {types}"
    has_sql = "tool_use" in types
    text_parts = [e.get("text", "") for e in events if e.get("type") == "text_delta"]
    reply = "".join(text_parts)[:150]
    _record(no, desc, True,
            f"触发 SQL 工具: {has_sql}\nSSE 类型: {set(types)}\n回复摘要: {reply}…")


def test_chat_revenue(no, desc):
    events = _chat_sse("最新一个月的停车总收入是多少？")
    types  = [e.get("type") for e in events]
    assert "done" in types, f"未收到 done 事件，收到: {types}"
    text_parts = [e.get("text", "") for e in events if e.get("type") == "text_delta"]
    reply = "".join(text_parts)[:200]
    _record(no, desc, True,
            f"SSE 类型: {set(types)}\n回复摘要: {reply}…")


# ─── 主入口 ───────────────────────────────────────────────────────────────────

FUNC_MAP = {
    "test_health":               test_health,
    "test_data_sources":         test_data_sources,
    "test_db_parking_lots":      test_db_parking_lots,
    "test_db_payment_count":     test_db_payment_count,
    "test_db_passage_count":     test_db_passage_count,
    "test_db_date_range":        test_db_date_range,
    "test_db_payment_success_rate": test_db_payment_success_rate,
    "test_chat_greeting":        test_chat_greeting,
    "test_chat_list_lots":       test_chat_list_lots,
    "test_chat_revenue":         test_chat_revenue,
}


def main():
    print("=" * 60)
    print("  Smart Data Query — 10 轮接口 + 数据健康测试")
    print(f"  服务地址: {BASE_URL}")
    print(f"  数据库  : {DB_PATH}")
    print("=" * 60)

    total_start = time.time()

    for no, desc, func_name in CASES:
        fn = FUNC_MAP[func_name]
        t0 = time.time()
        try:
            fn(no, desc)
        except Exception as e:
            elapsed = time.time() - t0
            detail = f"耗时: {elapsed:.2f}s\n{traceback.format_exc()}"
            _record(no, desc, False, detail)
        else:
            elapsed = time.time() - t0
            # 在最后一条 detail 行追加耗时
            results[-1]["detail"] += f"\n耗时: {elapsed:.2f}s"

    # ─── 汇总 ────────────────────────────────────────────────────────────────
    passed = sum(1 for r in results if r["passed"])
    failed = len(results) - passed
    total_elapsed = time.time() - total_start

    print()
    print("=" * 60)
    print(f"  结果汇总: {passed} 通过 / {failed} 失败  （共 {len(results)} 项）")
    print(f"  总耗时  : {total_elapsed:.1f}s")
    print("=" * 60)

    if failed:
        print("\n失败项明细:")
        for r in results:
            if not r["passed"]:
                print(f"  ❌ [{r['no']:02d}] {r['desc']}")
                for ln in r["detail"].splitlines():
                    print(f"       {ln}")


if __name__ == "__main__":
    main()
