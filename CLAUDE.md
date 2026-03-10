# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Start server (recommended — checks DB, handles deps, port)
./start.sh                  # macOS/Linux
run_windows.bat             # Windows

# Or directly
python3 server.py           # http://localhost:8000

# Rebuild SQLite from raw Excel files
python3 scripts/build_parking_ops_from_excels.py
```

**Environment** (`.env` or shell):
```
ANTHROPIC_AUTH_TOKEN=sk-ant-...
ANTHROPIC_BASE_URL=...        # optional proxy
ANTHROPIC_MODEL=claude-opus-4-6  # optional
```

## Architecture

Single-file web server (`server.py`) running a **Claude Tool Use loop** (max 8 rounds) over SSE. Claude writes SQL directly — there is no NL-to-SQL translation layer.

```
POST /api/chat
  └─→ _chat_stream()  [async SSE generator]
        └─→ Claude API (streaming, with 429 exponential backoff: 2s/4s/8s)
              ├─→ list_data_sources   → returns DATA_SOURCES descriptions (no IO)
              ├─→ execute_sql         → asyncio.to_thread(_handle_execute_sql)
              │     ├─ SQLite: execute_raw_sql_on_sqlite()  [scripts/connect_db.py]
              │     └─ CSV:    execute_raw_sql_on_csv()      [scripts/connect_db.py]
              ├─→ reflect             → emits SSE {type:"reflect"}, returns plan text
              └─→ save_insight        → appends to memory/insights.jsonl
```

Sessions are in-memory (`dict[str, SessionData]`), TTL 2h, cleaned hourly.

At startup, `references/db-schema.md` + `references/term-glossary.md` are read and injected into `SYSTEM_PROMPT` so Claude knows the full schema without calling `list_data_sources`.

`memory/insights.jsonl` last 10 entries are appended to `system=` on every conversation start (`_load_memory_context()`).

## Key Behaviours

**SQL safety**: strips `--` and `/* */` comments, then checks first keyword is `SELECT` or `WITH` (allows CTEs). Blocks all write operations.

**Clarification first**: `SYSTEM_PROMPT` instructs Claude to ask for time range / target / metric before running any tool when the question is ambiguous. Claude must not guess.

**reflect tool**: required before complex multi-step analysis (reports, multi-source, period comparison). Emits a `{type:"reflect", plan:"..."}` SSE event rendered as a collapsible block in the UI.

## SSE Event Types

| type | when |
|------|------|
| `text_delta` | Claude streaming text |
| `tool_use` | tool call started (with `label`) |
| `tool_result` | tool execution done (with `row_count`, `chart_svg`, `error`) |
| `reflect` | reflect tool fired (with `plan`) |
| `retrying` | 429 rate limit, retrying (with `wait`, `attempt`) |
| `error` | any error |
| `done` | stream finished |

## Data

`data/sample_parking_ops.db` — 3 tables:
- `parking_lots` — lot_id, parking_lot_name, total_spaces
- `parking_payment_records` — payment_id, lot_id, initiated_at, paid_at, license_plate, entry_at, receivable_amount, actual_amount, payment_result, payment_method, refund_amount, payment_source, invoice_flag
- `parking_passage_records` — passage_id, lot_id, license_plate, vehicle_type, entry_at, entry_gate, exit_at, exit_gate, stay_minutes, receivable_amount, actual_amount, notes

Common SQL patterns (from `SYSTEM_PROMPT`):
- Payment failure: `payment_result LIKE '%失败%' OR payment_result = '支付成功,通知车场失败'`
- Free release: `vehicle_type = '临时车' AND COALESCE(receivable_amount,0) = 0 AND COALESCE(actual_amount,0) = 0`
- Occupancy rate: `SUM(stay_minutes) / (total_spaces * 1440.0)`
- Time filter: `WHERE date(paid_at) >= '2025-01-01'`
