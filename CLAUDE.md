# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**smart-data-query** is a Python CLI tool + FastAPI web service that converts natural language business questions into structured data queries, analyzes results, and generates charts + summaries. Specializes in parking lot operations analytics and general sales data querying.

No build system — pure Python 3.9+ project.

## Commands

### Web Server (primary interface)

```bash
# Start the web server (recommended — handles deps, port, data auto-generation)
./start.sh

# Or directly
python3 server.py          # runs on http://localhost:8000
```

**Environment** (`.env` or shell export):
```
ANTHROPIC_AUTH_TOKEN=sk-...     # or ANTHROPIC_API_KEY
ANTHROPIC_BASE_URL=...          # optional, for compatible APIs
ANTHROPIC_MODEL=claude-opus-4-6 # optional override
```

### CLI tool

```bash
# CSV
python3 main.py --source-type csv --source data/sample_sales.csv \
  --schema references/db-schema.md --glossary references/term-glossary.md \
  --question "对比最近30天华东和华南的成交额趋势" --output-dir run-output

# SQLite (primary parking data source)
python3 main.py --source-type sqlite --source data/sample_parking_ops.db \
  --schema references/db-schema.md --glossary references/term-glossary.md \
  --question "最近7天哪个车场收入下滑最明显" --output-dir run-output

# Multi-turn follow-up
python3 main.py --follow-up-question "为什么是B停车场？" \
  --session-file run-output/session.json --output-dir run-output
```

### Tests

```bash
# Run all tests
python3 -m unittest tests/test_codex_upgrade.py tests/test_real_parking_ingestion.py tests/test_report_page.py -v

# Run a single test file
python3 -m unittest tests/test_codex_upgrade.py -v
```

### Rebuild parking data from real Excel files

```bash
python3 scripts/build_parking_ops_from_excels.py
# Outputs: data/sample_parking_ops.db + data/sample_parking_ops.csv
```

## Architecture

### Two modes of operation

1. **CLI** (`main.py` → `scripts/smart_query.py`) — batch, file-in/file-out
2. **Web** (`server.py`) — FastAPI + SSE streaming, Claude Tool Use loop

### CLI data flow

```
User question
  → sql_generator.py: normalize_question()   # NL → semantic_plan → task JSON
  → connect_db.py: load_dataset()            # CSV / MySQL / SQLite → list[dict]
  → parking_analyst.py                       # parking intents: 4 analysis modes
  → connect_db.py: execute_structured_task() # other intents: generic executor
  → llm_enhancer.py: enhance_analysis()      # optional LLM or rule-based narrative
  → chart_render.py: render_svg_chart()      # SVG line chart
  → smart_query.py: write summary.json + chart.svg + session.json
```

### Web server architecture (`server.py`)

FastAPI serves `static/index.html` + a single POST endpoint `/api/chat` that streams SSE events. Internally runs a **Tool Use loop** (max 5 rounds):

```
Claude API (streaming)
  → if stop_reason == "tool_use":
      list_data_sources → returns DATA_SOURCES descriptions (no IO)
      query_data        → asyncio.to_thread(_handle_query_data) → run_query()
      reflect           → returns plan text, emits SSE {type: "reflect"} [CODEX_UPGRADE]
      compare_periods   → two run_query calls, merges delta/pct_change [CODEX_UPGRADE]
      save_insight      → appends to memory/insights.jsonl [CODEX_UPGRADE]
  → tool results appended to session.messages → next Claude round
```

Sessions are in-memory (`sessions: dict[str, SessionData]`), TTL 2h, cleaned hourly.

### Key files

| File | Role |
|------|------|
| `main.py` | CLI entry point, argument parsing |
| `server.py` | FastAPI app + SSE streaming + Tool Use loop |
| `static/index.html` | Single-page chat UI (all CSS/JS inline) |
| `scripts/smart_query.py` | CLI orchestrator — do not change `run_query()` signature |
| `scripts/sql_generator.py` | NL → task JSON; parking domain uses `semantic_plan` first |
| `scripts/connect_db.py` | Unified data loader (CSV/MySQL/SQLite) + generic executor |
| `scripts/parking_analyst.py` | 4 parking analysis modes: revenue, anomaly, flow, report |
| `scripts/llm_enhancer.py` | Narrative generation (OpenAI-compatible or rule fallback) |
| `scripts/chart_render.py` | SVG line chart renderer |
| `references/db-schema.md` | Field definitions — must be kept in sync with actual data |
| `references/term-glossary.md` | Business term aliases (e.g., 成交额 → paid_amount) |
| `CODEX_UPGRADE.md` | Pending upgrade instructions for Codex (Tool 3-5 + time range expansion) |

### Task JSON contract

Central data contract between `sql_generator` and all downstream modules:

```json
{
  "intent": "trend_compare|compare|summary|parking_revenue_analysis|parking_anomaly_diagnosis|parking_flow_efficiency_analysis|parking_management_report",
  "metric": {"field": "paid_amount", "label": "成交额", "aggregation": "sum"},
  "time_range": {"preset": "last_7_days", "start": "2025-01-01", "end": "2025-01-07"},
  "filters": [{"field": "region", "operator": "in", "values": ["华东"]}],
  "needs_clarification": false,
  "clarifying_question": null,
  "semantic_plan": { ... }   // parking domain only
}
```

**Parking domain only**: `sql_generator` first builds a `semantic_plan` (business_goal / analysis_job / focus_metrics), maps it to the task JSON above, falling back to minimal rule parsing on failure. The executor prefers `semantic_plan` and treats `intent` as compatibility fallback.

Parking intents always bypass the generic executor and go to `parking_analyst.py`.

### Data sources

| Source | File | Notes |
|--------|------|-------|
| Sales CSV | `data/sample_sales.csv` | Generated by `data/gen_mock_data.py` if < 100 rows |
| Parking SQLite | `data/sample_parking_ops.db` | Primary parking source; rebuilt from Excel via `build_parking_ops_from_excels.py` |
| Parking CSV | `data/sample_parking_ops.csv` | Derived daily-level fixture from SQLite; used as fallback |

The `.db` has 3 tables: `parking_lots`, `parking_payment_records`, `parking_passage_records`.
`sample_parking_ops.csv` is derived via multi-table join (see README for field derivation logic).

### Output contract

- `summary.json` — full structured result (task + rows + summary + artifact paths)
- `chart.svg` — line chart (only chart type in V1)
- `session.json` — conversation context for multi-turn follow-ups
- `memory/insights.jsonl` — persisted insights from `save_insight` tool (auto-created)

When `needs_clarification: true`, tool returns early with `clarifying_question` instead of executing.

### Relative time handling

When system date is later than sample data, the executor auto-aligns "今天/最近7天/本周" to the latest available date in the dataset so reports can still be generated.

## Pending upgrade (CODEX_UPGRADE.md)

`CODEX_UPGRADE.md` contains complete implementation instructions for:
- **3 new tools** in `server.py`: `reflect` (analysis planning), `compare_periods` (period-over-period diff), `save_insight` (persist findings to `memory/insights.jsonl`)
- **Extended time range parsing** in `sql_generator._detect_time_range()`: 本周/上周/本月/上月/最近3天/最近14天/今年
- **Memory injection** into SYSTEM_PROMPT: loads last 10 entries from `memory/insights.jsonl` at conversation start

## V1 Boundaries

- Chart rendering: **line charts only**
- `compare_periods` tool currently stable for `sales` only
- NL parsing: parking uses semantic planning; sales/other domains use lighter rule matching
- Excel data source: not implemented
- Schema discovery: manual (`db-schema.md` and `term-glossary.md` must be hand-maintained)
