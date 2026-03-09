# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**smart-data-query** is a Python CLI tool that converts natural language business questions into structured data queries, analyzes results, and generates charts + summaries. It specializes in parking lot operations analytics and general sales data querying.

No build system is needed — this is a pure Python 3.9+ project.

## Running the Tool

```bash
# CSV data source
python3 main.py \
  --source-type csv \
  --source data/sample_sales.csv \
  --schema references/db-schema.md \
  --glossary references/term-glossary.md \
  --question "对比最近30天华东和华南的成交额趋势" \
  --output-dir run-output

# MySQL data source
python3 main.py \
  --source-type mysql \
  --source data/sample_mysql_config.json \
  --schema references/db-schema.md \
  --glossary references/term-glossary.md \
  --question "最近7天哪个车场收入下滑最明显" \
  --output-dir mysql-output

# With LLM enhancement
export OPENAI_API_KEY="your-key"
python3 main.py --enable-llm --question "..." [other args]

# Multi-turn follow-up (first turn must use --session-file)
python3 main.py --follow-up-question "为什么是B停车场？" --session-file session.json --output-dir output
```

Optional deps for MySQL: `pip install pymysql` or `pip install mysql-connector-python`

## Architecture

### Data Flow

```
User question
  → sql_generator.py: normalize_question()   # NL → task JSON (intent, metric, dimensions, filters, chart)
  → connect_db.py: load_dataset()            # CSV or MySQL → list[dict]
  → (parking intents) → parking_analyst.py  # 4 specialized analysis modes
  → (other intents)  → connect_db.py: execute_structured_task()
  → llm_enhancer.py: enhance_analysis()     # optional LLM or rule-based narrative
  → chart_render.py: render_svg_chart()     # SVG line chart output
  → smart_query.py: aggregate results → summary.json + chart.svg + session.json
```

### Key Files

| File | Role |
|------|------|
| `main.py` | CLI entry point, argument parsing, task routing |
| `scripts/smart_query.py` | Main orchestrator, coordinates all stages |
| `scripts/sql_generator.py` | NL → structured task JSON using keyword matching + regex |
| `scripts/connect_db.py` | Unified data loader (CSV/MySQL) + generic query executor |
| `scripts/parking_analyst.py` | Parking-specific diagnostics (revenue, anomaly, flow, weekly report) |
| `scripts/llm_enhancer.py` | Narrative generation — OpenAI-compatible API or rule engine |
| `scripts/chart_render.py` | SVG line chart renderer (V1 only supports line charts) |
| `references/db-schema.md` | Field definitions — required for query normalization |
| `references/term-glossary.md` | Business term aliases (e.g., 成交额 → paid_amount) |

### Normalized Task JSON Schema

The central data contract between `sql_generator` and all downstream modules:

```json
{
  "intent": "trend_compare|compare|summary|parking_revenue_analysis|parking_anomaly_diagnosis|parking_flow_efficiency_analysis|parking_management_report",
  "metric": {"field": "paid_amount", "label": "成交额", "aggregation": "sum"},
  "dimensions": ["region"],
  "time_field": "order_date",
  "time_granularity": "day",
  "time_range": {"preset": "last_30_days", "start": "...", "end": "..."},
  "filters": [{"field": "region", "operator": "in", "values": ["华东"]}],
  "chart": {"type": "line", "x_field": "order_date", "y_field": "成交额", "series_field": "region"},
  "assumptions": ["..."],
  "needs_clarification": false,
  "clarifying_question": null
}
```

Parking intents (`parking_*`) bypass the generic executor and go directly to `parking_analyst.py`.

### Output Contract

- **`summary.json`** — full structured result (task + result rows + summary + artifact paths)
- **`chart.svg`** — line chart (only chart type supported in V1)
- **`session.json`** — conversation context for multi-turn follow-ups

When `needs_clarification: true`, the tool returns early with a clarifying question instead of executing.

## V1 Boundaries (Known Limitations)

- Chart rendering: **line charts only** — bar/pie/heatmap are future extensions
- NL parsing: keyword matching + regex, not ML-based; complex multi-table reasoning is out of scope
- Schema discovery: manual — `db-schema.md` and `term-glossary.md` must be maintained by hand
- Excel data source: not yet implemented (planned extension per SKILL.md)

## Reference Docs

- `SKILL.md` — product spec: when/how the tool should be used, output contracts, NL optimization strategy
- `references/nl-optimization-notes.md` — intermediate representation design and follow-up question strategy
