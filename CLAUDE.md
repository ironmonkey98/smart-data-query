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
  → sql_generator.py: normalize_question()   # NL → semantic_plan → compatible task JSON
  → connect_db.py: load_dataset()            # CSV or MySQL → list[dict]
  → parking_analyst.py                       # semantic-first parking analysis dispatch
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
| `scripts/sql_generator.py` | NL → semantic_plan → compatible task JSON |
| `scripts/connect_db.py` | Unified data loader (CSV/MySQL) + generic query executor |
| `scripts/parking_analyst.py` | Parking semantic-first diagnostics (revenue, anomaly, flow, daily/weekly report) |
| `scripts/llm_enhancer.py` | Narrative generation — OpenAI-compatible API or rule engine |
| `scripts/chart_render.py` | SVG line chart renderer (V1 only supports line charts) |
| `references/db-schema.md` | Field definitions — required for query normalization |
| `references/term-glossary.md` | Business term aliases (e.g., 成交额 → paid_amount) |

### Semantic Plan + Task Schema

`sql_generator.py` now works in two steps for parking questions:

1. Build a first-principles `semantic_plan`
2. Map that plan to a backward-compatible task for the executor

`semantic_plan` (parking domain only):

```json
{
  "domain": "parking_ops",
  "business_goal": "management_reporting|risk_detection|efficiency_diagnosis|revenue_diagnosis",
  "analysis_job": "operational_overview|anomaly_focus|flow_or_occupancy|revenue_focus",
  "decision_scope": "executive|operations",
  "deliverable": "web_report|daily_brief|null",
  "time_scope": {"preset": "last_7_days", "start": "...", "end": "..."},
  "focus_entities": ["A停车场"],
  "focus_dimensions": ["parking_lot"],
  "focus_metrics": ["total_revenue", "occupancy_rate"],
  "implicit_requirements": ["summary_first"],
  "missing_information": []
}
```

Backward-compatible task contract:

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
  "semantic_plan": {"business_goal": "management_reporting"},
  "needs_clarification": false,
  "clarifying_question": null
}
```

Parking tasks still bypass the generic executor and go directly to `parking_analyst.py`.
The executor now prefers `semantic_plan` and only falls back to `intent` when semantic routing is missing or incomplete.

### Output Contract

- **`summary.json`** — full structured result (task + result rows + summary + artifact paths)
- **`chart.svg`** — line chart (only chart type supported in V1)
- **`session.json`** — conversation context for multi-turn follow-ups

When `needs_clarification: true`, the tool returns early with a clarifying question instead of executing.

## V1 Boundaries (Known Limitations)

- Chart rendering: **line charts only** — bar/pie/heatmap are future extensions
- NL parsing: parking domain uses semantic planning + mapping + rule fallback; other domains still use lighter rule parsing
- Schema discovery: manual — `db-schema.md` and `term-glossary.md` must be maintained by hand
- Excel data source: not yet implemented (planned extension per SKILL.md)

## Reference Docs

- `SKILL.md` — product spec: when/how the tool should be used, output contracts, NL optimization strategy
- `references/nl-optimization-notes.md` — intermediate representation design and follow-up question strategy
