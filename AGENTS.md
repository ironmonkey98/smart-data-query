# Repository Guidelines

## Project Structure & Module Organization
This repository is a small Python data-query application with both CLI and web entry points. `main.py` is the CLI launcher and delegates to modules in `scripts/`. `server.py` exposes the same query flow through FastAPI and serves `static/index.html`. Sample datasets and config live in `data/`, while business metadata and query-mapping references live in `references/`. Generated outputs such as `summary.json`, `chart.svg`, and session files should be written to a local output directory and not committed.

## Build, Test, and Development Commands
Create an isolated environment before installing dependencies.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-server.txt
python3 main.py --source-type csv --source data/sample_sales.csv --schema references/db-schema.md --glossary references/term-glossary.md --question "对比最近30天华东和华南成交额趋势" --output-dir run-output/demo
python3 server.py
python3 -m compileall "main.py" "server.py" "scripts"
```

Use `main.py` for end-to-end CLI checks, `server.py` for local web development, and `compileall` as the minimum syntax validation step.

## Coding Style & Naming Conventions
Use 4-space indentation, type hints where practical, and `snake_case` for modules, functions, and variables. Prefer the underscore module variants already used by imports, such as `scripts/connect_db.py` and `scripts/sql_generator.py`; avoid introducing new hyphenated module names. Keep functions focused, avoid speculative abstractions, and preserve existing Chinese comments/docstrings when adding new ones.

## Testing Guidelines
There is currently no committed `tests/` directory. New contributions should add focused regression tests under `tests/test_*.py`, preferably with `unittest` so they run with the standard library:

```bash
python3 -m unittest discover -s tests
```

Prioritize tests for question normalization, CSV/MySQL loading, parking analysis branches, and follow-up session behavior.

## Commit & Pull Request Guidelines
Git history is not available in this workspace, so no repository-specific commit pattern can be inferred. Use short imperative commit messages such as `feat: add parking follow-up regression test` or `fix: handle empty chart rows`. PRs should include the problem statement, affected entry points (`main.py`, `server.py`, or `scripts/*`), verification commands, and screenshots when `static/index.html` changes.

## Configuration & Security Tips
Keep API keys and database credentials in environment variables or local `.env` files only. Do not commit real MySQL configs, generated session payloads, or output artifacts containing business data.
