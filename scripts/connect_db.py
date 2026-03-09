from __future__ import annotations

import csv
import importlib
import json
import sqlite3
from collections import defaultdict
from contextlib import closing
from datetime import date, datetime
from pathlib import Path


def load_dataset(source: str, source_type: str = "csv", connector_module=None, task: dict | None = None) -> list[dict]:
    normalized = source_type.lower()
    if normalized == "csv":
        return _load_csv(source)
    if normalized == "excel":
        raise NotImplementedError("Excel 数据源留作下一阶段扩展，V1 先支持 CSV。")
    if normalized == "sqlite":
        return _load_sqlite(source, task=task)
    if normalized == "mysql":
        return _load_mysql(source, connector_module=connector_module)
    raise ValueError(f"不支持的数据源类型: {source_type}")


def execute_structured_task(rows: list[dict], task: dict) -> dict:
    filtered_rows = [row for row in rows if _match_filters(row, task.get("filters", []))]
    filtered_rows = _apply_time_range(filtered_rows, task)
    grouped = defaultdict(float)
    metric = task["metric"]
    dimensions = list(task.get("dimensions", []))
    time_field = task.get("time_field")
    if time_field:
        dimensions.insert(0, time_field)

    for row in filtered_rows:
        key = tuple(row[field] for field in dimensions)
        grouped[key] += float(row[metric["field"]])

    result_rows = []
    for key, value in grouped.items():
        item = {field: key[index] for index, field in enumerate(dimensions)}
        item[metric["label"]] = round(value, 2)
        result_rows.append(item)

    result_rows.sort(key=lambda item: tuple(item[field] for field in dimensions))
    return {
        "row_count": len(result_rows),
        "rows": result_rows,
        "metric": metric,
        "dimensions": dimensions,
    }


def _load_csv(source: str) -> list[dict]:
    rows = []
    with Path(source).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            normalized = {key: _coerce_scalar(value) for key, value in dict(row).items()}
            rows.append(normalized)
    return rows


def _match_filters(row: dict, filters: list[dict]) -> bool:
    for item in filters:
        field = item["field"]
        operator = item["operator"]
        if operator == "=" and str(row.get(field)) != str(item["value"]):
            return False
        if operator == "!=" and str(row.get(field)) == str(item["value"]):
            return False
        if operator == "in" and str(row.get(field)) not in [str(value) for value in item["values"]]:
            return False
    return True


def _apply_time_range(rows: list[dict], task: dict) -> list[dict]:
    time_field = task.get("time_field")
    if not time_field:
        return rows
    time_range = task.get("time_range", {})
    start_value = time_range.get("start")
    end_value = time_range.get("end")
    if not start_value and not end_value:
        return rows

    start_date = _to_date(start_value) if start_value else None
    end_date = _to_date(end_value) if end_value else None
    result = []
    for row in rows:
        current = _to_date(row[time_field])
        if start_date and current < start_date:
            continue
        if end_date and current > end_date:
            continue
        result.append(row)
    return result


def _to_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def _coerce_scalar(value: str):
    if value is None:
        return value
    text = str(value).strip()
    if text == "":
        return text
    try:
        if "." in text:
            return float(text)
        return int(text)
    except ValueError:
        return text


def _load_mysql(source: str, connector_module=None) -> list[dict]:
    config = _load_mysql_config(source)
    connector = connector_module or _resolve_mysql_connector()
    connection = connector.connect(
        host=config["host"],
        port=int(config.get("port", 3306)),
        user=config["user"],
        password=config["password"],
        database=config["database"],
    )
    cursor = connection.cursor()
    query = config.get("query") or _build_select_query(config)
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [item[0] for item in cursor.description]
        return [
            {column: _coerce_scalar(value) for column, value in zip(columns, row)}
            for row in rows
        ]
    finally:
        cursor.close()
        connection.close()


def _load_mysql_config(source: str) -> dict:
    text = Path(source).read_text(encoding="utf-8")
    return json.loads(text)


def _resolve_mysql_connector():
    for module_name in ("pymysql", "mysql.connector"):
        try:
            return importlib.import_module(module_name)
        except ModuleNotFoundError:
            continue
    raise ModuleNotFoundError("未找到可用的 MySQL 连接器，请安装 pymysql 或 mysql-connector-python。")


def _build_select_query(config: dict) -> str:
    table = config.get("table")
    if not table:
        raise ValueError("MySQL 配置必须提供 query 或 table。")
    limit = int(config.get("limit", 1000))
    return f"SELECT * FROM {table} LIMIT {limit}"


def _load_sqlite(source: str, task: dict | None = None) -> list[dict]:
    if not task:
        raise ValueError("SQLite 数据源必须提供 task。")
    if str(task.get("domain")) == "parking_ops" or str(task.get("intent", "")).startswith("parking_"):
        query_profile = task.get("query_profile") or "parking_daily_overview_join"
        if query_profile == "parking_daily_overview_join":
            return _filter_focus_entity_rows(_load_sqlite_parking_daily_rows(source), task)
        if query_profile == "payment_passage_reconciliation_by_date":
            return _load_sqlite_payment_passage_reconciliation_by_date(source, task)
        if query_profile == "payment_passage_reconciliation_by_plate":
            return _load_sqlite_payment_passage_reconciliation_by_plate(source, task)
        if query_profile == "lot_capacity_efficiency_ranking":
            return _load_sqlite_lot_capacity_efficiency_ranking(source, task)
        if query_profile == "payment_method_risk_breakdown":
            return _load_sqlite_payment_method_risk_breakdown(source, task)
        raise ValueError(f"SQLite 数据源暂不支持 query_profile={query_profile}")
    raise ValueError("SQLite 数据源当前仅支持 parking_ops 多表联查。")


def _load_sqlite_parking_daily_rows(source: str) -> list[dict]:
    query = """
    WITH payment_daily AS (
        SELECT
            date(pr.paid_at) AS stat_date,
            pr.lot_id,
            SUM(pr.actual_amount) AS total_revenue,
            SUM(pr.actual_amount) AS temp_revenue,
            AVG(
                CASE
                    WHEN pr.payment_result LIKE '%失败%' OR pr.payment_result = '支付成功,通知车场失败' THEN 1.0
                    ELSE 0.0
                END
            ) AS payment_failure_rate
        FROM parking_payment_records pr
        WHERE pr.paid_at IS NOT NULL
        GROUP BY date(pr.paid_at), pr.lot_id
    ),
    passage_daily AS (
        SELECT
            date(ps.entry_at) AS stat_date,
            ps.lot_id,
            COUNT(*) AS entry_count,
            SUM(
                CASE
                    WHEN ps.vehicle_type = '临时车' AND COALESCE(ps.receivable_amount, 0) = 0 AND COALESCE(ps.actual_amount, 0) = 0 THEN 1
                    ELSE 0
                END
            ) AS free_release_count,
            MIN(
                SUM(MIN(MAX(COALESCE(ps.stay_minutes, 0), 0), 1440.0)) / NULLIF(MAX(pl.total_spaces) * 1440.0, 0),
                1.0
            ) AS occupancy_rate
        FROM parking_passage_records ps
        JOIN parking_lots pl ON pl.lot_id = ps.lot_id
        WHERE ps.entry_at IS NOT NULL
        GROUP BY date(ps.entry_at), ps.lot_id
    ),
    daily_keys AS (
        SELECT stat_date, lot_id FROM payment_daily
        UNION
        SELECT stat_date, lot_id FROM passage_daily
    )
    SELECT
        dk.stat_date AS stat_date,
        pl.parking_lot_name AS parking_lot,
        ROUND(COALESCE(pd.total_revenue, 0), 2) AS total_revenue,
        ROUND(COALESCE(pd.temp_revenue, 0), 2) AS temp_revenue,
        0.0 AS monthly_revenue,
        COALESCE(psd.entry_count, 0) AS entry_count,
        ROUND(COALESCE(pd.payment_failure_rate, 0), 4) AS payment_failure_rate,
        0 AS abnormal_open_count,
        COALESCE(psd.free_release_count, 0) AS free_release_count,
        ROUND(COALESCE(psd.occupancy_rate, 0), 4) AS occupancy_rate
    FROM daily_keys dk
    JOIN parking_lots pl ON pl.lot_id = dk.lot_id
    LEFT JOIN payment_daily pd ON pd.stat_date = dk.stat_date AND pd.lot_id = dk.lot_id
    LEFT JOIN passage_daily psd ON psd.stat_date = dk.stat_date AND psd.lot_id = dk.lot_id
    ORDER BY dk.stat_date, pl.parking_lot_name
    """
    return _execute_sqlite_query(source, query)


def _load_sqlite_payment_passage_reconciliation_by_date(source: str, task: dict) -> list[dict]:
    constraints = task.get("constraints", {})
    mismatch_type = constraints.get("mismatch_type")
    date_clause, params = _build_time_range_clause(task, "dk.stat_date")
    mismatch_clause = ""
    if mismatch_type == "payment_without_passage":
        mismatch_clause = "AND COALESCE(pd.payment_count, 0) > 0 AND COALESCE(psd.entry_count, 0) = 0"
    elif mismatch_type == "passage_without_payment":
        mismatch_clause = "AND COALESCE(psd.entry_count, 0) > 0 AND COALESCE(pd.payment_count, 0) = 0"

    query = f"""
    WITH payment_daily AS (
        SELECT
            date(pr.paid_at) AS stat_date,
            pr.lot_id,
            COUNT(*) AS payment_count,
            SUM(pr.actual_amount) AS total_revenue
        FROM parking_payment_records pr
        WHERE pr.paid_at IS NOT NULL
        GROUP BY date(pr.paid_at), pr.lot_id
    ),
    passage_daily AS (
        SELECT
            date(ps.entry_at) AS stat_date,
            ps.lot_id,
            COUNT(*) AS entry_count
        FROM parking_passage_records ps
        WHERE ps.entry_at IS NOT NULL
        GROUP BY date(ps.entry_at), ps.lot_id
    ),
    daily_keys AS (
        SELECT stat_date, lot_id FROM payment_daily
        UNION
        SELECT stat_date, lot_id FROM passage_daily
    )
    SELECT
        dk.stat_date AS stat_date,
        pl.parking_lot_name AS parking_lot,
        COALESCE(pd.payment_count, 0) AS payment_count,
        COALESCE(psd.entry_count, 0) AS entry_count,
        ROUND(COALESCE(pd.total_revenue, 0), 2) AS total_revenue,
        CASE
            WHEN COALESCE(pd.payment_count, 0) > 0 AND COALESCE(psd.entry_count, 0) = 0 THEN 'payment_without_passage'
            WHEN COALESCE(psd.entry_count, 0) > 0 AND COALESCE(pd.payment_count, 0) = 0 THEN 'passage_without_payment'
            ELSE 'matched'
        END AS mismatch_type
    FROM daily_keys dk
    JOIN parking_lots pl ON pl.lot_id = dk.lot_id
    LEFT JOIN payment_daily pd ON pd.stat_date = dk.stat_date AND pd.lot_id = dk.lot_id
    LEFT JOIN passage_daily psd ON psd.stat_date = dk.stat_date AND psd.lot_id = dk.lot_id
    WHERE 1=1 {date_clause} {mismatch_clause}
    ORDER BY dk.stat_date, pl.parking_lot_name
    """
    return _filter_focus_entity_rows(_execute_sqlite_query(source, query, params), task)


def _load_sqlite_payment_passage_reconciliation_by_plate(source: str, task: dict) -> list[dict]:
    constraints = task.get("constraints", {})
    date_clause, params = _build_time_range_clause(task, "keys.stat_date")
    extra_clause = ""
    if constraints.get("min_stay_minutes"):
        extra_clause += " AND COALESCE(psd.stay_minutes, 0) >= ?"
        params.append(int(constraints["min_stay_minutes"]))
    if constraints.get("mismatch_type") == "passage_without_payment":
        extra_clause += " AND COALESCE(psd.passage_count, 0) > 0 AND COALESCE(ppd.payment_count, 0) = 0"
    elif constraints.get("mismatch_type") == "payment_without_passage":
        extra_clause += " AND COALESCE(ppd.payment_count, 0) > 0 AND COALESCE(psd.passage_count, 0) = 0"

    query = f"""
    WITH payment_plate_daily AS (
        SELECT
            date(COALESCE(pr.entry_at, pr.paid_at)) AS stat_date,
            pr.lot_id,
            pr.license_plate,
            COUNT(*) AS payment_count,
            SUM(pr.actual_amount) AS payment_amount,
            SUM(pr.receivable_amount) AS receivable_amount
        FROM parking_payment_records pr
        WHERE COALESCE(pr.entry_at, pr.paid_at) IS NOT NULL
        GROUP BY date(COALESCE(pr.entry_at, pr.paid_at)), pr.lot_id, pr.license_plate
    ),
    passage_plate_daily AS (
        SELECT
            date(ps.entry_at) AS stat_date,
            ps.lot_id,
            ps.license_plate,
            COUNT(*) AS passage_count,
            MAX(COALESCE(ps.stay_minutes, 0)) AS stay_minutes,
            SUM(COALESCE(ps.actual_amount, 0)) AS passage_amount
        FROM parking_passage_records ps
        WHERE ps.entry_at IS NOT NULL
        GROUP BY date(ps.entry_at), ps.lot_id, ps.license_plate
    ),
    keys AS (
        SELECT stat_date, lot_id, license_plate FROM payment_plate_daily
        UNION
        SELECT stat_date, lot_id, license_plate FROM passage_plate_daily
    )
    SELECT
        keys.stat_date AS stat_date,
        pl.parking_lot_name AS parking_lot,
        keys.license_plate AS license_plate,
        COALESCE(ppd.payment_count, 0) AS payment_count,
        ROUND(COALESCE(ppd.payment_amount, 0), 2) AS payment_amount,
        ROUND(COALESCE(ppd.receivable_amount, 0), 2) AS receivable_amount,
        COALESCE(psd.passage_count, 0) AS passage_count,
        ROUND(COALESCE(psd.passage_amount, 0), 2) AS passage_amount,
        COALESCE(psd.stay_minutes, 0) AS stay_minutes
    FROM keys
    JOIN parking_lots pl ON pl.lot_id = keys.lot_id
    LEFT JOIN payment_plate_daily ppd
      ON ppd.stat_date = keys.stat_date AND ppd.lot_id = keys.lot_id AND ppd.license_plate = keys.license_plate
    LEFT JOIN passage_plate_daily psd
      ON psd.stat_date = keys.stat_date AND psd.lot_id = keys.lot_id AND psd.license_plate = keys.license_plate
    WHERE 1=1 {date_clause} {extra_clause}
    ORDER BY keys.stat_date DESC, pl.parking_lot_name, keys.license_plate
    """
    return _filter_focus_entity_rows(_execute_sqlite_query(source, query, params), task)


def _load_sqlite_lot_capacity_efficiency_ranking(source: str, task: dict) -> list[dict]:
    date_clause_payment, params_payment = _build_time_range_clause(task, "date(pr.paid_at)")
    date_clause_passage, params_passage = _build_time_range_clause(task, "date(ps.entry_at)")
    params = params_payment + params_passage
    query = f"""
    WITH payment_lot AS (
        SELECT
            pr.lot_id,
            SUM(pr.actual_amount) AS total_revenue
        FROM parking_payment_records pr
        WHERE pr.paid_at IS NOT NULL {date_clause_payment}
        GROUP BY pr.lot_id
    ),
    passage_lot AS (
        SELECT
            ps.lot_id,
            COUNT(*) AS entry_count,
            SUM(COALESCE(ps.stay_minutes, 0)) AS total_stay_minutes
        FROM parking_passage_records ps
        WHERE ps.entry_at IS NOT NULL {date_clause_passage}
        GROUP BY ps.lot_id
    )
    SELECT
        pl.parking_lot_name AS parking_lot,
        pl.total_spaces AS total_spaces,
        ROUND(COALESCE(pay.total_revenue, 0), 2) AS total_revenue,
        COALESCE(pas.entry_count, 0) AS entry_count,
        ROUND(COALESCE(pay.total_revenue, 0) / NULLIF(pl.total_spaces, 0), 2) AS revenue_per_space,
        ROUND(COALESCE(pas.entry_count, 0) * 1.0 / NULLIF(pl.total_spaces, 0), 2) AS entry_per_space,
        ROUND(COALESCE(pas.total_stay_minutes, 0) * 1.0 / NULLIF(pl.total_spaces, 0), 2) AS stay_minutes_per_space
    FROM parking_lots pl
    LEFT JOIN payment_lot pay ON pay.lot_id = pl.lot_id
    LEFT JOIN passage_lot pas ON pas.lot_id = pl.lot_id
    ORDER BY revenue_per_space DESC, total_revenue DESC, pl.parking_lot_name
    """
    return _filter_focus_entity_rows(_execute_sqlite_query(source, query, params), task)


def _load_sqlite_payment_method_risk_breakdown(source: str, task: dict) -> list[dict]:
    date_clause, params = _build_time_range_clause(task, "date(pr.paid_at)")
    query = f"""
    SELECT
        pl.parking_lot_name AS parking_lot,
        pr.payment_method AS payment_method,
        COUNT(*) AS payment_count,
        SUM(
            CASE
                WHEN pr.payment_result LIKE '%失败%' OR pr.payment_result = '支付成功,通知车场失败' THEN 1
                ELSE 0
            END
        ) AS failure_count,
        ROUND(
            AVG(
                CASE
                    WHEN pr.payment_result LIKE '%失败%' OR pr.payment_result = '支付成功,通知车场失败' THEN 1.0
                    ELSE 0.0
                END
            ),
            4
        ) AS payment_failure_rate,
        ROUND(SUM(pr.actual_amount), 2) AS total_revenue
    FROM parking_payment_records pr
    JOIN parking_lots pl ON pl.lot_id = pr.lot_id
    WHERE pr.paid_at IS NOT NULL {date_clause}
    GROUP BY pl.parking_lot_name, pr.payment_method
    ORDER BY payment_failure_rate DESC, failure_count DESC, total_revenue DESC
    """
    return _filter_focus_entity_rows(_execute_sqlite_query(source, query, params), task)


def _execute_sqlite_query(source: str, query: str, params: list | None = None) -> list[dict]:
    with closing(sqlite3.connect(source)) as connection:
        with closing(connection.cursor()) as cursor:
            raw_rows = cursor.execute(query, params or []).fetchall()
            columns = [column[0] for column in cursor.description]
    return [{key: _coerce_scalar(value) for key, value in zip(columns, row)} for row in raw_rows]


# ─── 通用 SQL 执行接口（供 Agent execute_sql 工具调用）───────────────────────

def execute_raw_sql_on_sqlite(source_path: str, sql: str) -> list[dict]:
    """对 SQLite 文件执行任意 SELECT，返回 list[dict]。"""
    return _execute_sqlite_query(source_path, sql)


def execute_raw_sql_on_csv(csv_path: str, sql: str, table_name: str = "sales") -> list[dict]:
    """将 CSV 载入内存 SQLite，执行 SELECT SQL，返回 list[dict]。
    table_name 即 SQL 中的表名，默认为 'sales'。
    """
    rows_raw = _load_csv(csv_path)
    if not rows_raw:
        return []

    cols = list(rows_raw[0].keys())

    # 按首行值推断 SQLite 列类型，确保数值运算正确
    def _col_type(val) -> str:
        if isinstance(val, bool):
            return "INTEGER"
        if isinstance(val, int):
            return "INTEGER"
        if isinstance(val, float):
            return "REAL"
        return "TEXT"

    sample = rows_raw[0]
    col_defs = ", ".join(f'"{col}" {_col_type(sample[col])}' for col in cols)

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute(f'CREATE TABLE "{table_name}" ({col_defs})')
        conn.executemany(
            f'INSERT INTO "{table_name}" VALUES ({", ".join(["?"] * len(cols))})',
            [[row[col] for col in cols] for row in rows_raw],
        )
        conn.commit()
        cursor = conn.execute(sql)
        columns = [d[0] for d in cursor.description]
        return [
            {col: _coerce_scalar(val) for col, val in zip(columns, row)}
            for row in cursor.fetchall()
        ]
    finally:
        conn.close()


def _filter_focus_entity_rows(rows: list[dict], task: dict) -> list[dict]:
    focus_entities = task.get("focus_entities") or []
    if not focus_entities:
        semantic_plan = task.get("semantic_plan") or {}
        focus_entities = semantic_plan.get("focus_entities") or []
    if not focus_entities:
        return rows
    allowed = {str(item).strip() for item in focus_entities if str(item).strip()}
    if not allowed:
        return rows
    return [row for row in rows if str(row.get("parking_lot", "")).strip() in allowed]


def _build_time_range_clause(task: dict, column: str) -> tuple[str, list]:
    time_range = task.get("time_range", {}) or {}
    start_value = time_range.get("start")
    end_value = time_range.get("end")
    params = []
    clauses = []
    if start_value:
        clauses.append(f"{column} >= ?")
        params.append(start_value)
    if end_value:
        clauses.append(f"{column} <= ?")
        params.append(end_value)
    if not clauses:
        return "", params
    return " AND " + " AND ".join(clauses), params
