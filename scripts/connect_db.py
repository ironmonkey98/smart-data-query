from __future__ import annotations

import csv
import importlib
import json
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path


def load_dataset(source: str, source_type: str = "csv", connector_module=None) -> list[dict]:
    normalized = source_type.lower()
    if normalized == "csv":
        return _load_csv(source)
    if normalized == "excel":
        raise NotImplementedError("Excel 数据源留作下一阶段扩展，V1 先支持 CSV。")
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
